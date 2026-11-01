//! Cooperative stop first; bounded task cancellation only at the deadline.
//!
//! Background workers restart one at a time, each restart held off longer
//! while they keep dying. A burst beyond the restart intensity, or the docking
//! listener stopping, drains the whole ship for Mothership to relaunch.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use deadline_machines::{Clock, NS_PER_SECOND, SystemClock};
use supervisor_machines::{Decision, Exit, Intensity, Policy, Strategy, Supervisor};
use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{Config, DockingConfig};
use crate::control::Control;
use crate::domain_intel::Intel;
use crate::provider::domain_block_guard::DomainBlockGuard;
use crate::provider::guard::SuppressionGuard;
use crate::provider::queued::QueuedProvider;
use crate::provider::verify_guard::EmailVerificationGuard;
use crate::rogue::Experiments;
use crate::runtime::Runtime;
use crate::state::Store;
use crate::verify::Verifier;

#[cfg(feature = "ses")]
mod ses;

/// Dispatcher, feedback consumer, rate discovery, suppression reconciliation
/// and the rogue experiment watcher.
const WORKERS: usize = 5;
const RESTART_HISTORY: usize = 8;
const RESTART_INTENSITY: Intensity = Intensity::new(3, 60 * NS_PER_SECOND);
const RESTART_BASE: Duration = Duration::from_secs(1);
const RESTART_CEILING: Duration = Duration::from_secs(30);

type Run = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

pub struct Worker {
    name: &'static str,
    start: Box<dyn Fn() -> Run + Send + Sync>,
}

impl Worker {
    pub fn new<F, R>(name: &'static str, start: F) -> Self
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Self { name, start: Box::new(move || Box::pin(start())) }
    }
}

struct Crew {
    tasks: JoinSet<anyhow::Result<()>>,
    workers: Vec<Worker>,
    running: HashMap<Id, usize>,
    tree: Supervisor<WORKERS, RESTART_HISTORY>,
    clock: SystemClock,
    stop: CancellationToken,
    base: Duration,
    ceiling: Duration,
}

impl Crew {
    fn new(stop: CancellationToken, base: Duration, ceiling: Duration) -> Self {
        Self {
            tasks: JoinSet::new(),
            workers: Vec::with_capacity(WORKERS),
            running: HashMap::new(),
            tree: Supervisor::new(Strategy::OneForOne, RESTART_INTENSITY),
            clock: crate::budget::clock(),
            stop,
            base,
            ceiling,
        }
    }

    /// A task whose exit is never restarted: it stops the ship.
    fn essential(&mut self, run: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        self.tasks.spawn(run);
    }

    fn enlist(&mut self, worker: Worker) {
        self.workers.push(worker);
        self.start(self.workers.len() - 1, Duration::ZERO);
    }

    fn start(&mut self, index: usize, delay: Duration) {
        let run = (self.workers[index].start)();
        let stop = self.stop.clone();
        let handle = self.tasks.spawn(async move {
            tokio::select! {
                () = stop.cancelled() => return Ok(()),
                () = tokio::time::sleep(delay) => {}
            }
            run.await
        });
        self.running.insert(handle.id(), index);
    }

    /// `true` when supervision gave up before `signal`.
    async fn supervise(&mut self, signal: impl Future<Output = ()>) -> bool {
        tokio::pin!(signal);
        loop {
            let joined = tokio::select! {
                () = &mut signal => return false,
                Some(joined) = self.tasks.join_next_with_id() => joined,
            };
            let (id, outcome) = match joined {
                Ok((id, Ok(()))) => (id, Ok(())),
                Ok((id, Err(error))) => (id, Err(error.to_string())),
                Err(error) => (error.id(), Err(error.to_string())),
            };
            let Some(index) = self.running.remove(&id) else {
                error!("The docking listener stopped");
                return true;
            };
            let worker = self.workers[index].name;
            let exit = match &outcome {
                Ok(()) => Exit::Normal,
                Err(error) => {
                    warn!(worker, %error, "Supervised worker died");
                    Exit::Abnormal
                }
            };
            let now = self.clock.now_ns();
            match self.tree.stopped(index, Policy::Permanent, exit, now) {
                Decision::Restart(_) => {
                    let delay = Duration::from_nanos(self.tree.restart_delay_ns(
                        crate::budget::nanos(self.base),
                        crate::budget::nanos(self.ceiling),
                        now,
                    ));
                    warn!(
                        worker,
                        recent_restarts = self.tree.recent_restarts(now),
                        delay_ms = delay.as_millis(),
                        "Restarting supervised worker"
                    );
                    self.start(index, delay);
                }
                Decision::Ignore => {}
                Decision::Escalate => {
                    error!(worker, "Worker restart intensity exceeded");
                    return true;
                }
            }
        }
    }
}

// One-time startup wiring, called from exactly one place (main.rs); grouping
// these into a struct would add indirection without a second call site to
// justify it.
#[allow(clippy::too_many_arguments)]
pub async fn run<P: QueuedProvider + 'static>(
    docking: DockingConfig,
    config: Config,
    provider: Arc<P>,
    store: Store,
    control: Arc<Control>,
    verifier: Arc<Verifier>,
    intel: Arc<Intel>,
    experiments: Arc<Experiments>,
) -> anyhow::Result<()> {
    let new_domain_threshold_days =
        config.domain_intel.as_ref().map(|intel| intel.new_domain_threshold_days);
    let runtime = Runtime::new(
        control,
        config.feedback.is_some(),
        config.suppression_sync.is_some(),
        config.queued.clone(),
        intel,
        new_domain_threshold_days,
    );
    let stop = CancellationToken::new();
    let mut crew = Crew::new(stop.clone(), RESTART_BASE, RESTART_CEILING);
    // Cheapest/broadest filters run first: domain blocklist (free, local,
    // whole-domain), then suppression (free, local, per-address), then paid
    // external verification only for recipients that survived both - never
    // pay to verify an address we were dropping anyway.
    let (reject_states, cache_ttl) = config.email_verification.as_ref().map_or_else(
        || (vec![crate::verify::State::Undeliverable], Duration::from_hours(168)),
        |verification| {
            (
                verification.reject_states.clone(),
                Duration::from_secs(verification.cache_ttl_seconds),
            )
        },
    );
    let email_verified = Arc::new(EmailVerificationGuard {
        inner: provider.clone(),
        verifier,
        store: store.clone(),
        reject_states,
        cache_ttl,
        experiments: experiments.clone(),
    });
    let suppressed = Arc::new(SuppressionGuard {
        inner: email_verified,
        store: store.clone(),
        control: runtime.control.clone(),
    });
    let guarded = Arc::new(DomainBlockGuard { inner: suppressed, store: store.clone() });
    crew.essential(crate::bay::serve(
        docking,
        guarded,
        store.clone(),
        runtime.clone(),
        stop.clone(),
    ));
    if runtime.queued.is_some() {
        let (store, runtime, stop) = (store.clone(), runtime.clone(), stop.clone());
        crew.enlist(Worker::new("dispatcher", move || {
            crate::outbound::run(provider.clone(), store.clone(), runtime.clone(), stop.clone())
        }));
    }
    if let Some(rogue) = &config.rogue {
        let every = Duration::from_secs(rogue.poll_interval_seconds);
        let (store, experiments, stop) = (store.clone(), experiments.clone(), stop.clone());
        crew.enlist(Worker::new("rogue", move || {
            crate::rogue::watch(store.clone(), experiments.clone(), every, stop.clone())
        }));
    }
    #[cfg(feature = "ses")]
    for worker in ses::workers(&config, &store, &runtime, &experiments, &stop).await? {
        crew.enlist(worker);
    }
    let unexpected = crew.supervise(crate::wait_for_signal()).await;
    runtime.control.drain();
    stop.cancel();
    info!("Stopping admission and draining active operations");
    drain(&mut crew.tasks, Duration::from_secs(config.shutdown_timeout_seconds)).await?;
    anyhow::ensure!(!unexpected, "supervision gave up on a worker or the docking listener");
    Ok(())
}

pub async fn drain(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    deadline: Duration,
) -> anyhow::Result<()> {
    let finished = tokio::time::timeout(deadline, async {
        let mut failed = false;
        while let Some(result) = tasks.join_next().await {
            failed |= !matches!(result, Ok(Ok(())));
        }
        failed
    })
    .await;
    match finished {
        Ok(false) => Ok(()),
        Ok(true) => anyhow::bail!("a worker failed while draining"),
        Err(_) => {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            anyhow::bail!("shutdown deadline elapsed; in-flight send outcomes may be unknown")
        }
    }
}

#[cfg(test)]
mod tests;
