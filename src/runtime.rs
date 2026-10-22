//! Sanitized service sensors and request ownership. No operational secrets.

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde::Serialize;

use crate::config::QueuedConfig;
use crate::control::{Control, Decision, Snapshot};

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Component {
    Disabled,
    Starting,
    Healthy,
    Degraded,
}

#[derive(Clone, Copy)]
struct Observations {
    dispatcher: Component,
    feedback: Component,
    synchronization: Component,
    acknowledged: u64,
    feedback_errors: u64,
    sync_errors: u64,
    last_sync_merged: Option<u64>,
}

#[derive(Serialize)]
pub struct Health {
    pub dispatcher: Component,
    pub version: &'static str,
    pub phase: &'static str,
    pub ready: bool,
    pub storage: &'static str,
    pub provider_circuit: &'static str,
    pub feedback: Component,
    pub suppression_sync: Component,
    pub in_flight_requests: usize,
    pub feedback_acknowledged: u64,
    pub feedback_errors: u64,
    pub sync_errors: u64,
    pub last_sync_merged: Option<u64>,
}

pub struct Runtime {
    pub control: Arc<Control>,
    pub queued: Option<QueuedConfig>,
    active: AtomicUsize,
    observations: Mutex<Observations>,
}

pub struct RequestPermit(Arc<Runtime>);

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Runtime {
    fn observations(&self) -> MutexGuard<'_, Observations> {
        self.observations.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Marks a component degraded and bumps its error counter. Shared by
    /// `feedback_error` and `sync_error`, which differ only in which fields
    /// of `Observations` they touch.
    const fn degrade(component: &mut Component, errors: &mut u64) {
        *component = Component::Degraded;
        *errors = errors.saturating_add(1);
    }

    pub fn new(
        control: Arc<Control>,
        feedback: bool,
        synchronization: bool,
        queued: Option<QueuedConfig>,
    ) -> Arc<Self> {
        let dispatcher = if queued.is_some() { Component::Starting } else { Component::Disabled };
        Arc::new(Self {
            control,
            queued,
            active: AtomicUsize::new(0),
            observations: Mutex::new(Observations {
                dispatcher,
                feedback: if feedback { Component::Starting } else { Component::Disabled },
                synchronization: if synchronization {
                    Component::Starting
                } else {
                    Component::Disabled
                },
                acknowledged: 0,
                feedback_errors: 0,
                sync_errors: 0,
                last_sync_merged: None,
            }),
        })
    }

    pub fn enter(self: &Arc<Self>) -> Option<RequestPermit> {
        if self.control.draining() {
            return None;
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        let permit = RequestPermit(self.clone());
        if self.control.draining() {
            return None;
        }
        Some(permit)
    }

    pub fn health(&self, storage_ready: bool, provider_circuit: &'static str) -> Health {
        let observed = *self.observations();
        let ready = self.control.decide(Snapshot {
            valid: true,
            service_ready: true,
            storage_ready,
            recipients_allowed: true,
            rate_allowed: true,
        }) == Decision::Allow;
        let phase = if self.control.draining() {
            "draining"
        } else if !storage_ready {
            "degraded"
        } else if !ready {
            "starting"
        } else if observed.feedback == Component::Degraded
            || observed.dispatcher == Component::Degraded
            || observed.synchronization == Component::Degraded
            || provider_circuit == "open"
        {
            "degraded"
        } else {
            "ready"
        };
        Health {
            dispatcher: observed.dispatcher,
            version: env!("CARGO_PKG_VERSION"),
            phase,
            ready,
            storage: if storage_ready { "ready" } else { "unavailable" },
            provider_circuit,
            feedback: observed.feedback,
            suppression_sync: observed.synchronization,
            in_flight_requests: self.active.load(Ordering::Acquire),
            feedback_acknowledged: observed.acknowledged,
            feedback_errors: observed.feedback_errors,
            sync_errors: observed.sync_errors,
            last_sync_merged: observed.last_sync_merged,
        }
    }

    pub fn dispatch_state(&self, healthy: bool) {
        self.observations().dispatcher =
            if healthy { Component::Healthy } else { Component::Degraded };
    }

    pub fn dispatcher_ready(&self, storage_ready: bool, provider_circuit: &str) -> bool {
        self.queued.is_some()
            && self.control.ready()
            && storage_ready
            && provider_circuit != "open"
            && self.observations().dispatcher == Component::Healthy
    }

    pub fn feedback_ok(&self) {
        self.observations().feedback = Component::Healthy;
    }

    pub fn feedback_error(&self) {
        let mut guard = self.observations();
        let observed = &mut *guard;
        Self::degrade(&mut observed.feedback, &mut observed.feedback_errors);
        drop(guard);
    }

    pub fn acknowledged(&self) {
        let mut observed = self.observations();
        observed.acknowledged = observed.acknowledged.saturating_add(1);
    }

    pub fn sync_ok(&self, merged: u64) {
        let mut observed = self.observations();
        observed.synchronization = Component::Healthy;
        observed.last_sync_merged = Some(merged);
        drop(observed);
        self.control.bootstrap_done();
    }

    pub fn sync_error(&self) {
        let mut guard = self.observations();
        let observed = &mut *guard;
        Self::degrade(&mut observed.synchronization, &mut observed.sync_errors);
        drop(guard);
    }
}
