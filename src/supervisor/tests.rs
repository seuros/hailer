use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn crew() -> Crew {
    Crew::new(CancellationToken::new(), Duration::from_millis(1), Duration::from_millis(4))
}

fn counted(starts: &Arc<AtomicUsize>, run: fn(usize) -> Run) -> Worker {
    let starts = starts.clone();
    Worker::new("fixture", move || run(starts.fetch_add(1, Ordering::SeqCst)))
}

async fn reached(starts: &AtomicUsize, count: usize) {
    while starts.load(Ordering::SeqCst) < count {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn drain_waits_for_owned_work_then_cancels_only_at_its_deadline() {
    let mut tasks = JoinSet::new();
    tasks.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    });
    drain(&mut tasks, Duration::from_secs(1)).await.expect("graceful finish");
    assert!(tasks.is_empty());
    tasks.spawn(std::future::pending());
    let error = drain(&mut tasks, Duration::from_millis(10)).await.expect_err("deadline");
    assert!(error.to_string().contains("outcomes may be unknown"));
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn a_worker_that_keeps_dying_escalates_once_intensity_is_spent() {
    let mut crew = crew();
    let starts = Arc::new(AtomicUsize::new(0));
    crew.enlist(counted(&starts, |_| Box::pin(async { anyhow::bail!("boom") })));
    assert!(crew.supervise(std::future::pending()).await);
    let restarts = usize::try_from(RESTART_INTENSITY.restarts).expect("small");
    assert_eq!(starts.load(Ordering::SeqCst), restarts + 1);
}

#[tokio::test]
async fn a_dead_worker_restarts_alone_and_the_signal_still_stops_supervision() {
    let mut crew = crew();
    let (flaky, steady) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    crew.enlist(counted(&flaky, |start| {
        Box::pin(async move {
            anyhow::ensure!(start > 0, "first run dies");
            std::future::pending().await
        })
    }));
    crew.enlist(counted(&steady, |_| Box::pin(std::future::pending())));
    assert!(!crew.supervise(reached(&flaky, 2)).await);
    assert_eq!(steady.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_panicking_worker_is_restarted() {
    let mut crew = crew();
    let starts = Arc::new(AtomicUsize::new(0));
    crew.enlist(counted(&starts, |start| {
        Box::pin(async move {
            assert!(start > 0, "first run panics");
            std::future::pending().await
        })
    }));
    assert!(!crew.supervise(reached(&starts, 2)).await);
}

#[tokio::test]
async fn the_docking_listener_stopping_escalates_without_a_restart() {
    let mut crew = crew();
    let starts = Arc::new(AtomicUsize::new(0));
    crew.enlist(counted(&starts, |_| Box::pin(std::future::pending())));
    crew.essential(async { Ok(()) });
    assert!(crew.supervise(std::future::pending()).await);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_pending_restart_ends_on_stop_instead_of_running_the_worker() {
    let stop = CancellationToken::new();
    let mut crew = Crew::new(stop.clone(), Duration::from_secs(60), Duration::from_secs(60));
    let starts = Arc::new(AtomicUsize::new(0));
    crew.enlist(counted(&starts, |start| {
        Box::pin(async move {
            anyhow::ensure!(start > 0, "first run dies");
            anyhow::bail!("a restart must not run after stop")
        })
    }));
    assert!(!crew.supervise(reached(&starts, 2)).await);
    stop.cancel();
    drain(&mut crew.tasks, Duration::from_secs(1)).await.expect("restart abandoned cleanly");
}
