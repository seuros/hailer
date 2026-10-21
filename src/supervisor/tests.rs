use super::*;

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
