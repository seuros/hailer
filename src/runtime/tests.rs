use super::*;

#[test]
fn health_reflects_bootstrap_storage_failure_and_drain_without_leaking_data() {
    let control = Arc::new(Control::new(None, false).expect("control"));
    let runtime = Runtime::new(control, true, true);
    assert_eq!(runtime.health(true, "closed").phase, "starting");
    runtime.sync_error();
    assert!(!runtime.health(true, "closed").ready);
    runtime.sync_ok(12);
    runtime.feedback_ok();
    assert!(runtime.health(true, "closed").ready);
    assert_eq!(runtime.health(true, "closed").phase, "ready");
    runtime.feedback_error();
    assert_eq!(runtime.health(true, "closed").phase, "degraded");
    // A provider circuit must still be able to take half-open probes.
    assert!(runtime.health(true, "open").ready);
    assert!(!runtime.health(false, "closed").ready);
    let permit = runtime.enter().expect("request");
    runtime.control.drain();
    assert!(runtime.enter().is_none());
    assert_eq!(runtime.health(true, "closed").in_flight_requests, 1);
    drop(permit);
    let health = runtime.health(true, "closed");
    assert_eq!(health.phase, "draining");
    assert_eq!(health.in_flight_requests, 0);
    let json = serde_json::to_string(&health).expect("sensors");
    for forbidden in ["postgres://", "recipient", "raw_mime", "next_token", "queue_url"] {
        assert!(!json.contains(forbidden));
    }
}
