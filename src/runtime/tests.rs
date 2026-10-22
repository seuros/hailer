use super::*;

#[test]
fn health_reflects_bootstrap_storage_failure_and_drain_without_leaking_data() {
    let control = Arc::new(Control::new(None, false).expect("control"));
    let runtime = Runtime::new(control, true, true, None, Arc::new(Intel::Disabled), None);
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

#[test]
fn dispatcher_readiness_requires_a_live_worker_without_changing_sync_readiness() {
    let control = Arc::new(Control::new(None, true).expect("control"));
    let runtime = Runtime::new(
        control,
        false,
        false,
        Some(crate::config::QueuedConfig {
            max_bytes: 1000,
            max_messages: 10,
            unknown_retention_seconds: 604_800,
        }),
        Arc::new(Intel::Disabled),
        None,
    );
    assert!(runtime.health(true, "closed").ready);
    assert!(!runtime.dispatcher_ready(true, "closed"));
    runtime.dispatch_state(true);
    assert!(runtime.dispatcher_ready(true, "closed"));
    assert!(!runtime.dispatcher_ready(true, "open"));
    assert!(!runtime.dispatcher_ready(false, "closed"));
    runtime.dispatch_state(false);
    assert!(!runtime.dispatcher_ready(true, "closed"));
}
