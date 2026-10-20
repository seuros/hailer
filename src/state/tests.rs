use super::*;
use crate::feedback::test_support::{ADDRESS, hard_bounce};

#[test]
fn suppressions_events_and_deduplication_survive_restart() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("state.redb");
    let event = hard_bounce("notification-1");
    {
        let store = Store::open(&path).expect("state");
        assert!(store.record(&event).expect("commit"));
    }
    let store = Store::open(&path).expect("reopened state");
    assert!(!store.record(&event).expect("duplicate"));
    assert_eq!(store.events(0, 100).expect("events").events.len(), 1);
    assert_eq!(
        store.blocked(&[ADDRESS.into(), "USER@example.com".into()]).expect("suppressed").len(),
        1
    );
}

#[test]
fn invalid_suppression_rolls_back_event_and_deduplication_key() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("state.redb")).expect("state");
    let mut event = hard_bounce("notification-1");
    event.recipients.push("invalid-address".into());
    assert!(store.record(&event).is_err());
    assert!(store.events(0, 100).expect("events").events.is_empty());
    assert!(store.blocked(&[ADDRESS.into()]).expect("suppressions").is_empty());
    event.recipients.pop();
    assert!(store.record(&event).expect("retry valid event"));
}

#[test]
fn independent_consumers_can_page_events_without_destructive_reads() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("state.redb")).expect("state");
    for id in ["first", "second", "third"] {
        store.record(&hard_bounce(id)).expect("commit");
    }
    let first = store.events(0, 2).expect("first page");
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.next_cursor, 2);
    let second = store.events(first.next_cursor, 2).expect("second page");
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.next_cursor, 3);
    let empty = store.events(3, 2).expect("empty page");
    assert!(empty.events.is_empty());
    assert_eq!(empty.next_cursor, 3);
    assert_eq!(store.events(0, 100).expect("other consumer").events.len(), 3);
    assert!(store.events(0, 0).is_err());
    assert!(store.events(0, 101).is_err());
}

fn imported(email: &str, reason: SuppressionReason) -> Suppression {
    Suppression {
        email_address: email.into(),
        reason,
        suppressed_at: "2026-10-19T12:00:00Z".into(),
        originating_message_id: Some("old-message".into()),
        source_event_id: None,
        metadata: serde_json::json!({"imported": true}),
    }
}

#[test]
fn import_is_atomic_and_preserves_commonshare_case_and_alias_matching() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("state.redb")).expect("state");
    assert!(
        store
            .import(vec![
                imported("USER+old@example.com", SuppressionReason::Bounce),
                imported("invalid", SuppressionReason::Complaint),
            ])
            .is_err()
    );
    assert!(store.blocked(&[ADDRESS.into()]).expect("state").is_empty());
    assert_eq!(
        store
            .import(vec![imported("USER+old@example.com", SuppressionReason::Bounce)])
            .expect("import"),
        1
    );
    assert!(store.blocked(&[ADDRESS.into()]).expect("lookup").contains("user@example.com"));
}

#[test]
fn imports_cannot_downgrade_complaints() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("state.redb")).expect("state");
    store.import(vec![imported(ADDRESS, SuppressionReason::Complaint)]).expect("complaint");
    store.record(&hard_bounce("later-bounce")).expect("bounce");
    store.import(vec![imported(ADDRESS, SuppressionReason::Bounce)]).expect("old import");
    let transaction = store.0.begin_read().expect("read");
    let table = transaction.open_table(SUPPRESSIONS).expect("suppressions");
    let record: Suppression = serde_json::from_str(
        table.get("user@example.com").expect("lookup").expect("record").value(),
    )
    .expect("suppression");
    assert_eq!(record.reason, SuppressionReason::Complaint);
    assert_eq!(record.metadata["imported"], true);
}

#[test]
fn quoted_local_parts_are_not_mistaken_for_plus_aliases() {
    assert_eq!(normalize(r#""a+b"@example.com"#).expect("quoted mailbox"), r#""a+b"@example.com"#);
    assert_eq!(normalize(r#""a@b"@example.com"#).expect("quoted at"), r#""a@b"@example.com"#);
}
