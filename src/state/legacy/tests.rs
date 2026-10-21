use super::*;
use crate::feedback::SuppressionReason;
use crate::feedback::test_support::hard_bounce;

fn fixture(path: &Path, corrupt: bool) -> anyhow::Result<()> {
    let database = redb::Database::create(path)?;
    let transaction = database.begin_write()?;
    {
        let mut events = transaction.open_table(EVENTS)?;
        let mut seen = transaction.open_table(SEEN)?;
        let mut suppressions = transaction.open_table(SUPPRESSIONS)?;
        // More than a batch: corruption at the end must undo flushed batches.
        for sequence in 1..=257 {
            let event = hard_bounce(&format!("legacy-{sequence}"));
            events.insert(sequence, serde_json::to_string(&event)?.as_str())?;
            seen.insert(
                event.event_id.as_str(),
                if corrupt && sequence == 257 { 999 } else { sequence },
            )?;
        }
        let record = Suppression {
            email_address: "user@example.com".into(),
            reason: SuppressionReason::Complaint,
            suppressed_at: "2026-10-20T12:00:00Z".into(),
            originating_message_id: Some("<old@example.com>".into()),
            source_event_id: Some("old-complaint".into()),
            metadata: serde_json::json!({"keep": true}),
        };
        suppressions.insert("user@example.com", serde_json::to_string(&record)?.as_str())?;
    }
    transaction.commit()?;
    Ok(())
}

#[tokio::test]
async fn migration_preserves_source_dedup_history_and_cursor_continuity() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("legacy.redb");
    fixture(&path, false).expect("source");
    let original = std::fs::read(&path).expect("source bytes");
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let report = store.migrate_redb(&path).await.expect("migration");
    assert_eq!(report.events, 257);
    assert_eq!(report.suppressions, 1);
    assert_eq!(report.last_sequence, 257);
    assert_eq!(std::fs::read(&path).expect("source unchanged"), original);
    let page = store.events(255, 100).await.expect("page");
    assert_eq!(page.events.iter().map(|event| event.sequence).collect::<Vec<_>>(), [256, 257]);
    assert!(!store.record(&hard_bounce("legacy-257")).await.expect("dedup"));
    store.record(&hard_bounce("new-feedback")).await.expect("new feedback");
    assert_eq!(store.events(257, 100).await.expect("continued").next_cursor, 258);
    let Json(metadata): Json<serde_json::Value> = sqlx::query_scalar(
        "SELECT metadata FROM hailer.suppressions WHERE email_address = 'user@example.com'",
    )
    .fetch_one(&store.pool)
    .await
    .expect("metadata");
    assert_eq!(metadata["keep"], true);
    assert!(store.migrate_redb(&path).await.expect_err("nonempty").to_string().contains("empty"));
}

#[tokio::test]
async fn corruption_after_a_flushed_batch_rolls_back_every_destination_change() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("corrupt.redb");
    fixture(&path, true).expect("source");
    let original = std::fs::read(&path).expect("bytes");
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    assert!(store.migrate_redb(&path).await.expect_err("corrupt").to_string().contains("mapping"));
    assert!(store.events(0, 100).await.expect("rollback").events.is_empty());
    let value: i64 = sqlx::query_scalar("SELECT value FROM hailer.event_counter")
        .fetch_one(&store.pool)
        .await
        .expect("counter");
    assert_eq!(value, 0);
    assert_eq!(std::fs::read(&path).expect("unchanged"), original);
    let missing = directory.path().join("missing.redb");
    assert!(store.migrate_redb(&missing).await.is_err());
    assert!(!missing.exists());
}
