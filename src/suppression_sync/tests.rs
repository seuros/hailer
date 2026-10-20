use std::collections::VecDeque;
use std::sync::Mutex;

use sqlx::Row;

use super::*;
use crate::feedback::SuppressionReason;

type Step = (Option<String>, Result<Page, ReadError>);

struct Fake {
    scope: &'static str,
    steps: Mutex<VecDeque<Step>>,
}

impl Fake {
    fn new(steps: Vec<Step>) -> Self {
        Self { scope: "ses:123456789012:us-east-1", steps: Mutex::new(steps.into()) }
    }
}

#[async_trait]
impl Source for Fake {
    fn scope(&self) -> &str {
        self.scope
    }

    async fn page(&self, token: Option<&str>) -> Result<Page, ReadError> {
        let (expected, result) =
            self.steps.lock().expect("steps").pop_front().expect("unexpected read");
        assert_eq!(token, expected.as_deref());
        result
    }
}

fn record(address: &str, reason: SuppressionReason) -> Suppression {
    Suppression {
        email_address: address.into(),
        reason,
        suppressed_at: "2026-10-20T12:00:00Z".into(),
        originating_message_id: None,
        source_event_id: None,
        metadata: serde_json::json!({"source": "aws_ses_account"}),
    }
}

fn page(records: Vec<Suppression>, next: Option<&str>) -> Result<Page, ReadError> {
    Ok(Page { records, next_token: next.map(str::to_owned) })
}

#[tokio::test]
async fn paginated_merge_promotes_complaints_and_never_fabricates_feedback() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![
        (
            None,
            page(vec![record("USER+old@example.com", SuppressionReason::Bounce)], Some("page2")),
        ),
        (
            Some("page2".into()),
            page(vec![record("user@example.com", SuppressionReason::Complaint)], None),
        ),
    ]);
    let report = run(&source, &store, false).await.expect("sync");
    assert_eq!(report.pages, 2);
    assert_eq!(report.merged, 2);
    assert!(report.completed);
    let reason: String = sqlx::query_scalar("SELECT reason FROM hailer.suppressions")
        .fetch_one(&store.pool)
        .await
        .expect("reason");
    assert_eq!(reason, "complaint");
    assert!(store.events(0, 100).await.expect("no fake feedback").events.is_empty());
    let empty = Fake::new(vec![(None, page(vec![], None))]);
    run(&empty, &store, false).await.expect("new full reconciliation");
    assert!(
        store
            .blocked(&["user@example.com".into()])
            .await
            .expect("retained")
            .contains("user@example.com")
    );
}

#[tokio::test]
async fn failed_page_resumes_from_committed_token_and_account_scope_isolated() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![
        (None, page(vec![record("one@example.com", SuppressionReason::Bounce)], Some("page2"))),
        (Some("page2".into()), Err(ReadError::Rejected)),
    ]);
    assert!(run(&source, &store, false).await.is_err());
    let checkpoint = store.checkpoint(source.scope()).await.expect("checkpoint").expect("source");
    assert_eq!(checkpoint.next_token.as_deref(), Some("page2"));
    assert_eq!(checkpoint.imported, 1);
    let resumed = Fake::new(vec![(
        Some("page2".into()),
        page(vec![record("two@example.com", SuppressionReason::Complaint)], None),
    )]);
    assert_eq!(run(&resumed, &store, false).await.expect("resume").merged, 2);
    let mut region = Fake::new(vec![(None, page(vec![], None))]);
    region.scope = "ses:123456789012:eu-west-1";
    assert_eq!(run(&region, &store, false).await.expect("separate scope").merged, 0);
}

#[tokio::test]
async fn page_failure_rolls_back_suppressions_and_checkpoint_together() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    sqlx::query("ALTER TABLE hailer.suppressions ADD CONSTRAINT fixture CHECK (email_address <> 'bad@example.com')")
        .execute(&store.pool).await.expect("failure fixture");
    let source = Fake::new(vec![(
        None,
        page(
            vec![
                record("good@example.com", SuppressionReason::Bounce),
                record("bad@example.com", SuppressionReason::Complaint),
            ],
            None,
        ),
    )]);
    assert!(run(&source, &store, false).await.is_err());
    let checkpoint = store.checkpoint(source.scope()).await.expect("checkpoint").expect("source");
    assert_eq!(checkpoint.imported, 0);
    assert!(checkpoint.next_token.is_none());
    assert!(!checkpoint.completed);
    assert!(store.blocked(&["good@example.com".into()]).await.expect("rollback").is_empty());
}

#[tokio::test]
async fn repeated_tokens_fail_before_advancing_or_importing_the_bad_page() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![
        (None, page(vec![], Some("same"))),
        (
            Some("same".into()),
            page(vec![record("bad@example.com", SuppressionReason::Bounce)], Some("same")),
        ),
    ]);
    assert!(run(&source, &store, false).await.expect_err("loop").to_string().contains("repeated"));
    assert!(store.blocked(&["bad@example.com".into()]).await.expect("not imported").is_empty());
    assert_eq!(
        store
            .checkpoint(source.scope())
            .await
            .expect("checkpoint")
            .expect("source")
            .next_token
            .as_deref(),
        Some("same")
    );
}

#[tokio::test]
async fn expired_tokens_require_explicit_restart_without_unsuppressing_anyone() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![
        (
            None,
            page(vec![record("old@example.com", SuppressionReason::Complaint)], Some("expired")),
        ),
        (Some("expired".into()), Err(ReadError::InvalidToken)),
    ]);
    assert!(
        run(&source, &store, false).await.expect_err("expired").to_string().contains("--restart")
    );
    let restarted = Fake::new(vec![(None, page(vec![], None))]);
    assert!(run(&restarted, &store, true).await.expect("restart").completed);
    assert!(
        store
            .blocked(&["old@example.com".into()])
            .await
            .expect("retain local policy")
            .contains("old@example.com")
    );
}

#[tokio::test]
async fn only_safe_read_failures_are_retried() {
    let source = Fake::new(vec![
        (None, Err(ReadError::Unavailable)),
        (None, Err(ReadError::Unavailable)),
        (None, page(vec![], None)),
    ]);
    assert!(read_page(&source, None).await.is_ok());
    let rejected = Fake::new(vec![(None, Err(ReadError::Rejected))]);
    assert!(matches!(read_page(&rejected, None).await, Err(ReadError::Rejected)));
}

#[tokio::test]
async fn aliases_in_one_bulk_page_cannot_downgrade_a_complaint() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![(
        None,
        page(
            vec![
                record("user+one@example.com", SuppressionReason::Bounce),
                record("user+two@example.com", SuppressionReason::Complaint),
                record("USER@example.com", SuppressionReason::Bounce),
            ],
            None,
        ),
    )]);
    run(&source, &store, false).await.expect("coalesced bulk upsert");
    let row =
        sqlx::query("SELECT count(*) AS count, min(reason) AS reason FROM hailer.suppressions")
            .fetch_one(&store.pool)
            .await
            .expect("suppression");
    assert_eq!(row.get::<i64, _>("count"), 1);
    assert_eq!(row.get::<String, _>("reason"), "complaint");
}

#[tokio::test]
async fn replicas_cannot_walk_the_same_source_checkpoint_concurrently() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("state");
    let source = Fake::new(vec![]);
    let lock = PgAdvisoryLock::new(format!("hailer:suppression-sync:{}", source.scope()));
    let guard =
        lock.acquire(store.pool.acquire().await.expect("connection").detach()).await.expect("lock");
    assert!(
        run(&source, &store, false)
            .await
            .expect_err("busy")
            .to_string()
            .contains("already running")
    );
    guard.release_now().await.expect("release").close().await.expect("close");
    let next = Fake::new(vec![(None, page(vec![], None))]);
    assert!(run(&next, &store, false).await.expect("next owner").completed);
}
