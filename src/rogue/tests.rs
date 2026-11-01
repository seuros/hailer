use super::*;

const MS: Duration = Duration::from_millis(1);

fn armed(target: Target, fault: Fault, remaining: Duration) -> Armed {
    Armed { target, fault, fraction: Fraction::ALL, remaining }
}

#[test]
fn an_armed_target_injects_while_every_other_target_passes() {
    let experiments = Experiments::default();
    assert_eq!(experiments.fault(Target::Provider), None);
    experiments.apply(&[armed(Target::Provider, Fault::Raise, Duration::from_secs(60))]);
    assert_eq!(experiments.fault(Target::Provider), Some(Fault::Raise));
    assert_eq!(experiments.fault(Target::Feedback), None);
    assert_eq!(experiments.fault(Target::Verifier), None);
    experiments.apply(&[]);
    assert_eq!(experiments.fault(Target::Provider), None);
}

#[test]
fn an_experiment_disarms_itself_once_its_remaining_time_is_spent() {
    let experiments = Experiments::default();
    experiments.apply(&[armed(Target::Feedback, Fault::Garble, MS)]);
    std::thread::sleep(5 * MS);
    assert_eq!(experiments.fault(Target::Feedback), None);
}

#[tokio::test]
async fn a_delay_is_spent_inside_the_timeout_and_one_as_long_times_the_call_out() {
    let experiments = Experiments::default();
    let delay = Fault::Delay { ns: crate::budget::nanos(10 * MS) };
    experiments.apply(&[armed(Target::Verifier, delay, Duration::from_secs(60))]);
    assert_eq!(experiments.inject(Target::Verifier, 50 * MS).await, Injected::Pass(40 * MS));
    assert_eq!(experiments.inject(Target::Verifier, 10 * MS).await, Injected::TimedOut);
    assert_eq!(experiments.inject(Target::Provider, 10 * MS).await, Injected::Pass(10 * MS));
}

#[tokio::test]
async fn stored_experiments_replace_per_target_expire_and_refuse_unsafe_rows() {
    let database = crate::test_support::database().await;
    let store = Store::connect(database.url(), 5).await.expect("store");
    let fraction = Fraction::new(500_000).expect("half");
    let hour = Duration::from_hours(1);
    store.arm_experiment(Target::Provider, Fault::Raise, fraction, hour).await.expect("arm");
    let delay = Fault::Delay { ns: crate::budget::nanos(250 * MS) };
    store.arm_experiment(Target::Provider, delay, fraction, hour).await.expect("replace");
    store.arm_experiment(Target::Feedback, Fault::Garble, Fraction::ALL, MS).await.expect("arm");
    tokio::time::sleep(10 * MS).await;
    let listed = store.experiments().await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (listed[0].target, listed[0].fault, listed[0].fraction),
        (Target::Provider, delay, fraction)
    );
    assert!(listed[0].remaining > Duration::from_mins(59) && listed[0].remaining <= hour);
    assert!(store.arm_experiment(Target::Verifier, Fault::Garble, fraction, hour).await.is_err());
    assert!(
        store.arm_experiment(Target::Provider, Fault::Raise, fraction, 25 * hour).await.is_err()
    );
    assert_eq!(store.disarm_experiments(Some(Target::Feedback)).await.expect("expired"), 0);
    assert_eq!(store.disarm_experiments(None).await.expect("disarm all"), 1);
    assert_eq!(store.experiments().await.expect("empty"), Vec::new());
}

#[cfg(feature = "ses")]
mod ses {
    use std::assert_matches;
    use std::sync::Arc;

    use super::*;
    use crate::aws::{Aws, Credentials};
    use crate::mail::SendRequest;
    use crate::provider::queued::{DispatchOutcome, QueuedProvider};
    use crate::provider::ses::SesProvider;
    use crate::provider::test_support::{Endpoint, correlation, endpoint, request};
    use crate::provider::{MailProvider, ProviderError};

    fn aws(server: &Endpoint) -> Arc<Aws> {
        let credentials =
            Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", None);
        Arc::new(Aws::fixed(&server.url, "us-east-1", credentials))
    }

    fn rogue(target: Target, fault: Fault) -> Arc<Experiments> {
        let experiments = Arc::new(Experiments::default());
        experiments.apply(&[armed(target, fault, Duration::from_secs(60))]);
        experiments
    }

    async fn send(provider: &SesProvider, request: &SendRequest) -> Result<String, ProviderError> {
        provider.send(request, deadline_machines::Deadline::never()).await
    }

    #[tokio::test]
    async fn a_raised_provider_fault_sends_nothing_on_either_path() {
        let server = endpoint(200, r#"{"MessageId":"real-id"}"#, false).await;
        let provider = SesProvider::new(aws(&server), None)
            .with_experiments(rogue(Target::Provider, Fault::Raise));
        let error = send(&provider, &request()).await.expect_err("injected");
        assert!(error.to_string().contains("rogue experiment"), "{error}");
        assert_eq!(
            provider.dispatch(&request(), &correlation()).await,
            DispatchOutcome::NotDispatched("request_not_dispatched")
        );
        assert_eq!(server.calls(), 0);
        server.stop().await;
    }

    #[tokio::test]
    async fn a_garbled_provider_still_sends_but_reports_a_traceable_id() {
        let server = endpoint(200, r#"{"MessageId":"real-id"}"#, false).await;
        let provider = SesProvider::new(aws(&server), None)
            .with_experiments(rogue(Target::Provider, Fault::Garble));
        assert_eq!(send(&provider, &request()).await.expect("sent"), "rogue-real-id");
        assert_eq!(server.calls(), 1);
        server.stop().await;
    }

    #[tokio::test]
    async fn a_delay_past_the_send_timeout_is_an_unknown_outcome_without_a_send() {
        let server = endpoint(200, r#"{"MessageId":"real-id"}"#, false).await;
        let delay = Fault::Delay { ns: u64::MAX };
        let provider =
            SesProvider::new(aws(&server), None).with_experiments(rogue(Target::Provider, delay));
        let deadline = crate::budget::after(crate::provider::SEND_FLOOR + 100 * MS);
        assert_matches!(provider.send(&request(), deadline).await, Err(ProviderError::Ambiguous));
        assert_eq!(server.calls(), 0);
        server.stop().await;
    }
}
