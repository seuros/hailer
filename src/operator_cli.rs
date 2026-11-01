#[cfg(test)]
mod tests;

use std::time::Duration;

use anyhow::ensure;

use rogue_machines::{Fault, Fraction, PER_MILLION};

use crate::outbound::{Resolution, valid_id};
use crate::rogue::{Armed, MAX_EXPIRY, Target};
use crate::state::Store;
use crate::verify::emailable::{BatchStatus, EmailableClient};
use crate::{
    ArmArgs, BanDomainArgs, BatchStatusArgs, CancelArgs, Command, DisarmArgs, MessageIdArgs,
    UnbanDomainArgs, VerifyBatchArgs,
};

fn target(name: &str) -> anyhow::Result<Target> {
    Target::parse(name)
        .ok_or_else(|| anyhow::anyhow!("target must be provider, feedback or verifier"))
}

fn experiment(options: &ArmArgs) -> anyhow::Result<Armed> {
    let target = target(&options.target)?;
    let fault = match (options.fault.as_str(), options.delay_ms) {
        ("raise", None) => Fault::Raise,
        ("garble", None) if target != Target::Verifier => Fault::Garble,
        ("delay", Some(ms)) if ms > 0 => {
            Fault::Delay { ns: crate::budget::nanos(Duration::from_millis(ms)) }
        }
        _ => anyhow::bail!(
            "fault must be raise, garble (not for verifier) or delay with a positive --delay-ms"
        ),
    };
    ensure!(
        options.percent > 0.0 && options.percent <= 100.0,
        "--percent must be above 0 and at most 100"
    );
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "bounded to 1..=PER_MILLION by the check above"
    )]
    let millionths = (options.percent / 100.0 * f64::from(PER_MILLION)).round().max(1.0) as u32;
    let remaining = Duration::from_secs(options.expires_in_seconds);
    ensure!(
        !remaining.is_zero() && remaining <= MAX_EXPIRY,
        "--expires-in-seconds must be in 1..86400"
    );
    Ok(Armed {
        target,
        fault,
        fraction: Fraction::new(millionths).ok_or_else(|| anyhow::anyhow!("percent too large"))?,
        remaining,
    })
}

/// `EMAILABLE_API_KEY` is read here, not from `hailer.toml` - same rule as
/// the real-time guard's own `build_verifier` in main.rs. Independent of
/// whether `[email_verification]` is configured; an operator may want to
/// pre-screen a list without the live guard enabled yet.
fn emailable_client() -> anyhow::Result<EmailableClient> {
    let api_key = std::env::var("EMAILABLE_API_KEY")
        .map_err(|_| anyhow::anyhow!("set EMAILABLE_API_KEY to use batch verification"))?;
    Ok(EmailableClient::new(api_key, 5))
}

/// Caches every verdict (best-effort - a cache write failure never fails
/// the command, the batch result itself is still reported) and builds the
/// per-address summary for the CLI's JSON output.
async fn cache_batch_verdicts(
    store: &Store,
    verdicts: &[(String, crate::verify::Verdict)],
    ttl: Duration,
) -> (usize, serde_json::Map<String, serde_json::Value>) {
    let mut cached = 0;
    let mut results = serde_json::Map::new();
    for (address, verdict) in verdicts {
        if store.cache_verdict(address, verdict, ttl).await.is_ok() {
            cached += 1;
        }
        results.insert(address.clone(), serde_json::json!(verdict.state.as_str()));
    }
    (cached, results)
}

/// Outcome counts for [`dedupe_against_cache`], everything except the
/// addresses actually worth submitting.
#[derive(Default, serde::Serialize)]
struct DedupeReport {
    invalid_lines: usize,
    duplicate_lines: usize,
    already_cached: usize,
}

/// Drops exact duplicate lines (by [`crate::state::normalize`] - the same
/// key the cache itself is keyed by), unparseable lines, and anything that
/// already has a fresh cached verdict, before ever spending a credit on it.
/// Nothing is silently dropped - every exclusion is counted and reported.
async fn dedupe_against_cache(store: &Store, lines: Vec<String>) -> (Vec<String>, DedupeReport) {
    let mut seen = std::collections::HashSet::new();
    let mut report = DedupeReport::default();
    let mut to_submit = Vec::new();
    for address in lines {
        let Ok(key) = crate::state::normalize(&address) else {
            report.invalid_lines += 1;
            continue;
        };
        if !seen.insert(key) {
            report.duplicate_lines += 1;
            continue;
        }
        if matches!(store.cached_verdict(&address).await, Ok(Some(_))) {
            report.already_cached += 1;
            continue;
        }
        to_submit.push(address);
    }
    (to_submit, report)
}

async fn submit_verify_batch(store: &Store, file: &str) -> anyhow::Result<()> {
    let lines: Vec<String> = std::fs::read_to_string(file)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    let read = lines.len();
    let (to_submit, report) = dedupe_against_cache(store, lines).await;
    if to_submit.is_empty() {
        println!(
            "{}",
            serde_json::json!({
                "read": read, "report": report, "submitted": 0,
                "note": "nothing left to submit after dropping duplicates/invalid/already-cached addresses",
            })
        );
        return Ok(());
    }
    let id = emailable_client()?.submit_batch(&to_submit).await?;
    println!(
        "{}",
        serde_json::json!({"read": read, "report": report, "submitted": to_submit.len(), "batch_id": id})
    );
    Ok(())
}

async fn print_batch_status(store: &Store, id: &str, cache_ttl_seconds: u64) -> anyhow::Result<()> {
    match emailable_client()?.batch_status(id).await? {
        BatchStatus::Processing { processed, total } => {
            println!(
                "{}",
                serde_json::json!({"status": "processing", "processed": processed, "total": total})
            );
        }
        BatchStatus::Complete { verdicts } => {
            let ttl = Duration::from_secs(cache_ttl_seconds);
            let (cached, results) = cache_batch_verdicts(store, &verdicts, ttl).await;
            println!(
                "{}",
                serde_json::json!({
                    "status": "complete", "total": verdicts.len(), "cached": cached,
                    "verdicts": results,
                })
            );
        }
        BatchStatus::TooLargeForInlineResults { download_file, total } => {
            println!(
                "{}",
                serde_json::json!({
                    "status": "too_large_for_inline_results", "total": total,
                    "download_file": download_file,
                    "note": "over 1000 addresses; this client does not fetch/parse the \
                             ZIP/CSV download - results were not cached",
                })
            );
        }
    }
    Ok(())
}

pub async fn run(command: &Command, store: &Store) -> anyhow::Result<bool> {
    match command {
        Command::MessageStatus(MessageIdArgs { id }) => {
            ensure!(valid_id(id), "invalid message ID");
            let message = store
                .message_status(id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("message not found"))?;
            println!("{}", serde_json::to_string(&message)?);
        }
        Command::CancelMessage(CancelArgs { id, reason }) => {
            ensure!(valid_id(id), "invalid message ID");
            store.cancel_message(id, reason).await?;
            println!("{}", serde_json::json!({"cancelled": true, "hailer_message_id": id}));
        }
        Command::ResolveMessage(options) => {
            ensure!(valid_id(&options.id), "invalid message ID");
            let resolution = match options.outcome.as_str() {
                "accepted" => {
                    ensure!(
                        !options.acknowledge_duplicate_risk,
                        "retry acknowledgement is only valid with retry"
                    );
                    Resolution::Accepted(options.provider_message_id.clone().ok_or_else(|| {
                        anyhow::anyhow!("accepted resolution requires --provider-message-id")
                    })?)
                }
                "not-accepted" => {
                    ensure!(
                        options.provider_message_id.is_none()
                            && !options.acknowledge_duplicate_risk,
                        "not-accepted does not take a provider ID or retry acknowledgement"
                    );
                    Resolution::NotAccepted
                }
                "retry" => {
                    ensure!(
                        options.provider_message_id.is_none(),
                        "retry does not take a provider ID"
                    );
                    Resolution::Retry {
                        acknowledge_duplicate_risk: options.acknowledge_duplicate_risk,
                    }
                }
                _ => anyhow::bail!("outcome must be accepted, not-accepted, or retry"),
            };
            if let Some(attempt_id) = &options.attempt_id {
                ensure!(valid_id(attempt_id), "invalid attempt ID");
                store.resolve_attempt(&options.id, attempt_id, resolution, &options.reason).await?;
            } else {
                store.resolve_message(&options.id, resolution, &options.reason).await?;
            }
            println!("{}", serde_json::json!({"resolved": true, "hailer_message_id": options.id}));
        }
        Command::BanDomain(BanDomainArgs { pattern, reason }) => {
            ensure!(!pattern.trim().is_empty(), "pattern must not be empty");
            store.ban_domain(pattern, reason.as_deref()).await?;
            println!("{}", serde_json::json!({"banned": true, "pattern": pattern}));
        }
        Command::UnbanDomain(UnbanDomainArgs { pattern }) => {
            let removed = store.unban_domain(pattern).await?;
            println!("{}", serde_json::json!({"removed": removed, "pattern": pattern}));
        }
        Command::ReleaseQuarantine(CancelArgs { id, reason }) => {
            ensure!(valid_id(id), "invalid message ID");
            store.release_quarantine(id, reason).await?;
            println!("{}", serde_json::json!({"released": true, "hailer_message_id": id}));
        }
        Command::VerifyBatch(VerifyBatchArgs { file }) => {
            submit_verify_batch(store, file).await?;
        }
        Command::BatchStatus(BatchStatusArgs { id, cache_ttl_seconds }) => {
            print_batch_status(store, id, *cache_ttl_seconds).await?;
        }
        Command::EmailableAccount => {
            let account = emailable_client()?.account().await?;
            println!(
                "{}",
                serde_json::json!({
                    "owner_email": account.owner_email,
                    "available_credits": account.available_credits,
                })
            );
        }
        _ => return experiment_command(command, store).await,
    }
    Ok(true)
}

async fn experiment_command(command: &Command, store: &Store) -> anyhow::Result<bool> {
    match command {
        Command::ArmExperiment(options) => {
            let armed = experiment(options)?;
            store
                .arm_experiment(armed.target, armed.fault, armed.fraction, armed.remaining)
                .await?;
            println!("{}", serde_json::json!({"armed": armed.to_json()}));
        }
        Command::DisarmExperiment(DisarmArgs { target: name }) => {
            let disarmed =
                store.disarm_experiments(name.as_deref().map(target).transpose()?).await?;
            println!("{}", serde_json::json!({"disarmed": disarmed}));
        }
        Command::ListExperiments => {
            let armed: Vec<_> =
                store.experiments().await?.into_iter().map(Armed::to_json).collect();
            println!("{}", serde_json::json!({"experiments": armed}));
        }
        _ => return Ok(false),
    }
    Ok(true)
}
