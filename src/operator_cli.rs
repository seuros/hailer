use anyhow::ensure;

use crate::outbound::{Resolution, valid_id};
use crate::state::Store;
use crate::{CancelArgs, Command, MessageIdArgs};

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
        _ => return Ok(false),
    }
    Ok(true)
}
