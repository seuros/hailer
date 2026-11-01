//! `SESv2`, REST-JSON: sending, the account, identities, the suppression list.

use std::time::Duration;

use serde::Deserialize;

use super::{Aws, AwsError, Call, CallError, Response};
use crate::web::percent_encode;

pub const SEND_TIMEOUT: Duration = Duration::from_secs(20);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn call(method: &'static str, path: String, query: Vec<(String, String)>, body: Vec<u8>) -> Call {
    Call {
        service: "ses",
        prefix: "email",
        method,
        path,
        query,
        content_type: (!body.is_empty()).then_some("application/json"),
        target: None,
        body,
        timeout: if method == "GET" { READ_TIMEOUT } else { SEND_TIMEOUT },
    }
}

/// `SendEmail` within `timeout`; the caller classifies the answer.
pub async fn send_email(
    aws: &Aws,
    body: Vec<u8>,
    timeout: Duration,
) -> Result<Response, CallError> {
    let call = Call { timeout, ..call("POST", "/v2/email/outbound-emails".into(), vec![], body) };
    aws.call(call).await
}

/// One `ListSuppressedDestinations` page; the caller classifies the answer.
pub async fn suppressed_destinations(
    aws: &Aws,
    next_token: Option<&str>,
) -> Result<Response, CallError> {
    let mut query = vec![("PageSize".to_owned(), "1000".to_owned())];
    if let Some(token) = next_token {
        query.push(("NextToken".to_owned(), token.to_owned()));
    }
    aws.call(call("GET", "/v2/email/suppression/addresses".into(), query, vec![])).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Account {
    pub dedicated_ip_auto_warmup_enabled: Option<bool>,
    pub enforcement_status: Option<String>,
    pub production_access_enabled: Option<bool>,
    pub sending_enabled: Option<bool>,
    pub send_quota: Option<SendQuota>,
}

#[derive(Debug, Deserialize)]
pub struct SendQuota {
    #[serde(rename = "Max24HourSend")]
    pub max_24_hour_send: Option<f64>,
    #[serde(rename = "MaxSendRate")]
    pub max_send_rate: Option<f64>,
    #[serde(rename = "SentLast24Hours")]
    pub sent_last_24_hours: Option<f64>,
}

pub async fn account(aws: &Aws) -> Result<Account, AwsError> {
    aws.read(call("GET", "/v2/email/account".into(), vec![], vec![]), "GetAccount").await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Identity {
    pub verified_for_sending_status: Option<bool>,
    pub verification_status: Option<String>,
}

pub async fn identity(aws: &Aws, identity: &str) -> Result<Identity, AwsError> {
    let path = format!("/v2/email/identities/{}", percent_encode(identity));
    aws.read(call("GET", path, vec![], vec![]), "GetEmailIdentity").await
}
