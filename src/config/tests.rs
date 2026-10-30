use super::*;

fn parse(raw: &str) -> Result<Config, String> {
    let config: Config = toml::from_str(raw).map_err(|error| error.to_string())?;
    config.validate().map_err(|error| error.to_string())?;
    Ok(config)
}

#[test]
fn ses_is_the_provider_unless_another_is_named() {
    let config = parse("").expect("defaults");
    assert_eq!(config.provider, ProviderKind::Ses);
    assert!(config.postmark.is_none());
}

#[test]
fn postmark_defaults_to_the_outbound_stream() {
    let config = parse("provider = \"postmark\"").expect("postmark");
    assert_eq!(config.provider, ProviderKind::Postmark);
    let stream = config.postmark.unwrap_or_default().message_stream;
    assert_eq!(stream, "outbound");

    let config = parse("provider = \"postmark\"\n[postmark]\nmessage_stream = \"receipts-2\"")
        .expect("custom stream");
    assert_eq!(config.postmark.expect("section").message_stream, "receipts-2");
}

#[test]
fn provider_settings_must_match_the_provider() {
    const FEEDBACK: &str = "[feedback]\nqueue_url = \"https://sqs.us-east-1.amazonaws.com/123456789012/q\"\ntopic_arn = \"arn:aws:sns:us-east-1:123456789012:t\"";
    let feedback = format!("provider = \"postmark\"\n{FEEDBACK}");
    let cases = [
        ("[postmark]\nmessage_stream = \"outbound\"", "postmark"),
        ("provider = \"postmark\"\nses_configuration_set = \"hailer\"", "ses_configuration_set"),
        (feedback.as_str(), "feedback"),
        ("provider = \"postmark\"\n[suppression_sync]", "suppression_sync"),
        (
            "provider = \"postmark\"\n[rate_limit]\nrecipients_per_second = 5.0\nburst = 10\ndiscover_from_aws = true",
            "rate_limit.discover_from_aws",
        ),
        ("provider = \"postmark\"\n[postmark]\nmessage_stream = \"\"", "postmark.message_stream"),
        (
            "provider = \"postmark\"\n[postmark]\nmessage_stream = \"bad stream\"",
            "postmark.message_stream",
        ),
    ];
    for (raw, field) in cases {
        let error = parse(raw).expect_err(raw);
        assert!(error.contains(field), "{raw:?} -> {error}");
    }
    parse("provider = \"postmark\"\n[rate_limit]\nrecipients_per_second = 5.0\nburst = 10")
        .expect("a fixed local rate limit works with any provider");
}
