use base64::Engine;

use super::*;

fn request(raw: &str, to: &[&str]) -> SendRequest {
    SendRequest {
        raw_mime: raw.to_owned(),
        from_email_address: None,
        to_addresses: to.iter().map(|address| (*address).to_owned()).collect(),
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

fn json(request: &SendRequest, correlation: Option<&Correlation>) -> serde_json::Value {
    let body = render(request, "outbound", correlation).expect("renders");
    serde_json::from_slice(&body).expect("JSON")
}

const RAILS: &[&str] = &[
    "From: =?UTF-8?Q?Ann_=C3=89lise?= <ann@example.dev>",
    "To: \"Bob Builder\" <bob@example.dev>, extra@example.dev",
    "Cc: Carol <carol@example.dev>",
    "Reply-To: support@example.dev",
    "Subject: =?UTF-8?Q?Caf=C3=A9_receipt?=",
    "Date: Thu, 29 Oct 2026 10:00:00 +0000",
    "Message-ID: <abc123@example.dev>",
    "MIME-Version: 1.0",
    "List-Unsubscribe: <https://example.dev/u/1>,",
    " <mailto:unsub@example.dev>",
    "X-Mailer: rails",
    "Content-Type: multipart/mixed; boundary=\"mixed\"",
    "",
    "--mixed",
    "Content-Type: multipart/related; boundary=\"related\"",
    "",
    "--related",
    "Content-Type: multipart/alternative; boundary=\"alt\"",
    "",
    "--alt",
    "Content-Type: text/plain; charset=UTF-8",
    "",
    "Hello Bob",
    "--alt",
    "Content-Type: text/html; charset=UTF-8",
    "",
    "<p>Hello <img src=\"cid:logo@example.dev\"></p>",
    "--alt--",
    "--related",
    "Content-Type: image/png",
    "Content-ID: <logo@example.dev>",
    "Content-Disposition: inline; filename=\"logo.png\"",
    "Content-Transfer-Encoding: base64",
    "",
    "iVBORw0KGgo=",
    "--related--",
    "--mixed",
    "Content-Type: application/pdf; name=\"receipt.pdf\"",
    "Content-Disposition: attachment; filename=\"receipt.pdf\"",
    "Content-Transfer-Encoding: base64",
    "",
    "JVBERi0xLjQK",
    "--mixed--",
    "",
];

#[test]
fn a_rails_message_becomes_postmark_fields() {
    let mut request = request(&RAILS.join("\r\n"), &["bob@example.dev", "dave@example.dev"]);
    request.cc_addresses = vec!["carol@example.dev".into()];
    request.bcc_addresses = vec!["eve@example.dev".into()];
    let body = json(&request, None);

    assert_eq!(body["From"], "\"Ann Élise\" <ann@example.dev>");
    assert_eq!(
        body["To"], "\"Bob Builder\" <bob@example.dev>, dave@example.dev",
        "the envelope decides who is mailed; the header only lends names"
    );
    assert_eq!(body["Cc"], "\"Carol\" <carol@example.dev>");
    assert_eq!(body["Bcc"], "eve@example.dev");
    assert_eq!(body["ReplyTo"], "support@example.dev");
    assert_eq!(body["Subject"], "Café receipt");
    assert_eq!(body["TextBody"].as_str().map(str::trim), Some("Hello Bob"));
    assert_eq!(
        body["HtmlBody"].as_str().map(str::trim),
        Some("<p>Hello <img src=\"cid:logo@example.dev\"></p>")
    );
    assert_eq!(body["MessageStream"], "outbound");
    assert!(body.get("Metadata").is_none());

    assert_eq!(
        body["Attachments"],
        serde_json::json!([
            {
                "Name": "logo.png",
                "Content": "iVBORw0KGgo=",
                "ContentType": "image/png",
                "ContentID": "cid:logo@example.dev",
            },
            {
                "Name": "receipt.pdf",
                "Content": "JVBERi0xLjQK",
                "ContentType": "application/pdf",
            },
        ])
    );
    assert_eq!(
        body["Headers"],
        serde_json::json!([
            {"Name": "Message-ID", "Value": "<abc123@example.dev>"},
            {"Name": "List-Unsubscribe", "Value": "<https://example.dev/u/1>, <mailto:unsub@example.dev>"},
            {"Name": "X-Mailer", "Value": "rails"},
        ])
    );
}

#[test]
fn a_single_part_body_keeps_its_own_type() {
    for (content_type, field, absent) in
        [("text/plain", "TextBody", "HtmlBody"), ("text/html", "HtmlBody", "TextBody")]
    {
        let raw = format!(
            "From: ann@example.dev\r\nTo: bob@example.dev\r\nContent-Type: {content_type}\r\n\r\n<p>Hi</p>"
        );
        let body = json(&request(&raw, &["bob@example.dev"]), None);
        assert_eq!(body[field], "<p>Hi</p>", "{content_type}");
        assert!(body.get(absent).is_none(), "{content_type} must not invent a {absent}");
        assert!(body.get("Attachments").is_none(), "{content_type}");
    }
}

#[test]
fn a_queued_attempt_carries_its_correlation_as_metadata() {
    let correlation = crate::provider::test_support::correlation();
    let raw = "From: ann@example.dev\r\nTo: bob@example.dev\r\n\r\nHi";
    let body = json(&request(raw, &["bob@example.dev"]), Some(&correlation));
    assert_eq!(
        body["Metadata"],
        serde_json::json!({
            "hailer_message_id": correlation.hailer_message_id,
            "hailer_attempt_id": correlation.attempt_id,
        })
    );
}

#[test]
fn the_envelope_sender_stands_in_only_for_a_missing_from_header() {
    let mut without_from = request("To: bob@example.dev\r\n\r\nHi", &["bob@example.dev"]);
    assert_eq!(render(&without_from, "outbound", None), Err(RenderError::NoSender));
    without_from.from_email_address = Some("bounces@example.dev".into());
    assert_eq!(json(&without_from, None)["From"], "bounces@example.dev");

    let mut with_from = request(
        "From: Ann <ann@example.dev>\r\nTo: bob@example.dev\r\n\r\nHi",
        &["bob@example.dev"],
    );
    with_from.from_email_address = Some("bounces@example.dev".into());
    assert_eq!(json(&with_from, None)["From"], "\"Ann\" <ann@example.dev>");
}

#[test]
fn messages_postmark_cannot_take_are_refused_before_sending() {
    let pdf_only = "From: ann@example.dev\r\nTo: bob@example.dev\r\nContent-Type: application/pdf\r\n\r\nJVBERi0xLjQK";
    let payload = base64::engine::general_purpose::STANDARD.encode(vec![0_u8; 8 * 1024 * 1024]);
    let oversized = format!(
        "From: ann@example.dev\r\nTo: bob@example.dev\r\nContent-Type: multipart/mixed; boundary=b\r\n\r\n\
         --b\r\nContent-Type: text/plain\r\n\r\nHi\r\n\
         --b\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\n{payload}\r\n--b--\r\n"
    );
    for (raw, error) in [
        ("garbage", RenderError::Unparsable),
        (pdf_only, RenderError::NoBody),
        (oversized.as_str(), RenderError::TooLarge),
    ] {
        assert_eq!(render(&request(raw, &["bob@example.dev"]), "outbound", None), Err(error));
    }
}

#[test]
fn display_names_are_quoted_and_cannot_break_a_header() {
    for (name, address, expected) in [
        (Some("Ann"), "ann@example.dev", Some("\"Ann\" <ann@example.dev>")),
        (Some("A \"B\" \\ C"), "ann@example.dev", Some("\"A \\\"B\\\" \\\\ C\" <ann@example.dev>")),
        (
            Some("Line\r\nBcc: x@example.dev"),
            "ann@example.dev",
            Some("\"Line  Bcc: x@example.dev\" <ann@example.dev>"),
        ),
        (Some("  "), "ann@example.dev", Some("ann@example.dev")),
        (None, "ann@example.dev", Some("ann@example.dev")),
        (Some("Ann"), "", None),
    ] {
        assert_eq!(mailbox_string(name, address).as_deref(), expected, "{name:?} {address:?}");
    }
}
