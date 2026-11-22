use super::*;
use crate::provider::test_support::correlation;

fn sample_request() -> SendRequest {
    SendRequest {
        raw_mime: "From: \"Alice\" <alice@example.dev>\r\n\
                   To: bob@example.dev\r\n\
                   Message-ID: <msg-123@example.dev>\r\n\
                   Subject: Hello\r\n\r\n\
                   Body"
            .into(),
        from_email_address: None,
        to_addresses: vec!["bob@example.dev".into()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

#[test]
fn queue_id_extraction_respects_word_boundaries() {
    assert_eq!(extract_queue_id(&["2.0.0 Ok: queued as 4X1a2B3c".into()]), Some("4X1a2B3c".into()));
    assert_eq!(extract_queue_id(&["OK (id=postal-99)".into()]), Some("postal-99".into()));
    assert_eq!(
        extract_queue_id(&["OK id=1tAbCd-000123-45".into()]),
        Some("1tAbCd-000123-45".into())
    );
    assert_eq!(extract_queue_id(&["2.0.0 Valid message received".into()]), None);
    assert_eq!(extract_queue_id(&["2.0.0 Server said accepted".into()]), None);
}

#[test]
fn envelope_from_handles_folded_headers_comments_and_injection() {
    let mut req = sample_request();
    req.raw_mime =
        "From: \"Alice\"\r\n\t<alice@example.dev>\r\nTo: bob@example.dev\r\n\r\nHi".into();
    assert_eq!(envelope_from(&req), Some("alice@example.dev".into()));

    req.from_email_address = Some("bounce@example.dev".into());
    assert_eq!(envelope_from(&req), Some("bounce@example.dev".into()));

    req.from_email_address = Some("bad\r\nRCPT TO:<evil@example.dev>".into());
    assert_eq!(envelope_from(&req), None);
    assert!(!is_safe_envelope_address("a\x00b@example.dev"));
    assert!(!is_safe_envelope_address("a<b>@example.dev"));

    req.from_email_address = None;
    req.raw_mime =
        "From: Alice <alice@example.dev> (priority > 1)\r\nTo: bob@example.dev\r\n\r\nHi".into();
    assert_eq!(envelope_from(&req), Some("alice@example.dev".into()));
}

#[test]
fn message_id_and_prepare_data_normalize_crlf_stuff_dots_and_prepend_correlation() {
    assert_eq!(
        extract_message_id("Message-ID: <msg-123@example.dev>\r\n\r\nHi"),
        Some("msg-123@example.dev".into())
    );
    assert_eq!(extract_message_id("From: alice@example.dev\r\n\r\nHi"), None);
    let overlong_id = format!("Message-ID: <{}@example.dev>\r\n\r\nHi", "a".repeat(260));
    assert_eq!(extract_message_id(&overlong_id), None);

    let stuffed = prepare_data(".lead\rbar\n.dot", None);
    assert_eq!(stuffed, b"..lead\r\nbar\r\n..dot\r\n.\r\n");

    let corr = correlation();
    let with_corr =
        String::from_utf8(prepare_data("From: a@b.dev\n\nHi", Some(&corr))).expect("valid UTF-8");
    assert!(with_corr.starts_with(&format!(
        "X-Hailer-Message-Id: {}\r\nX-Hailer-Attempt-Id: {}\r\n",
        corr.hailer_message_id, corr.attempt_id
    )));
}
