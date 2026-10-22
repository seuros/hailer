use std::assert_matches;

use super::*;
use crate::provider::ProviderError;

fn request(to: &str) -> SendRequest {
    SendRequest {
        raw_mime: "From: Alice <alice@example.org>\r\nTo: bob@example.org\r\n\r\nHello".into(),
        from_email_address: None,
        to_addresses: vec![to.to_owned()],
        cc_addresses: vec![],
        bcc_addresses: vec![],
    }
}

#[test]
fn a_real_looking_address_passes() {
    request("bob@realcompany.com").validate().expect("valid");
}

#[test]
fn toto_at_example_com_is_rejected() {
    let error = request("toto@example.com").validate().expect_err("should reject");
    assert_matches!(error, ProviderError::InvalidAddress(address) if address == "toto@example.com");
}

#[test]
fn every_rfc_2606_reserved_second_level_domain_is_rejected() {
    for domain in ["example.com", "example.net", "example.org"] {
        request(&format!("user@{domain}")).validate().expect_err("reserved domain");
    }
}

#[test]
fn every_rfc_2606_reserved_tld_is_rejected() {
    for domain in ["foo.test", "bar.example", "baz.invalid", "qux.localhost", "localhost"] {
        request(&format!("user@{domain}")).validate().expect_err("reserved tld");
    }
}

#[test]
fn a_subdomain_of_a_reserved_second_level_domain_is_rejected() {
    request("user@mail.example.com").validate().expect_err("reserved subdomain");
}

#[test]
fn a_domain_merely_containing_the_word_example_is_not_rejected() {
    // "exampleindustries.com" is a real-looking domain; only the exact
    // reserved names and TLDs are blocked, never a substring match.
    request("user@exampleindustries.com").validate().expect("not reserved");
}
