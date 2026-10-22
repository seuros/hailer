use super::*;

#[tokio::test]
async fn disabled_intel_always_fails_open_as_unknown() {
    let facts = Intel::Disabled.lookup("example.com").await.expect("disabled lookup");
    assert_eq!(facts, DomainFacts::UNKNOWN);
}

#[test]
fn unknown_registration_date_is_never_new() {
    let now = "2026-01-01T00:00:00Z".parse().expect("timestamp");
    assert!(!is_new(DomainFacts::UNKNOWN, now, 30));
}

#[test]
fn a_domain_registered_within_the_threshold_is_new() {
    let registered_at = "2026-01-01T00:00:00Z".parse().expect("timestamp");
    let now = "2026-01-15T00:00:00Z".parse().expect("timestamp");
    let facts = DomainFacts { registered_at: Some(registered_at) };
    assert!(is_new(facts, now, 30));
}

#[test]
fn a_domain_registered_before_the_threshold_is_not_new() {
    let registered_at = "2020-01-01T00:00:00Z".parse().expect("timestamp");
    let now = "2026-01-01T00:00:00Z".parse().expect("timestamp");
    let facts = DomainFacts { registered_at: Some(registered_at) };
    assert!(!is_new(facts, now, 30));
}

#[test]
fn ascii_domains_are_not_flagged() {
    assert!(!has_non_ascii_label("example.com"));
    assert!(!has_non_ascii_label("xn--80akhbyknj4f.com")); // punycode IDN
}

#[test]
fn raw_non_ascii_domains_are_flagged() {
    assert!(has_non_ascii_label("пример.com"));
    assert!(has_non_ascii_label("例え.jp"));
}
