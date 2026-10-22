use super::*;

#[test]
fn domain_of_splits_on_the_last_at() {
    assert_eq!(domain_of("alice@example.com"), Some("example.com"));
    assert_eq!(domain_of("\"a@b\"@example.com"), Some("example.com"));
    assert_eq!(domain_of("no-at-sign"), None);
}

#[test]
fn exact_pattern_matches_only_that_domain() {
    assert!(matches("guerrillamail.com", "guerrillamail.com"));
    assert!(matches("GuerrillaMail.com", "guerrillamail.com"));
    assert!(!matches("guerrillamail.com", "notguerrillamail.com"));
    assert!(!matches("guerrillamail.com", "sub.guerrillamail.com"));
}

#[test]
fn suffix_glob_matches_the_domain_and_its_subdomains() {
    assert!(matches("*.il", "example.il"));
    assert!(matches("*.il", "mail.example.il"));
    assert!(matches("*.IL", "example.il"));
    assert!(!matches("*.il", "example.ilx"));
    assert!(!matches("*.il", "notil"));
}

#[test]
fn rfc_2606_reserved_domains_and_tlds_are_flagged() {
    for domain in [
        "example.com",
        "example.net",
        "example.org",
        "foo.test",
        "bar.example",
        "baz.invalid",
        "qux.localhost",
        "localhost",
        "sub.example.com",
    ] {
        assert!(is_reserved_example_domain(domain), "{domain} should be reserved");
    }
}

#[test]
fn a_domain_only_resembling_a_reserved_one_is_not_flagged() {
    for domain in ["exampleindustries.com", "notexample.com", "example.co"] {
        assert!(!is_reserved_example_domain(domain), "{domain} should not be reserved");
    }
}

#[test]
fn blocking_reason_reports_the_first_match_or_none() {
    let patterns = vec![
        ("*.kp".to_string(), Some("sanctioned TLD".to_string())),
        ("guerrillamail.com".to_string(), None),
    ];
    assert_eq!(blocking_reason(&patterns, "example.kp"), Some("sanctioned TLD"));
    assert_eq!(blocking_reason(&patterns, "guerrillamail.com"), Some("blocked"));
    assert_eq!(blocking_reason(&patterns, "legit.com"), None);
}
