use super::*;

#[test]
fn pairs_are_percent_encoded() {
    assert_eq!(
        encode_pairs([("email", "user+tag@example.dev"), ("api_key", "a b&c=d"), ("timeout", "5")]),
        "email=user%2Btag%40example.dev&api_key=a%20b%26c%3Dd&timeout=5"
    );
    assert_eq!(percent_encode("A-z_0.9~/"), "A-z_0.9~%2F");
}
