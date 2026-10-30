use super::*;

#[test]
fn query_values_are_percent_encoded() {
    assert_eq!(
        encode_query(&[
            ("email", "user+tag@example.dev"),
            ("api_key", "a b&c=d"),
            ("timeout", "5")
        ]),
        "email=user%2Btag%40example.dev&api_key=a%20b%26c%3Dd&timeout=5"
    );
}
