use super::*;

const SECOND: Duration = Duration::from_secs(1);

#[test]
fn within_caps_at_the_remaining_budget_and_refuses_below_the_floor() {
    let deadline = after(10 * SECOND);
    let wait = within(deadline, SECOND, 60 * SECOND).expect("fresh budget");
    assert!(wait <= 10 * SECOND && wait > 9 * SECOND);
    assert_eq!(within(deadline, SECOND, 2 * SECOND), Some(2 * SECOND));
    assert_eq!(within(deadline, 11 * SECOND, 60 * SECOND), None);
    assert_eq!(within(Deadline::never(), SECOND, 2 * SECOND), Some(2 * SECOND));
}

#[test]
fn a_spent_budget_allows_nothing_even_without_a_floor() {
    assert_eq!(within(after(Duration::ZERO), Duration::ZERO, SECOND), None);
}

#[test]
fn leaving_reserves_the_tail_and_never_bounds_an_unbounded_budget() {
    let deadline = after(10 * SECOND);
    let earlier = leaving(deadline, 4 * SECOND);
    let wait = within(earlier, Duration::ZERO, 60 * SECOND).expect("six seconds left");
    assert!(wait <= 6 * SECOND && wait > 5 * SECOND);
    assert_eq!(within(leaving(deadline, 11 * SECOND), Duration::ZERO, SECOND), None);
    assert_eq!(leaving(Deadline::never(), SECOND), Deadline::never());
}
