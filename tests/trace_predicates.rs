//! Identity-aware observation checks, independent of engine behavior.

use proptest::collection::vec;
use proptest::proptest;
use yaoki::equivalence::EffectRecord;
use yaoki::equivalence::EffectTrace;
use yaoki::equivalence::ExactTrace;
use yaoki::equivalence::OrderedRetries;
use yaoki::equivalence::SingleAdjacentDuplicate;
use yaoki::equivalence::TracePredicate;
use yaoki::execution::ExecutionId;
use yaoki::journal::EventPayload;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::IdempotencyKey;
use yaoki::step::StepName;

struct FixedRng {
    bytes: [u8; 32],
}

impl RngSource for FixedRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new(self.bytes)
    }
}

fn renewal_key(seq: Seq) -> IdempotencyKey {
    let execution = ExecutionId::generate(&mut FixedRng { bytes: [0x52; 32] });
    IdempotencyKey::new(execution, seq)
}

fn charge_name() -> StepName {
    StepName::new("charge-renewal").unwrap()
}

fn charge_result() -> EventPayload {
    EventPayload::new(br#"{"status":"charged"}"#.to_vec())
}

fn charge_trace(keys: &[IdempotencyKey]) -> EffectTrace {
    let mut trace = EffectTrace::new();
    for key in keys {
        trace.record(*key, charge_name(), charge_result());
    }
    trace
}

fn renewal_reference() -> EffectTrace {
    // Two subscription renewals can have the same step name and result.
    charge_trace(&[
        renewal_key(Seq::zero()),
        renewal_key(Seq::zero().next().unwrap()),
    ])
}

#[test]
fn effect_records_with_equal_names_and_results_but_distinct_keys_remain_distinct() {
    let reference = renewal_reference();

    assert_ne!(reference.records()[0], reference.records()[1]);
    assert!(ExactTrace::matches(&reference, &reference));
    assert!(SingleAdjacentDuplicate::matches(&reference, &reference));
    assert!(OrderedRetries::matches(&reference, &reference));
}

#[test]
fn effect_record_constructor_retains_the_operation_key() {
    let key = renewal_key(Seq::zero());
    let record = EffectRecord::new(key, charge_name(), charge_result());

    assert_eq!(record.operation(), key);
}

#[test]
fn ordered_retries_with_a_missing_equal_valued_operation_rejects_the_trace() {
    let reference = renewal_reference();
    let observed = charge_trace(&[renewal_key(Seq::zero()); 2]);

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_reordered_equal_valued_operations_rejects_the_trace() {
    let reference = renewal_reference();
    let observed = charge_trace(&[
        renewal_key(Seq::zero().next().unwrap()),
        renewal_key(Seq::zero()),
    ]);

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_nonadjacent_identical_retries_accepts_the_trace() {
    let reference = renewal_reference();
    let first = renewal_key(Seq::zero());
    let second = renewal_key(Seq::zero().next().unwrap());
    let observed = charge_trace(&[first, first, second, first, second]);

    assert!(OrderedRetries::matches(&observed, &reference));
    assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    assert!(!ExactTrace::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_a_changed_retry_result_rejects_the_trace() {
    let reference = renewal_reference();
    let mut observed = reference.clone();
    observed.record(
        renewal_key(Seq::zero()),
        charge_name(),
        EventPayload::new(br#"{"status":"declined"}"#.to_vec()),
    );

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_a_changed_retry_name_rejects_the_trace() {
    let reference = renewal_reference();
    let mut observed = reference.clone();
    observed.record(
        renewal_key(Seq::zero()),
        StepName::new("refund-renewal").unwrap(),
        charge_result(),
    );

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_a_changed_first_result_rejects_the_trace() {
    let reference = charge_trace(&[renewal_key(Seq::zero())]);
    let mut observed = EffectTrace::new();
    observed.record(
        renewal_key(Seq::zero()),
        charge_name(),
        EventPayload::new(br#"{"status":"declined"}"#.to_vec()),
    );

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_a_changed_first_name_rejects_the_trace() {
    let reference = charge_trace(&[renewal_key(Seq::zero())]);
    let mut observed = EffectTrace::new();
    observed.record(
        renewal_key(Seq::zero()),
        StepName::new("refund-renewal").unwrap(),
        charge_result(),
    );

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_an_unknown_sequence_rejects_the_trace() {
    let reference = renewal_reference();
    let mut observed = reference.clone();
    observed.record(
        renewal_key(Seq::zero().next().unwrap().next().unwrap()),
        charge_name(),
        charge_result(),
    );

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn trace_predicates_with_a_key_from_another_execution_reject_the_trace() {
    let reference = charge_trace(&[renewal_key(Seq::zero())]);
    let other_execution = ExecutionId::generate(&mut FixedRng { bytes: [0x53; 32] });
    let observed = charge_trace(&[IdempotencyKey::new(other_execution, Seq::zero())]);

    assert!(!ExactTrace::matches(&observed, &reference));
    assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_a_duplicate_reference_key_rejects_even_identical_traces() {
    let reference = charge_trace(&[renewal_key(Seq::zero()); 2]);

    assert!(ExactTrace::matches(&reference, &reference));
    assert!(SingleAdjacentDuplicate::matches(&reference, &reference));
    assert!(!OrderedRetries::matches(&reference, &reference));
}

#[test]
fn ordered_retries_with_an_inconsistent_duplicate_reference_key_rejects_the_trace() {
    let mut reference = charge_trace(&[renewal_key(Seq::zero())]);
    reference.record(
        renewal_key(Seq::zero()),
        charge_name(),
        EventPayload::new(br#"{"status":"declined"}"#.to_vec()),
    );

    assert!(!OrderedRetries::matches(&reference, &reference));
}

#[test]
fn ordered_retries_with_an_extra_observation_against_an_empty_reference_rejects_it() {
    let reference = EffectTrace::new();
    let observed = charge_trace(&[renewal_key(Seq::zero())]);

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn ordered_retries_with_no_observations_against_a_nonempty_reference_rejects_it() {
    let reference = renewal_reference();
    let observed = EffectTrace::new();

    assert!(!OrderedRetries::matches(&observed, &reference));
}

#[test]
fn single_adjacent_duplicate_with_equal_values_but_a_new_key_rejects_the_trace() {
    let reference = charge_trace(&[renewal_key(Seq::zero())]);
    let observed = renewal_reference();

    assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
}

#[test]
fn single_adjacent_duplicate_with_a_changed_adjacent_result_rejects_the_trace() {
    let reference = charge_trace(&[renewal_key(Seq::zero())]);
    let mut observed = reference.clone();
    observed.record(
        renewal_key(Seq::zero()),
        charge_name(),
        EventPayload::new(br#"{"status":"declined"}"#.to_vec()),
    );

    assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
}

#[test]
fn single_adjacent_duplicate_with_a_duplicate_after_a_repeated_reference_record_accepts_it() {
    let reference = charge_trace(&[renewal_key(Seq::zero()); 2]);
    let observed = charge_trace(&[renewal_key(Seq::zero()); 3]);

    assert!(SingleAdjacentDuplicate::matches(&observed, &reference));
}

proptest! {
    #[test]
    fn single_adjacent_duplicate_matches_an_independent_removal_model(
        reference_operations in vec(0usize..4, 0..8),
        observed_operations in vec(0usize..4, 0..10),
    ) {
        let keys: Vec<_> = (0..4)
            .scan(Seq::zero(), |seq, _| {
                let key = renewal_key(*seq);
                *seq = seq.next().unwrap();
                Some(key)
            })
            .collect();
        let reference_keys: Vec<_> = reference_operations.iter().map(|index| keys[*index]).collect();
        let observed_keys: Vec<_> = observed_operations.iter().map(|index| keys[*index]).collect();
        let reference = charge_trace(&reference_keys);
        let observed = charge_trace(&observed_keys);
        let removable_duplicate = observed_operations.windows(2).enumerate().any(|(index, pair)| {
            let mut candidate = observed_operations.clone();
            candidate.remove(index);
            pair[0] == pair[1] && candidate == reference_operations
        });
        let expected = observed_operations == reference_operations || removable_duplicate;

        proptest::prop_assert_eq!(SingleAdjacentDuplicate::matches(&observed, &reference), expected);
    }

    #[test]
    fn ordered_retries_matches_an_independent_first_appearance_model(
        operation_count in 0usize..9,
        attempts in vec(0usize..10, 0..40),
    ) {
        let keys: Vec<_> = (0..10)
            .scan(Seq::zero(), |seq, _| {
                let key = renewal_key(*seq);
                *seq = seq.next().unwrap();
                Some(key)
            })
            .collect();
        let reference = charge_trace(&keys[..operation_count]);
        let observed_keys: Vec<_> = attempts.iter().map(|index| keys[*index]).collect();
        let observed = charge_trace(&observed_keys);
        let mut first_appearances = attempts.clone();
        // This deliberately uses a different, quadratic model for bounded tests.
        let mut seen = Vec::new();
        first_appearances.retain(|index| {
            if seen.contains(index) {
                false
            } else {
                seen.push(*index);
                true
            }
        });
        let expected = first_appearances == (0..operation_count).collect::<Vec<_>>();

        proptest::prop_assert_eq!(OrderedRetries::matches(&observed, &reference), expected);
    }
}
