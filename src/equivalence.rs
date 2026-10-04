//! Experimental comparisons of recorded observations with a reference.
//! Operation keys distinguish logical steps from repeated attempts. These
//! predicates neither control recovery nor prove recipient-side guarantees.

use std::collections::HashMap;

use crate::journal::EventPayload;
use crate::step::IdempotencyKey;
use crate::step::StepName;

/// One observed step effect, identified by its stable operation key.
/// A trace models one effect per step attempt, not every possible body effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectRecord {
    operation: IdempotencyKey,
    step: StepName,
    payload: EventPayload,
}

impl EffectRecord {
    /// Records an operation key, step name, and observed result payload.
    pub fn new(operation: IdempotencyKey, step: StepName, payload: EventPayload) -> Self {
        Self {
            operation,
            step,
            payload,
        }
    }

    /// Returns the key shared by attempts of this logical operation.
    pub fn operation(&self) -> IdempotencyKey {
        self.operation
    }
}

/// Ordered observations, including repeated attempts of the same operation.
/// Keys are supplied by the observer; they do not authenticate an effect or
/// establish that a recipient applied it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EffectTrace(Vec<EffectRecord>);

impl EffectTrace {
    /// Creates an empty observation trace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an observation without deduplication.
    /// Retries use the same key; distinct logical steps use distinct keys.
    ///
    /// Recording only a name and payload cannot distinguish logical operations.
    ///
    /// ```compile_fail
    /// use yaoki::equivalence::EffectTrace;
    /// use yaoki::journal::EventPayload;
    /// use yaoki::step::StepName;
    ///
    /// let mut trace = EffectTrace::new();
    /// trace.record(
    ///     StepName::new("charge-renewal").unwrap(),
    ///     EventPayload::new(b"charged".to_vec()),
    /// );
    /// ```
    pub fn record(&mut self, operation: IdempotencyKey, step: StepName, payload: EventPayload) {
        self.0.push(EffectRecord::new(operation, step, payload));
    }

    /// Borrows all observations in their recorded order.
    pub fn records(&self) -> &[EffectRecord] {
        &self.0
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed observation predicates, not runtime recovery contracts.
/// The comparisons are directional and need not be equivalence relations.
pub trait TracePredicate: sealed::Sealed {
    /// Checks the observed trace against this predicate's reference contract.
    fn matches(observed: &EffectTrace, reference: &EffectTrace) -> bool;
}

/// Exact ordered record equality, including operation keys and repeated records.
/// Unrecorded effects and external atomicity are outside this predicate's domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactTrace;

/// Equality or one extra adjacent identical record at any reference position.
/// One interruption after one effect but before its completion record can produce
/// this shape. Multiple interruptions or multi-effect bodies can exceed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleAdjacentDuplicate;

/// Identical retries with first appearances in reference order.
/// Each reference key must be unique. All reference operations must appear;
/// unknown keys and changed names or payloads are rejected. Identical retries
/// may occur anywhere after their first appearance. This checks neither
/// recipient idempotence nor whether a workflow was restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedRetries;

impl sealed::Sealed for ExactTrace {}
impl sealed::Sealed for SingleAdjacentDuplicate {}
impl sealed::Sealed for OrderedRetries {}

impl TracePredicate for ExactTrace {
    fn matches(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        observed == reference
    }
}

impl TracePredicate for SingleAdjacentDuplicate {
    fn matches(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        if observed == reference {
            return true;
        }
        if observed
            .records()
            .len()
            .checked_sub(reference.records().len())
            != Some(1)
        {
            return false;
        }
        reference
            .records()
            .iter()
            .enumerate()
            .any(|(index, record)| {
                let mut candidate: Vec<&EffectRecord> = reference.records().iter().collect();
                candidate.insert(index, record);
                candidate.into_iter().eq(observed.records().iter())
            })
    }
}

impl TracePredicate for OrderedRetries {
    fn matches(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        let mut operations = HashMap::new();
        for (index, record) in reference.records().iter().enumerate() {
            if operations
                .insert(record.operation, (index, record))
                .is_some()
            {
                return false;
            }
        }
        let mut next = 0;
        for record in observed.records() {
            let Some(&(index, expected)) = operations.get(&record.operation) else {
                return false;
            };
            if record != expected || index > next {
                return false;
            }
            if index == next {
                next += 1;
            }
        }
        next == reference.records().len()
    }
}

#[cfg(test)]
mod tests {
    use super::EffectRecord;
    use super::EffectTrace;
    use super::ExactTrace;
    use super::OrderedRetries;
    use super::SingleAdjacentDuplicate;
    use super::TracePredicate;
    use crate::execution::ExecutionId;
    use crate::journal::EventPayload;
    use crate::journal::Seq;
    use crate::random::RandomBytes;
    use crate::random::RngSource;
    use crate::step::IdempotencyKey;
    use crate::step::StepName;

    struct RenewalRng;

    impl RngSource for RenewalRng {
        fn next_bytes(&mut self) -> RandomBytes {
            RandomBytes::new([0x52; 32])
        }
    }

    fn renewal_key(seq: Seq) -> IdempotencyKey {
        IdempotencyKey::new(ExecutionId::generate(&mut RenewalRng), seq)
    }

    fn charge_renewal() -> StepName {
        StepName::new("charge-renewal").unwrap()
    }

    fn charge_renewal_confirmation() -> EventPayload {
        EventPayload::new(br#"{"charge_id":"ch_2026_0724"}"#.to_vec())
    }

    fn send_receipt() -> StepName {
        StepName::new("send-receipt").unwrap()
    }

    fn send_receipt_confirmation() -> EventPayload {
        EventPayload::new(br#"{"receipt_sent":true}"#.to_vec())
    }

    fn record_charge(trace: &mut EffectTrace) {
        trace.record(
            renewal_key(Seq::zero()),
            charge_renewal(),
            charge_renewal_confirmation(),
        );
    }

    fn record_receipt(trace: &mut EffectTrace) {
        trace.record(
            renewal_key(Seq::zero().next().unwrap()),
            send_receipt(),
            send_receipt_confirmation(),
        );
    }

    fn reference_trace() -> EffectTrace {
        let mut trace = EffectTrace::new();
        record_charge(&mut trace);
        record_receipt(&mut trace);
        trace
    }

    #[test]
    fn effect_trace_new_is_empty() {
        let trace = EffectTrace::new();

        assert_eq!(trace.records(), &[]);
    }

    #[test]
    fn effect_trace_record_appends_in_order() {
        let trace = reference_trace();

        assert_eq!(
            trace.records(),
            &[
                EffectRecord::new(
                    renewal_key(Seq::zero()),
                    charge_renewal(),
                    charge_renewal_confirmation()
                ),
                EffectRecord::new(
                    renewal_key(Seq::zero().next().unwrap()),
                    send_receipt(),
                    send_receipt_confirmation()
                ),
            ]
        );
    }

    #[test]
    fn exact_trace_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(ExactTrace::matches(&observed, &reference));
    }

    #[test]
    fn exact_trace_fails_for_a_duplicated_trailing_effect() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        record_receipt(&mut observed);

        assert!(!ExactTrace::matches(&observed, &reference));
    }

    #[test]
    fn exact_trace_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(ExactTrace::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_holds_for_a_duplicated_trailing_effect() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        record_receipt(&mut observed);

        assert!(SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_holds_for_an_interrupted_middle_step_duplicated_in_place() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);
        record_charge(&mut observed);
        record_receipt(&mut observed);

        assert!(SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_fails_for_a_nonadjacent_repeat() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        record_charge(&mut observed);

        assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_fails_for_two_duplicated_effects() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);
        record_charge(&mut observed);
        record_receipt(&mut observed);
        record_receipt(&mut observed);

        assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_fails_for_an_extra_effect_against_an_empty_reference() {
        let reference = EffectTrace::new();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);

        assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_fails_when_observed_is_a_strict_prefix_of_reference() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);

        assert!(!SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn single_adjacent_duplicate_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(SingleAdjacentDuplicate::matches(&observed, &reference));
    }

    #[test]
    fn ordered_retries_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(OrderedRetries::matches(&observed, &reference));
    }

    #[test]
    fn ordered_retries_holds_for_a_full_rerun_after_a_partial_reference_prefix() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);
        record_charge(&mut observed);
        record_receipt(&mut observed);

        assert!(OrderedRetries::matches(&observed, &reference));
    }

    #[test]
    fn ordered_retries_holds_for_a_triple_execution_of_the_whole_workflow() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        record_charge(&mut observed);
        record_receipt(&mut observed);
        record_charge(&mut observed);
        record_receipt(&mut observed);

        assert!(OrderedRetries::matches(&observed, &reference));
    }

    #[test]
    fn ordered_retries_fails_when_a_reference_operation_is_missing() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        record_charge(&mut observed);

        assert!(!OrderedRetries::matches(&observed, &reference));
    }

    #[test]
    fn ordered_retries_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(OrderedRetries::matches(&observed, &reference));
    }
}
