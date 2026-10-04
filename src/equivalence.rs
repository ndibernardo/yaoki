//! Experimental comparisons of recorded effect traces with a reference.
//! These predicates do not select engine behavior, enforce transactions,
//! or prove recipient-side guarantees. Records identify effects by name
//! and payload, so separate logical operations can be indistinguishable.

use crate::journal::EventPayload;
use crate::step::StepName;

/// One step's recorded external effect: its name and the payload it
/// produced. This is what `Equivalence::equivalent` compares. It is
/// distinct from `JournalEvent`, which is the durability record, not the
/// effect itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectRecord {
    step: StepName,
    payload: EventPayload,
}

impl EffectRecord {
    /// Records an observed effect's step name and result payload.
    pub fn new(step: StepName, payload: EventPayload) -> Self {
        Self { step, payload }
    }
}

/// Ordered log of effects a workflow run produced. Step bodies append to a
/// shared trace as they run; comparing a reference trace (failure-free run)
/// against an observed one (after a crash and recovery) is what
/// `Equivalence::equivalent` does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EffectTrace(Vec<EffectRecord>);

impl EffectTrace {
    /// Creates an empty observation trace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an observation without deduplicating equal records.
    pub fn record(&mut self, step: StepName, payload: EventPayload) {
        self.0.push(EffectRecord::new(step, payload));
    }

    /// Borrows all observations in their recorded order.
    pub fn records(&self) -> &[EffectRecord] {
        &self.0
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed experimental trace predicates, not runtime recovery contracts.
/// The comparisons are directional and need not be equivalence relations.
pub trait Equivalence: sealed::Sealed {
    /// Applies this predicate's observation rules to the supplied traces.
    fn equivalent(observed: &EffectTrace, reference: &EffectTrace) -> bool;
}

/// Exact equality of ordered records, including repeated equal values.
/// Only supplied observations are compared; unrecorded effects and external
/// atomicity are outside this predicate's domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactlyOnce;

/// Equality or exactly one extra adjacent copy of a reference record.
/// With one effect per step and one interruption after an effect but before
/// its completion record, the interrupted effect can repeat at any position.
/// Multiple interruptions and multi-effect bodies can exceed this allowance.
/// The predicate neither prevents duplicates nor checks recipient idempotence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicateLast;

/// Equality after retaining only the first occurrence of each observed record.
/// Reference records must be pairwise distinct for identical traces to pass.
/// Separate equal logical operations are indistinguishable in this model.
/// First appearances must follow reference order; duplicates can occur anywhere.
/// This neither checks recipient idempotence nor triggers a workflow restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayAll;

impl sealed::Sealed for ExactlyOnce {}
impl sealed::Sealed for DuplicateLast {}
impl sealed::Sealed for ReplayAll {}

impl Equivalence for ExactlyOnce {
    fn equivalent(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        observed == reference
    }
}

impl Equivalence for DuplicateLast {
    fn equivalent(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        if observed == reference {
            return true;
        }
        if observed.records().len() != reference.records().len() + 1 {
            return false;
        }
        // One crash, one interrupted step: observed must be reference with
        // exactly one of its effects repeated immediately after itself.
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

impl Equivalence for ReplayAll {
    fn equivalent(observed: &EffectTrace, reference: &EffectTrace) -> bool {
        let mut deduplicated: Vec<&EffectRecord> = Vec::new();
        for record in observed.records() {
            if !deduplicated.contains(&record) {
                deduplicated.push(record);
            }
        }
        deduplicated.into_iter().eq(reference.records().iter())
    }
}

#[cfg(test)]
mod tests {
    use super::DuplicateLast;
    use super::EffectRecord;
    use super::EffectTrace;
    use super::Equivalence;
    use super::ExactlyOnce;
    use super::ReplayAll;
    use crate::journal::EventPayload;
    use crate::step::StepName;

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

    fn reference_trace() -> EffectTrace {
        let mut trace = EffectTrace::new();
        trace.record(charge_renewal(), charge_renewal_confirmation());
        trace.record(send_receipt(), send_receipt_confirmation());
        trace
    }

    #[test]
    fn effect_trace_new_is_empty() {
        let trace = EffectTrace::new();

        assert_eq!(trace.records(), &[]);
    }

    #[test]
    fn effect_trace_record_appends_in_order() {
        let mut trace = EffectTrace::new();

        trace.record(charge_renewal(), charge_renewal_confirmation());
        trace.record(send_receipt(), send_receipt_confirmation());

        assert_eq!(
            trace.records(),
            &[
                EffectRecord::new(charge_renewal(), charge_renewal_confirmation()),
                EffectRecord::new(send_receipt(), send_receipt_confirmation()),
            ]
        );
    }

    #[test]
    fn exactly_once_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(ExactlyOnce::equivalent(&observed, &reference));
    }

    #[test]
    fn exactly_once_fails_for_a_duplicated_trailing_effect() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(!ExactlyOnce::equivalent(&observed, &reference));
    }

    #[test]
    fn exactly_once_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(ExactlyOnce::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_holds_for_a_duplicated_trailing_effect() {
        let reference = reference_trace();
        let mut observed = reference_trace();
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_holds_for_an_interrupted_middle_step_duplicated_in_place() {
        // Crashed after charge-renewal's effect landed but before its
        // StepCompleted was durable: recovery reruns charge-renewal, then
        // carries on to send-receipt.
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_fails_for_a_repeat_that_is_not_adjacent_to_its_original() {
        // charge-renewal repeating after send-receipt is not a crash window.
        // No single interrupted step produces this ordering.
        let reference = reference_trace();
        let mut observed = reference_trace();
        observed.record(charge_renewal(), charge_renewal_confirmation());

        assert!(!DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_fails_for_two_duplicated_effects() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(!DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_fails_for_an_extra_effect_against_an_empty_reference() {
        let reference = EffectTrace::new();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());

        assert!(!DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_fails_when_observed_is_a_strict_prefix_of_reference() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());

        assert!(!DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn duplicate_last_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(DuplicateLast::equivalent(&observed, &reference));
    }

    #[test]
    fn replay_all_holds_for_identical_traces() {
        let reference = reference_trace();
        let observed = reference_trace();

        assert!(ReplayAll::equivalent(&observed, &reference));
    }

    #[test]
    fn replay_all_holds_for_a_full_rerun_after_a_partial_reference_prefix() {
        // Crashed after charge-renewal alone, then a ReplayAll recovery
        // re-executed the whole workflow from scratch.
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(ReplayAll::equivalent(&observed, &reference));
    }

    #[test]
    fn replay_all_holds_for_a_triple_execution_of_the_whole_workflow() {
        // Two full reruns on top of the reference run, e.g. two separate
        // crashes each triggering a fresh full re-execution.
        let reference = reference_trace();
        let mut observed = reference_trace();
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());

        assert!(ReplayAll::equivalent(&observed, &reference));
    }

    #[test]
    fn replay_all_fails_when_a_step_is_missing_from_the_reference() {
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());

        assert!(!ReplayAll::equivalent(&observed, &reference));
    }

    #[test]
    fn replay_all_holds_for_empty_traces() {
        let reference = EffectTrace::new();
        let observed = EffectTrace::new();

        assert!(ReplayAll::equivalent(&observed, &reference));
    }
}
