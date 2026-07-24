//! Recovery equivalence contracts. The sealed `Equivalence` trait and its
//! three modes (`ExactlyOnce`, `DuplicateLast`, `ReplayAll`) describe how far
//! an observed effect trace may diverge from a failure-free reference trace
//! after a crash and recovery. `TransactionalBoundary` gates which modes a
//! given `JournalStore` supports.

use crate::journal::EventPayload;
use crate::journal::JournalStore;
use crate::step::StepName;

/// Marker for a `JournalStore` whose journal append and side effects commit
/// atomically. `MemoryJournal` qualifies trivially (one process, one
/// memory); a `FileJournal` writing to disk while a step calls out over the
/// network does not.
pub trait TransactionalBoundary {}

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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, step: StepName, payload: EventPayload) {
        self.0.push(EffectRecord::new(step, payload));
    }

    pub fn records(&self) -> &[EffectRecord] {
        &self.0
    }
}

mod sealed {
    pub trait Sealed {}
}

/// The comparison a recovery mechanism promises to satisfy. Sealed: exactly
/// three implementations, below.
pub trait Equivalence: sealed::Sealed {
    fn equivalent(observed: &EffectTrace, reference: &EffectTrace) -> bool;
}

/// Whether store `S` supports equivalence mode `E`. `DuplicateLast` and
/// `ReplayAll` accept any `JournalStore`; `ExactlyOnce` additionally
/// requires `TransactionalBoundary`. Without atomic append-plus-effect, a
/// crash between a side effect landing and its journal record can duplicate
/// the effect, and no comparison function can undo that after the fact.
pub trait SupportedOn<S: JournalStore>: Equivalence {}

/// Every effect happens exactly once: identity over effect traces. Usable
/// only on a `TransactionalBoundary` store; see `SupportedOn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactlyOnce;

/// Recovery may duplicate at most the last effect of an interrupted step:
/// the crash window between a side effect landing and `StepCompleted` being
/// journaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicateLast;

/// Recovery may re-run the whole workflow from scratch, legal only when
/// every step's effect is idempotent. `observed`, with repeated effects
/// collapsed to their first occurrence, must equal `reference`.
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
        match (reference.records().last(), observed.records().split_last()) {
            (Some(expected_last), Some((observed_last, observed_prefix))) => {
                observed_prefix == reference.records() && observed_last == expected_last
            }
            _ => false,
        }
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

impl<S: JournalStore + TransactionalBoundary> SupportedOn<S> for ExactlyOnce {}
impl<S: JournalStore> SupportedOn<S> for DuplicateLast {}
impl<S: JournalStore> SupportedOn<S> for ReplayAll {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::journal::Journal;
    use crate::journal::JournalError;

    /// Never used as a real store. It only stands in for a generic `S` in
    /// `SupportedOn` bound checks below.
    struct StubStore;

    impl JournalStore for StubStore {
        fn append(
            &self,
            _id: &ExecutionId,
            _event: crate::journal::JournalEvent,
        ) -> Result<crate::journal::Seq, JournalError> {
            Err(JournalError::Poisoned)
        }

        fn load(&self, _id: &ExecutionId) -> Result<Journal, JournalError> {
            Err(JournalError::Poisoned)
        }
    }

    impl TransactionalBoundary for StubStore {}

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
    fn duplicate_last_fails_for_a_duplicate_in_the_middle_rather_than_the_tail() {
        // charge-renewal duplicated where it happened, not at the trailing
        // position. That is a different bug than the documented crash
        // window, and DuplicateLast makes no promise about it.
        let reference = reference_trace();
        let mut observed = EffectTrace::new();
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(charge_renewal(), charge_renewal_confirmation());
        observed.record(send_receipt(), send_receipt_confirmation());

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

    #[test]
    fn memory_journal_supports_exactly_once() {
        fn assert_supported<S: JournalStore, E: SupportedOn<S>>() {}
        assert_supported::<crate::stores::memory::MemoryJournal, ExactlyOnce>();
    }

    #[test]
    fn any_journal_store_supports_duplicate_last_and_replay_all() {
        fn assert_supported<S: JournalStore, E: SupportedOn<S>>() {}
        assert_supported::<StubStore, DuplicateLast>();
        assert_supported::<StubStore, ReplayAll>();
    }
}
