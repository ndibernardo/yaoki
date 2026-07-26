//! Crash injection. Each window where the engine can die between a side
//! effect and its journal record is named here. A `FailpointPolicy` decides,
//! per window, whether the engine continues or simulates process death.
//! Production uses `NeverCrash`. Tests use `CrashOnce`, then recover over the
//! same store.

use std::cell::Cell;

use crate::journal::Seq;

/// A window where the engine can die, at the command identified by `Seq`.
/// Exhaustive by construction: the live paths of `WorkflowCtx::step` and
/// `WorkflowCtx::sleep_until` consult the policy at each of these and nowhere
/// else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    /// Before anything is journaled for this step.
    BeforeStepScheduled(Seq),
    /// `StepScheduled` durable, `StepStarted` not yet.
    AfterStepScheduled(Seq),
    /// `StepStarted` durable, the closure has not run.
    AfterStepStarted(Seq),
    /// The closure returned, so its side effect landed, and `StepCompleted`
    /// is not yet durable. Costs a duplicate effect on recovery.
    AfterSideEffect(Seq),
    /// `StepCompleted` durable. Recovery replays the step from the journal.
    AfterStepCompleted(Seq),
    /// `TimerScheduled` durable, the wait has not started.
    AfterTimerScheduled(Seq),
    /// `TimerFired` durable, the workflow has not resumed.
    AfterTimerFired(Seq),
}

impl CrashPoint {
    /// The command position this window belongs to.
    pub fn seq(self) -> Seq {
        match self {
            CrashPoint::BeforeStepScheduled(seq)
            | CrashPoint::AfterStepScheduled(seq)
            | CrashPoint::AfterStepStarted(seq)
            | CrashPoint::AfterSideEffect(seq)
            | CrashPoint::AfterStepCompleted(seq)
            | CrashPoint::AfterTimerScheduled(seq)
            | CrashPoint::AfterTimerFired(seq) => seq,
        }
    }
}

/// What the engine does at a crash window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailpointDecision {
    Continue,
    Crash,
}

/// Consulted by the engine at every `CrashPoint`.
pub trait FailpointPolicy {
    fn at(&self, point: CrashPoint) -> FailpointDecision;
}

/// Production policy: the engine never simulates its own death.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NeverCrash;

impl FailpointPolicy for NeverCrash {
    fn at(&self, _point: CrashPoint) -> FailpointDecision {
        FailpointDecision::Continue
    }
}

/// Test policy: crashes the first time the engine reaches `at`, then lets
/// every later window through. One value can therefore drive both the
/// crashing run and a reused recovery run.
///
/// `Cell` is required: `FailpointPolicy::at` takes `&self` because the engine
/// holds the policy by shared reference, so the one-shot flag cannot be a
/// `&mut` field.
#[derive(Debug)]
pub struct CrashOnce {
    at: CrashPoint,
    fired: Cell<bool>,
}

impl CrashOnce {
    pub fn new(at: CrashPoint) -> Self {
        Self {
            at,
            fired: Cell::new(false),
        }
    }

    /// True once the policy has crashed the engine.
    pub fn has_fired(&self) -> bool {
        self.fired.get()
    }
}

impl FailpointPolicy for CrashOnce {
    fn at(&self, point: CrashPoint) -> FailpointDecision {
        if point == self.at && !self.fired.get() {
            self.fired.set(true);
            return FailpointDecision::Crash;
        }
        FailpointDecision::Continue
    }
}

/// Whether an injected crash fired during a run. The engine consults this
/// after `Workflow::run` returns: a crash stands for process death, so no
/// terminal event may be journaled regardless of the workflow's own result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashStatus {
    Intact,
    Crashed(CrashPoint),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn charge_card_seq() -> Seq {
        Seq::zero()
    }

    fn create_account_seq() -> Seq {
        Seq::zero().next()
    }

    #[test]
    fn crash_point_seq_returns_the_position_of_every_window() {
        let seq = create_account_seq();

        assert_eq!(CrashPoint::BeforeStepScheduled(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterStepScheduled(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterStepStarted(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterSideEffect(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterStepCompleted(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterTimerScheduled(seq).seq(), seq);
        assert_eq!(CrashPoint::AfterTimerFired(seq).seq(), seq);
    }

    #[test]
    fn never_crash_continues_at_every_window() {
        let policy = NeverCrash;

        assert_eq!(
            policy.at(CrashPoint::AfterSideEffect(charge_card_seq())),
            FailpointDecision::Continue
        );
        assert_eq!(
            policy.at(CrashPoint::AfterStepCompleted(charge_card_seq())),
            FailpointDecision::Continue
        );
    }

    #[test]
    fn crash_once_crashes_at_its_configured_window() {
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(charge_card_seq()));

        let decision = policy.at(CrashPoint::AfterSideEffect(charge_card_seq()));

        assert_eq!(decision, FailpointDecision::Crash);
        assert!(policy.has_fired());
    }

    #[test]
    fn crash_once_continues_at_a_different_window() {
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(charge_card_seq()));

        let decision = policy.at(CrashPoint::AfterStepCompleted(charge_card_seq()));

        assert_eq!(decision, FailpointDecision::Continue);
        assert!(!policy.has_fired());
    }

    #[test]
    fn crash_once_continues_at_the_same_window_of_a_different_command() {
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(charge_card_seq()));

        let decision = policy.at(CrashPoint::AfterSideEffect(create_account_seq()));

        assert_eq!(decision, FailpointDecision::Continue);
    }

    #[test]
    fn crash_once_continues_on_the_second_visit_to_its_window() {
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(charge_card_seq()));
        let _first = policy.at(CrashPoint::AfterSideEffect(charge_card_seq()));

        let second = policy.at(CrashPoint::AfterSideEffect(charge_card_seq()));

        assert_eq!(second, FailpointDecision::Continue);
    }
}
