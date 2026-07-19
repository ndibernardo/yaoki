//! Wall-clock types. Durable timers persist deadlines, never durations:
//! restart must not reset the clock.

use std::sync::Mutex;
use std::sync::PoisonError;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Wall-clock instant, milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn from_millis_since_epoch(millis: u64) -> Self {
        Self(millis)
    }

    pub fn as_millis_since_epoch(&self) -> u64 {
        self.0
    }
}

/// Wall-clock deadline. Never a duration: recovery re-arms the remainder
/// against this absolute instant, so restart does not reset the clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(Timestamp);

impl Deadline {
    pub fn at(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn timestamp(&self) -> Timestamp {
        self.0
    }
}

/// Source of wall-clock time for a workflow run. `ctx.now()` is the only
/// door: `SystemTime::now` is banned elsewhere in this crate
/// (`clippy.toml` `disallowed-methods`), so `SystemClock` below is the sole
/// sanctioned caller.
pub trait Clock {
    fn now(&self) -> Timestamp;

    /// Blocks until `deadline`. A deadline already in the past returns
    /// immediately. Recovery re-arming an already-expired timer must fire
    /// it, not wait again. `TestClock` never blocks at all.
    fn sleep_until(&self, deadline: Timestamp);
}

/// Real wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[allow(
        clippy::disallowed_methods,
        reason = "the one sanctioned SystemTime::now call site; ctx.now() is the workflow-facing door"
    )]
    fn now(&self) -> Timestamp {
        // A clock set before the Unix epoch clamps to it rather than
        // panicking: unreachable in practice, not worth threading a
        // fallible `Clock::now` through every `ctx.now()` call.
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Timestamp::from_millis_since_epoch(millis as u64)
    }

    fn sleep_until(&self, deadline: Timestamp) {
        let now = self.now();
        if deadline > now {
            let remaining = deadline.as_millis_since_epoch() - now.as_millis_since_epoch();
            thread::sleep(Duration::from_millis(remaining));
        }
    }
}

/// Deterministic clock for tests: holds a fixed instant, advanced only by
/// explicit calls, never by reading the OS clock. "Charge in a week"
/// compresses to milliseconds by advancing this instead of sleeping.
#[derive(Debug)]
pub struct TestClock {
    now: Mutex<Timestamp>,
}

impl TestClock {
    pub fn at(timestamp: Timestamp) -> Self {
        Self {
            now: Mutex::new(timestamp),
        }
    }

    pub fn advance_millis(&self, millis: u64) {
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = Timestamp::from_millis_since_epoch(now.as_millis_since_epoch() + millis);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Never blocks. Advance the clock explicitly with `advance_millis`
    /// instead of waiting on wall time.
    fn sleep_until(&self, _deadline: Timestamp) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_millis_since_epoch() {
        let timestamp = Timestamp::from_millis_since_epoch(1_752_838_800_000);

        assert_eq!(timestamp.as_millis_since_epoch(), 1_752_838_800_000);
    }

    #[test]
    fn deadline_at_preserves_the_absolute_timestamp() {
        let one_week_from_epoch = Timestamp::from_millis_since_epoch(604_800_000);

        let deadline = Deadline::at(one_week_from_epoch);

        assert_eq!(deadline.timestamp(), one_week_from_epoch);
    }

    #[test]
    fn system_clock_now_returns_a_time_after_the_epoch() {
        let clock = SystemClock;

        let now = clock.now();

        assert!(now.as_millis_since_epoch() > 0);
    }

    #[test]
    fn test_clock_now_returns_the_fixed_instant() {
        let charge_renewal = Timestamp::from_millis_since_epoch(1_753_401_600_000);
        let clock = TestClock::at(charge_renewal);

        assert_eq!(clock.now(), charge_renewal);
    }

    #[test]
    fn test_clock_advance_millis_moves_the_clock_forward() {
        let charge_renewal = Timestamp::from_millis_since_epoch(1_753_401_600_000);
        let clock = TestClock::at(charge_renewal);

        // "Charge in a week" compressed to milliseconds.
        clock.advance_millis(50);

        assert_eq!(
            clock.now(),
            Timestamp::from_millis_since_epoch(1_753_401_600_050)
        );
    }

    #[test]
    fn system_clock_sleep_until_returns_immediately_when_the_deadline_has_passed() {
        let clock = SystemClock;
        let already_past = Timestamp::from_millis_since_epoch(1);

        let before = clock.now();
        clock.sleep_until(already_past);
        let after = clock.now();

        // No observable wait: allow generous scheduling slack rather than
        // asserting an exact zero-duration bound.
        assert!(after.as_millis_since_epoch() - before.as_millis_since_epoch() < 1000);
    }

    #[test]
    fn test_clock_sleep_until_never_blocks() {
        let clock = TestClock::at(Timestamp::from_millis_since_epoch(0));
        let one_week_away = Timestamp::from_millis_since_epoch(604_800_000);

        clock.sleep_until(one_week_away);

        assert_eq!(clock.now(), Timestamp::from_millis_since_epoch(0));
    }
}
