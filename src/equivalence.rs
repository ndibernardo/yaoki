//! Recovery equivalence contracts. The sealed `Equivalence` trait and its
//! three modes (`ExactlyOnce`, `DuplicateLast`, `ReplayAll`) land once the
//! engine can replay; `TransactionalBoundary` arrives first because stores
//! need to declare it.

/// Marker for a `JournalStore` whose journal append and side effects commit
/// atomically. `MemoryJournal` qualifies trivially (one process, one
/// memory); a `FileJournal` writing to disk while a step calls out over the
/// network does not.
pub trait TransactionalBoundary {}
