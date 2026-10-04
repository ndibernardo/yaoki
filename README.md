# yaoki

A synchronous Rust prototype for durable execution. It journals workflow steps,
replays recorded results, and resumes interrupted work. It includes in-memory and
file-backed journals, recorded time and randomness, durable timers, and
crash-recovery tests.

Recovery can repeat external effects that completed before their results were
journaled. It does not provide exactly-once external effects.
