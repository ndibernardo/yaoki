//! `FileJournal`: one append-only file per execution, `<dir>/<id-hex>.journal`.
//!
//! Framing: each event is one frame `crc32:len:<json>\n`. `append` fsyncs
//! after every write: durability before acknowledgement is the point.
//! `load` re-parses the frames; a torn write (crash mid-append) leaves an
//! incomplete or CRC-mismatched tail frame, which `load` detects, truncates
//! off the file, and drops from the returned `Journal`. The store heals
//! itself on the next read instead of staying corrupt forever.
//!
//! Serde is confined to this file: `JournalEventRecord` mirrors
//! `JournalEvent` with plain, derive-friendly fields, converted via
//! `From<&JournalEvent>` / `TryFrom<JournalEventRecord>`. `JournalEvent`
//! itself stays serde-free.

use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::execution::ExecutionId;
use crate::execution::WorkflowErrorRecord;
use crate::execution::WorkflowName;
use crate::execution::WorkflowVersion;
use crate::journal::EventPayload;
use crate::journal::Journal;
use crate::journal::JournalError;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;
use crate::journal::Seq;
use crate::random::RandomBytes;
use crate::step::Attempt;
use crate::step::StepErrorRecord;
use crate::step::StepName;
use crate::time::Deadline;
use crate::time::Timestamp;

/// On-disk mirror of `JournalEvent`. Plain fields only; domain validation
/// happens in `TryFrom<JournalEventRecord> for JournalEvent`, at the
/// boundary where an on-disk record becomes a domain value.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum JournalEventRecord {
    ExecutionStarted {
        workflow: String,
        version: String,
        input: Vec<u8>,
    },
    StepScheduled {
        seq: u64,
        name: String,
    },
    StepStarted {
        seq: u64,
        attempt: u32,
    },
    StepCompleted {
        seq: u64,
        result: Vec<u8>,
    },
    StepFailed {
        seq: u64,
        attempt: u32,
        error: String,
    },
    NowRecorded {
        seq: u64,
        value: u64,
    },
    RandomRecorded {
        seq: u64,
        value: [u8; 32],
    },
    TimerScheduled {
        seq: u64,
        deadline: u64,
    },
    TimerFired {
        seq: u64,
    },
    ExecutionCompleted {
        output: Vec<u8>,
    },
    ExecutionFailed {
        error: String,
    },
}

impl From<&JournalEvent> for JournalEventRecord {
    fn from(event: &JournalEvent) -> Self {
        match event {
            JournalEvent::ExecutionStarted {
                workflow,
                version,
                input,
            } => JournalEventRecord::ExecutionStarted {
                workflow: workflow.as_str().to_owned(),
                version: version.as_str().to_owned(),
                input: input.as_bytes().to_vec(),
            },
            JournalEvent::StepScheduled { seq, name } => JournalEventRecord::StepScheduled {
                seq: seq.get(),
                name: name.as_str().to_owned(),
            },
            JournalEvent::StepStarted { seq, attempt } => JournalEventRecord::StepStarted {
                seq: seq.get(),
                attempt: attempt.get(),
            },
            JournalEvent::StepCompleted { seq, result } => JournalEventRecord::StepCompleted {
                seq: seq.get(),
                result: result.as_bytes().to_vec(),
            },
            JournalEvent::StepFailed {
                seq,
                attempt,
                error,
            } => JournalEventRecord::StepFailed {
                seq: seq.get(),
                attempt: attempt.get(),
                error: error.message().to_owned(),
            },
            JournalEvent::NowRecorded { seq, value } => JournalEventRecord::NowRecorded {
                seq: seq.get(),
                value: value.as_millis_since_epoch(),
            },
            JournalEvent::RandomRecorded { seq, value } => JournalEventRecord::RandomRecorded {
                seq: seq.get(),
                value: *value.as_bytes(),
            },
            JournalEvent::TimerScheduled { seq, deadline } => JournalEventRecord::TimerScheduled {
                seq: seq.get(),
                deadline: deadline.timestamp().as_millis_since_epoch(),
            },
            JournalEvent::TimerFired { seq } => JournalEventRecord::TimerFired { seq: seq.get() },
            JournalEvent::ExecutionCompleted { output } => JournalEventRecord::ExecutionCompleted {
                output: output.as_bytes().to_vec(),
            },
            JournalEvent::ExecutionFailed { error } => JournalEventRecord::ExecutionFailed {
                error: error.message().to_owned(),
            },
        }
    }
}

impl TryFrom<JournalEventRecord> for JournalEvent {
    type Error = JournalError;

    fn try_from(record: JournalEventRecord) -> Result<Self, JournalError> {
        let malformed = |message: String| JournalError::Codec { message };
        match record {
            JournalEventRecord::ExecutionStarted {
                workflow,
                version,
                input,
            } => Ok(JournalEvent::ExecutionStarted {
                workflow: WorkflowName::new(workflow).map_err(|e| malformed(e.to_string()))?,
                version: WorkflowVersion::new(version).map_err(|e| malformed(e.to_string()))?,
                input: EventPayload::new(input),
            }),
            JournalEventRecord::StepScheduled { seq, name } => Ok(JournalEvent::StepScheduled {
                seq: Seq::from_index(seq),
                name: StepName::new(name).map_err(|e| malformed(e.to_string()))?,
            }),
            JournalEventRecord::StepStarted { seq, attempt } => Ok(JournalEvent::StepStarted {
                seq: Seq::from_index(seq),
                attempt: Attempt::new(attempt).map_err(|e| malformed(e.to_string()))?,
            }),
            JournalEventRecord::StepCompleted { seq, result } => Ok(JournalEvent::StepCompleted {
                seq: Seq::from_index(seq),
                result: EventPayload::new(result),
            }),
            JournalEventRecord::StepFailed {
                seq,
                attempt,
                error,
            } => Ok(JournalEvent::StepFailed {
                seq: Seq::from_index(seq),
                attempt: Attempt::new(attempt).map_err(|e| malformed(e.to_string()))?,
                error: StepErrorRecord::new(error),
            }),
            JournalEventRecord::NowRecorded { seq, value } => Ok(JournalEvent::NowRecorded {
                seq: Seq::from_index(seq),
                value: Timestamp::from_millis_since_epoch(value),
            }),
            JournalEventRecord::RandomRecorded { seq, value } => Ok(JournalEvent::RandomRecorded {
                seq: Seq::from_index(seq),
                value: RandomBytes::new(value),
            }),
            JournalEventRecord::TimerScheduled { seq, deadline } => {
                Ok(JournalEvent::TimerScheduled {
                    seq: Seq::from_index(seq),
                    deadline: Deadline::at(Timestamp::from_millis_since_epoch(deadline)),
                })
            }
            JournalEventRecord::TimerFired { seq } => Ok(JournalEvent::TimerFired {
                seq: Seq::from_index(seq),
            }),
            JournalEventRecord::ExecutionCompleted { output } => {
                Ok(JournalEvent::ExecutionCompleted {
                    output: EventPayload::new(output),
                })
            }
            JournalEventRecord::ExecutionFailed { error } => Ok(JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new(error),
            }),
        }
    }
}

/// CRC-32 (IEEE 802.3, reflected, poly 0xEDB88320), table-driven.
const fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = crc32_table();

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    crc ^ 0xFFFF_FFFF
}

fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = format!("{:08x}:{}:", crc32(payload), payload.len()).into_bytes();
    frame.extend_from_slice(payload);
    frame.push(b'\n');
    frame
}

/// Parses as many well-formed `crc32:len:payload\n` frames as `bytes` holds
/// a complete, CRC-valid copy of. Stops at the first header that doesn't
/// parse, the first length that runs past the end of `bytes`, or the first
/// payload whose CRC doesn't match. Those are exactly the shapes a crash
/// mid-`write` can leave behind. Returns the frame payloads and the byte
/// count consumed by good frames alone, so the caller can truncate the rest.
fn parse_frames(bytes: &[u8]) -> (Vec<&[u8]>, usize) {
    let mut payloads = Vec::new();
    let mut position = 0usize;
    while let Some((payload, frame_len)) = parse_one_frame(&bytes[position..]) {
        payloads.push(payload);
        position += frame_len;
    }
    (payloads, position)
}

fn parse_one_frame(bytes: &[u8]) -> Option<(&[u8], usize)> {
    let crc_end = bytes.iter().position(|&b| b == b':')?;
    let expected_crc =
        u32::from_str_radix(std::str::from_utf8(&bytes[..crc_end]).ok()?, 16).ok()?;

    let len_field = &bytes[crc_end + 1..];
    let len_end = len_field.iter().position(|&b| b == b':')?;
    let len: usize = std::str::from_utf8(&len_field[..len_end])
        .ok()?
        .parse()
        .ok()?;

    let payload_start = crc_end + 1 + len_end + 1;
    let payload_end = payload_start.checked_add(len)?;
    if payload_end >= bytes.len() || bytes[payload_end] != b'\n' {
        return None; // torn write: header present, payload/newline is not
    }

    let payload = &bytes[payload_start..payload_end];
    if crc32(payload) != expected_crc {
        return None; // torn write: payload bytes only partially overwritten
    }
    Some((payload, payload_end + 1))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn io_error(source: io::Error) -> JournalError {
    JournalError::Io {
        message: source.to_string(),
    }
}

/// One append-only file per execution: `<dir>/<execution-id-hex>.journal`.
/// `MemoryJournal`'s append+effect atomicity does not hold across a process
/// boundary, so this store does not implement `TransactionalBoundary`.
pub struct FileJournal {
    dir: PathBuf,
}

impl FileJournal {
    /// Creates `dir` (and any missing parents) if it doesn't exist yet.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, JournalError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(io_error)?;
        Ok(Self { dir })
    }

    fn path_for(&self, id: &ExecutionId) -> PathBuf {
        self.dir
            .join(format!("{}.journal", hex_encode(id.as_bytes())))
    }

    /// Reads and decodes every well-formed frame in `path`. If the file's
    /// tail is a torn write, truncates the file down to the last good frame
    /// before returning. The file heals on this read, not just in memory.
    fn read_and_heal(path: &Path) -> Result<Vec<JournalEvent>, JournalError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(path).map_err(io_error)?;
        let (payloads, good_len) = parse_frames(&bytes);

        let mut events = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let record: JournalEventRecord =
                serde_json::from_slice(payload).map_err(|e| JournalError::Codec {
                    message: e.to_string(),
                })?;
            events.push(JournalEvent::try_from(record)?);
        }

        if good_len < bytes.len() {
            let file = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(io_error)?;
            file.set_len(good_len as u64).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
        }

        Ok(events)
    }
}

impl JournalStore for FileJournal {
    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<Seq, JournalError> {
        let path = self.path_for(id);
        let existing = Self::read_and_heal(&path)?;
        let position = Seq::from_index(existing.len() as u64);

        let record = JournalEventRecord::from(&event);
        let payload = serde_json::to_vec(&record).map_err(|e| JournalError::Codec {
            message: e.to_string(),
        })?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_error)?;
        file.write_all(&encode_frame(&payload)).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;

        Ok(position)
    }

    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError> {
        let path = self.path_for(id);
        Ok(Journal::new(Self::read_and_heal(&path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::random::RngSource;

    struct FixedRng {
        bytes: [u8; 32],
    }

    impl RngSource for FixedRng {
        fn next_bytes(&mut self) -> RandomBytes {
            RandomBytes::new(self.bytes)
        }
    }

    fn signup_execution() -> ExecutionId {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x51; // 'Q', arbitrary deterministic marker
        let mut rng = FixedRng { bytes };
        ExecutionId::generate(&mut rng)
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("yaoki-file-journal-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn execution_started() -> JournalEvent {
        JournalEvent::ExecutionStarted {
            workflow: WorkflowName::new("signup").unwrap(),
            version: WorkflowVersion::new("2026.07.18").unwrap(),
            input: EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
        }
    }

    fn step_scheduled(name: &str) -> JournalEvent {
        JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new(name).unwrap(),
        }
    }

    fn step_completed(seq: Seq) -> JournalEvent {
        JournalEvent::StepCompleted {
            seq,
            result: EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec()),
        }
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        // The industry-standard CRC-32/ISO-HDLC conformance vector, not
        // domain data: every implementation is checked against this string.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn load_on_unknown_execution_returns_empty_journal() {
        let store = FileJournal::new(temp_dir("unknown-execution")).unwrap();
        let execution = signup_execution();

        let journal = store.load(&execution).unwrap();

        assert!(journal.is_empty());
    }

    #[test]
    fn append_then_load_preserves_order() {
        let store = FileJournal::new(temp_dir("append-then-load")).unwrap();
        let execution = signup_execution();

        store.append(&execution, execution_started()).unwrap();
        store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();
        let journal = store.load(&execution).unwrap();

        assert_eq!(
            journal.events(),
            &[execution_started(), step_scheduled("charge-card")]
        );
    }

    #[test]
    fn append_returns_the_zero_based_position_of_the_appended_event() {
        let store = FileJournal::new(temp_dir("append-position")).unwrap();
        let execution = signup_execution();

        let first_position = store.append(&execution, execution_started()).unwrap();
        let second_position = store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();

        assert_eq!(first_position.get(), 0);
        assert_eq!(second_position.get(), 1);
    }

    #[test]
    fn different_executions_have_independent_files() {
        let dir = temp_dir("independent-files");
        let store = FileJournal::new(&dir).unwrap();
        let signup = signup_execution();
        let mut renewal_bytes = [0u8; 32];
        renewal_bytes[0] = 0x52; // 'R', arbitrary deterministic marker
        let renewal = ExecutionId::generate(&mut FixedRng {
            bytes: renewal_bytes,
        });

        store.append(&signup, execution_started()).unwrap();
        let renewal_journal = store.load(&renewal).unwrap();
        let signup_journal = store.load(&signup).unwrap();

        assert!(renewal_journal.is_empty());
        assert_eq!(signup_journal.len(), 1);
    }

    #[test]
    fn journal_survives_a_reopened_store_over_the_same_directory() {
        // Append via one `FileJournal`, drop it, open a second over
        // the same directory (simulates a process restart with the journal
        // directory kept).
        let dir = temp_dir("reopen-store");
        let execution = signup_execution();
        {
            let store = FileJournal::new(&dir).unwrap();
            store.append(&execution, execution_started()).unwrap();
            store
                .append(&execution, step_scheduled("charge-card"))
                .unwrap();
        }

        let reopened = FileJournal::new(&dir).unwrap();
        let journal = reopened.load(&execution).unwrap();

        assert_eq!(
            journal.events(),
            &[execution_started(), step_scheduled("charge-card")]
        );
    }

    #[test]
    fn load_truncates_a_torn_write_off_the_tail_and_returns_only_good_frames() {
        // Two good frames, then a manually appended garbage tail
        // simulating a `kill -9` mid-`write_all` of a third frame.
        let dir = temp_dir("torn-write-truncation");
        let store = FileJournal::new(&dir).unwrap();
        let execution = signup_execution();
        store.append(&execution, execution_started()).unwrap();
        store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();
        let path = store.path_for(&execution);
        let good_len = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            // Well-formed header, payload cut off mid-write, with no trailing
            // newline, exactly what a crash mid-`write_all` leaves behind.
            file.write_all(b"deadbeef:40:{\"kind\":\"StepStarted\",\"seq\":1")
                .unwrap();
        }

        let journal = store.load(&execution).unwrap();

        // Only the two good frames come back, and the file itself
        // was truncated back to them (self-healed, not just filtered in
        // memory).
        assert_eq!(
            journal.events(),
            &[execution_started(), step_scheduled("charge-card")]
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn load_truncates_a_frame_with_a_corrupted_payload_and_bad_crc() {
        // Two good frames, then a complete frame whose payload byte
        // was flipped after the CRC was computed (bit rot / a torn write
        // that happened to land exactly on a frame boundary).
        let dir = temp_dir("corrupted-payload-crc");
        let store = FileJournal::new(&dir).unwrap();
        let execution = signup_execution();
        store.append(&execution, execution_started()).unwrap();
        let good_len = fs::metadata(store.path_for(&execution)).unwrap().len();
        let path = store.path_for(&execution);
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            // Correct length (29 bytes) and a well-placed trailing newline,
            // but a CRC that does not match the payload.
            file.write_all(b"00000000:29:{\"kind\":\"TimerFired\",\"seq\":9}\n")
                .unwrap();
        }

        let journal = store.load(&execution).unwrap();

        assert_eq!(journal.events(), &[execution_started()]);
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn append_after_a_healed_torn_write_continues_at_the_correct_position() {
        // A torn write left a garbage tail after two good frames.
        let dir = temp_dir("append-after-heal");
        let store = FileJournal::new(&dir).unwrap();
        let execution = signup_execution();
        store.append(&execution, execution_started()).unwrap();
        store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();
        let path = store.path_for(&execution);
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"garbage-not-even-a-frame").unwrap();
        }

        // Append triggers a heal (via read_and_heal) before writing the
        // third frame.
        let third_position = store
            .append(&execution, step_completed(Seq::zero()))
            .unwrap();
        let journal = store.load(&execution).unwrap();

        assert_eq!(third_position.get(), 2);
        assert_eq!(
            journal.events(),
            &[
                execution_started(),
                step_scheduled("charge-card"),
                step_completed(Seq::zero()),
            ]
        );
    }

    #[test]
    fn journal_event_record_round_trips_every_variant_through_json() {
        // One event per `JournalEvent` variant, each with
        // domain-meaningful field values.
        let events = [
            execution_started(),
            step_scheduled("charge-card"),
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
            step_completed(Seq::zero()),
            JournalEvent::StepFailed {
                seq: Seq::zero(),
                attempt: Attempt::first(),
                error: StepErrorRecord::new("payment gateway timed out"),
            },
            JournalEvent::NowRecorded {
                seq: Seq::zero(),
                value: Timestamp::from_millis_since_epoch(1_753_401_600_000),
            },
            JournalEvent::RandomRecorded {
                seq: Seq::zero(),
                value: RandomBytes::new([0x7a; 32]),
            },
            JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline: Deadline::at(Timestamp::from_millis_since_epoch(1_753_401_600_000)),
            },
            JournalEvent::TimerFired { seq: Seq::zero() },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(br#"{"account_id":"acct_2026_0718"}"#.to_vec()),
            },
            JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new("account creation rolled back"),
            },
        ];

        for event in events {
            let record = JournalEventRecord::from(&event);
            let json = serde_json::to_vec(&record).unwrap();
            let decoded_record: JournalEventRecord = serde_json::from_slice(&json).unwrap();
            let round_tripped = JournalEvent::try_from(decoded_record).unwrap();

            assert_eq!(round_tripped, event, "round trip mismatch for {event:?}");
        }
    }

    #[test]
    fn try_from_record_rejects_a_malformed_step_name() {
        // A CRC-valid, well-formed frame whose payload decodes to
        // a `StepName` the validated constructor rejects.
        let record = JournalEventRecord::StepScheduled {
            seq: 0,
            name: String::new(),
        };

        let result = JournalEvent::try_from(record);

        assert!(matches!(result, Err(JournalError::Codec { .. })));
    }

    #[test]
    fn new_creates_the_journal_directory_if_missing() {
        let dir = temp_dir("creates-directory")
            .join("nested")
            .join("journals");
        assert!(!dir.exists());

        let _store = FileJournal::new(&dir).unwrap();

        assert!(dir.is_dir());
    }
}
