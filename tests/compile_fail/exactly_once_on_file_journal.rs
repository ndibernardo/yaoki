//! `FileJournal` is not a `TransactionalBoundary` (disk append and an
//! external side effect cannot commit atomically), so `ExactlyOnce` is not
//! `SupportedOn<FileJournal>`. This must fail to compile, not at runtime.

use yaoki::engine::Engine;
use yaoki::equivalence::ExactlyOnce;
use yaoki::stores::file::FileJournal;

fn main() {
    let store = FileJournal::new(std::env::temp_dir().join("yaoki-trybuild-exactly-once")).unwrap();
    let _engine = Engine::<FileJournal, ExactlyOnce>::new(&store);
}
