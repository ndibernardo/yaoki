use std::env;
use std::ptr;

use yaoki::engine::Engine;
use yaoki::stores::file::FileJournal;
use yaoki::stores::memory::MemoryJournal;

#[test]
fn engine_with_memory_store_and_no_guarantee_parameter_borrows_the_store() {
    let store = MemoryJournal::new();

    let engine = Engine::<MemoryJournal>::new(&store);

    assert!(ptr::eq(engine.store(), &store));
}

#[test]
fn engine_with_file_store_and_no_guarantee_parameter_borrows_the_store() {
    // Borrowing the store requires no journal writes or scratch directory.
    let store = FileJournal::new(env::current_dir().unwrap()).unwrap();

    let engine = Engine::<FileJournal>::new(&store);

    assert!(ptr::eq(engine.store(), &store));
}
