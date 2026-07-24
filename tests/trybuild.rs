//! Compile-fail harness: pins two impossibilities the type system
//! must enforce, forever, at compile time rather than at runtime.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/exactly_once_on_file_journal.rs");
    t.compile_fail("tests/compile_fail/completed_execution_cannot_start.rs");
}
