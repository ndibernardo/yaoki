//! Compile-fail coverage for execution lifecycle restrictions.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/completed_execution_cannot_start.rs");
}
