//! T02 外部可见性负例：`Flow::provide_output` 未公开。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_provide_output.rs \
//!   -o /tmp/v22_t02_neg_extern_provide_output.rmeta
//! ```
//!
//! 预期主诊断：E0624，落在 `provide_output` 调用行。

use srflow::Flow;

fn wire(flow: &mut Flow<'_>) {
    // 期望主诊断：E0624。
    let _ = flow.provide_output(|| 1u32);
}

fn main() {}
