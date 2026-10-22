//! T02 外部可见性负例：`Flow::then_with_output` 未公开。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_then_with_output.rs \
//!   -o /tmp/v22_t02_neg_extern_then_with_output.rmeta
//! ```
//!
//! 预期主诊断：E0624，落在 `then_with_output` 调用行。

use srflow::{BodyError, Flow, Query, Ref};

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

fn wire(flow: &mut Flow<'_>, input: Ref<u32>) {
    // 期望主诊断：E0624。
    let _ = flow.then_with_output(&add_one, input);
}

fn main() {}
