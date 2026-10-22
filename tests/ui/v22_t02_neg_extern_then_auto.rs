//! T02 外部可见性负例：`Flow::then_auto` 未公开（T02 阶段内部入口）。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_then_auto.rs -o /tmp/v22_t02_neg_extern_then_auto.rmeta
//! ```
//!
//! 预期主诊断：E0624（关联函数／方法是私有的），落在 `then_auto` 调用行。

use srflow::{BodyError, Flow, Query};

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

fn wire(flow: &mut Flow<'_>) {
    // 期望主诊断：E0624——内部自动入口不对外可见。
    flow.then_auto(&add_one);
}

fn main() {}
