//! 判定能力见证：错误码相同但符号/位置不同的私有访问，不能冒充目标入口被拒。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_witness_wrong_symbol_e0624.rs \
//!   -o /tmp/v22_t02_witness_wrong_symbol_e0624.rmeta
//! ```
//!
//! 本文件调用 `Flow::then_auto`（E0624）；见证清单要求的是 `provide_output`，
//! 因此严格匹配必须判 FAIL（仅比较错误码的旧判定会误判 PASS）。

use srflow::{BodyError, Flow, Query};

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

fn wire(flow: &mut Flow<'_>) {
    flow.then_auto(&add_one);
}

fn main() {}
