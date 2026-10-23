//! 判定能力见证：错误码相同但锚点行无该诊断的私有访问，不能冒充目标入口被拒。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_witness_wrong_symbol_e0624.rs \
//!   -o /tmp/v22_t03_from_t02_witness_wrong_symbol_e0624.rmeta
//! ```
//!
//! 本文件调用仍未公开的 `Flow::next`（`pub(crate)`，外部可见性 = E0624）；见证清单声明的锚点是
//! `async fn add_one(...)` 那一行——该行没有任何主诊断。因此严格匹配必须判 FAIL
//! （仅比较错误码的宽松判定会误判 PASS）。
//!
//! T03 迁移：原见证用当时私有的 `then_auto` 制造私有错误锚点，该 T02 内部入口已在 T03 退场为
//! 公开方法（正例见 `v22_t03_from_t02_neg_extern_then_auto.rs`），故改用仍是 `pub(crate)` 的
//! `Flow::next` 制造同一 E0624 错位；见证目的（宽松判定误通过、严格判定拒绝）与判定字段不变。

use srflow::{BodyError, Flow, Query};

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

fn wire(flow: &mut Flow<'_>) {
    // 期望：E0624（关联方法是私有的）——但见证清单的锚点不在此行。
    flow.next::<u32>();
}

fn main() {}
