//! T02 负例：Factory 移走唯一捕获值（FnOnce 语义）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_provide_fnonce.rs -o /tmp/v22_t02_neg_provide_fnonce.rmeta
//! ```
//!
//! 预期主诊断：E0507（无法从捕获变量中移出），落在 `provide` 调用行；契约只接受 `Fn() -> T`。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::flow::Flow;

struct Amount(u32);
impl Data for Amount {}

fn wire<'n>(flow: &mut Flow<'n>) {
    let owned = Amount(1);
    // 期望主诊断：E0507——返回捕获值需要移出，闭包因此不是 `Fn`。
    flow.provide(move || owned);
}

fn main() {}
