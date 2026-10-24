//! T03 负例：仅实现 `FnMut` 的闭包不能作为 Factory。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/factory__fn_mut_only_rejected.rs -o /tmp/v22_t03_from_t02_neg_provide_fnonly.rmeta
//! ```
//!
//! 预期主诊断：E0594（按 `Fn` 推断后，闭包内对被捕获变量赋值被拒绝），落在 `provide` 调用行。
//!
//! T03 迁移：`provide` 已是公开方法，Factory 仍只接受 `Fn`；调用点、断言与诊断阶段不变，
//! 原“仅 FnMut 的闭包被拒绝”的目的完整保留。
#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::flow::Flow;

struct Amount(u32);
impl Data for Amount {}

fn wire<'n>(flow: &mut Flow<'n>) {
    let mut count = 0u32;
    // 期望主诊断：E0594——`count += 1` 使闭包不能满足 `Fn`。
    flow.provide(move || {
        count += 1;
        Amount(count)
    });
}

fn main() {}
