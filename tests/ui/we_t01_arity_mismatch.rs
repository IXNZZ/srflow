//! WE-T01 A05 负例 2：arity 不匹配——2 输入 Node 只收到 1 个 Ref。
//!
//! 意图：Node 输入形状（`Query<(&A, &B)>` → `A::Spec = (A, B)`）与提供的 Ref 形状
//! 必须逐槽位一致，数量不匹配在编译期拒绝。
//!
//! 预期主诊断：E0271（`<Amount as InputSpec>::Borrowed<'a> == (&Amount, &Other)` 无法满足），
//! 落在本文件第 39 行 `flow.then(two_inputs, amount)`（2026-10-11 实测）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t01_arity_mismatch.rs -o /tmp/we_t01_arity_mismatch.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;
use crate::core::engine::error::BodyError;
use crate::core::engine::shape::Query;

struct Amount(u32);

impl Data for Amount {}

struct Other(u32);

impl Data for Other {}

/// 2 输入函数 Node：声明 `Query<(&Amount, &Other)>`。
async fn two_inputs(query: Query<(&Amount, &Other)>) -> Result<(), BodyError> {
    let (_first, _second) = query.get();
    Ok(())
}

fn main() {
    let _ = build_definition::<Amount, _, _>(|flow, amount| {
        // 期望主诊断：E0271——只提供 1 个 Ref，而 Node 声明需要 2 个输入槽位。
        flow.then(two_inputs, amount)
    });
}
