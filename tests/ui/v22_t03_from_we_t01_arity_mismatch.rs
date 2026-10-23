//! WE-T01 A05 负例 2：arity 不匹配——2 输入 Node 只收到 1 个 Ref。
//!
//! 意图：Node 输入形状（`Query<(&A, &B)>` → `A::Spec = (A, B)`）与提供的 Ref 形状
//! 必须逐槽位一致，数量不匹配在编译期拒绝。
//!
//! 预期主诊断：E0271（`<Amount as InputSpec>::Borrowed<'a> == (&Amount, &Other)` 无法满足），
//! 落在本文件 `main` 内的显式接线调用行（同一行多条 E0271，实测 3 条）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_arity_mismatch.rs -o /tmp/v22_t03_from_we_t01_arity_mismatch.rmeta
//! ```
//!
//! T03 迁移：旧两参 `then(two_inputs, amount)` 改为显式取输出的 `then_with_output`；
//! Root 闭包固定返回 `()`，Out 泛型取原尾值形状的 owned `()`（2 输入 Node 输出为 `()`）。
//! 拒绝阶段与目的不变（接线调用本身即拒绝点）。

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
    let _ = build_definition::<Amount, (), _>(|flow, amount| {
        // 期望主诊断：E0271——只提供 1 个 Ref，而 Node 声明需要 2 个输入槽位。
        let refs = flow.then_with_output(two_inputs, amount);
        flow.output_with(refs);
    });
}
