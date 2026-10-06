//! J 负例：真实业务借用未结束时，受控收口（Round 边界提交）不可调用。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j07_borrow_blocks_round_collection.rs -o /tmp/j07.rmeta
//! ```
//! 预期：E0502（`ExecutionContext` 已被不可变借用，无法可变借用），证明"借用结束前不可
//! 变更状态"，而不是靠运行期约定。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::ExecutionContext;
use core::identity::ScopeId;
use core::internal_error::ScopeError;

fn collect_while_borrowed(
    ctx: &mut ExecutionContext,
    scope: &ScopeId,
    round: &ScopeId,
    permit: &core::context::RoundCollectPermit,
    selected: &core::ref_id::RefId,
) -> Result<(), ScopeError> {
    // 业务借用仍未结束：这里对 Context 再取可变借用必须被拒绝。
    let borrowed: &u32 = ctx.resolve::<u32>(scope, selected)?;
    let _ = ctx.promote_in_round_boundary(round, selected, permit);
    let _ = *borrowed;
    Ok(())
}

fn main() {
    let _ = collect_while_borrowed;
}
