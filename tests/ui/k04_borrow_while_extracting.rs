//! K23 负例：真实 `&T` 借用存活时不能可变提取。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/k04_borrow_while_extracting.rs -o /tmp/k04.rmeta
//! ```
//! 预期：E0502——借用未结束时不能调用 `&mut` 提取入口。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::ExecutionContext;
use core::identity::ScopeId;
use core::internal_error::ScopeError;
use core::ref_id::RefId;

/// 可变提取入口（生产代码里由 Root guard 持有）。
fn extract(ctx: &mut ExecutionContext, root: &ScopeId, position: &RefId) -> Result<(), ScopeError> {
    let _ = (ctx, root, position);
    Ok(())
}

fn borrow_blocks_extraction(ctx: &mut ExecutionContext, root: &ScopeId, position: &RefId) -> u32 {
    let borrowed: &u32 = ctx.resolve::<u32>(root, position).expect("borrow");
    // 期望：E0502——借用在其后仍被使用时不能可变提取。
    let _ = extract(ctx, root, position);
    *borrowed
}

fn main() {}
