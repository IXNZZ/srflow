//! K23 负例：真实 `&T` 借用存活时不能可变提取。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/borrow__blocks_extraction.rs -o /tmp/k04.rmeta
//! ```
//! 预期：E0502——借用未结束时不能调用 `&mut` 提取入口。
//! V22-T03 迁移：源 `tests/ui/k04_borrow_while_extracting.rs`。本夹具只以 `#[path]` 装配真实 src/core，不触及 Root／Flow 公开
//! 签名（T03 只改 `then`/Root 输出声明），因此判据与拒绝码逐字承接：E0502。
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
