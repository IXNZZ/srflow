//! B19 借用冲突负例：resolve 得到的借用仍在使用时，失败清理（需要 `&mut self`）被拒绝。
//!
//! 单独编译并确认在 `abort` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t03_from_legacy_b19_borrow_while_aborting.rs \
//!   -o /tmp/b19_borrow_while_aborting.rmeta
//! ```
//! V22-T03 迁移：源 `tests/ui/b19_borrow_while_aborting.rs`。本夹具只以 `#[path]` 装配真实 src/core，不触及 Root／Flow 公开
//! 签名（T03 只改 `then`/Root 输出声明），因此判据与拒绝码逐字承接：E0502。

#[path = "../../src/core/mod.rs"]
mod core;

use core::identity::ExecutionIdentity;
use core::ref_id::{RefIdAllocator, RefIdSource};
use core::scope::ScopeCoordinator;

struct Item(u32);

fn main() {
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let ids = RefIdAllocator::new(RefIdSource::new());
    let root = coordinator.root();
    let position = ids.allocate().unwrap();
    coordinator.register_owned(&root, &position, Item(1)).unwrap();

    let borrowed = coordinator.resolve::<Item>(&root, &position).unwrap();

    // 借用仍然存活：失败清理需要 `&mut coordinator`，预期 E0502。
    coordinator.abort(&root).unwrap();

    let _still_borrowed = borrowed.0;
}