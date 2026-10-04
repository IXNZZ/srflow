//! B19 借用冲突负例：resolve 得到的借用仍在使用时，失败清理（需要 `&mut self`）被拒绝。
//!
//! 单独编译并确认在 `abort` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/b19_borrow_while_aborting.rs \
//!   -o /tmp/b19_borrow_while_aborting.rmeta
//! ```

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