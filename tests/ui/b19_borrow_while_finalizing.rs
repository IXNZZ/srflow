//! B19 借用冲突负例：resolve 得到的借用仍在使用时，正常退出（需要 `&mut self`）被拒绝。
//!
//! 单独编译并确认在 `finalize` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/b19_borrow_while_finalizing.rs \
//!   -o /tmp/b19_borrow_while_finalizing.rmeta
//! ```
//!
//! 夹具装配的是真实内部协调组件：registry 与 Container 位于同一 owner，resolve 与
//! finalize 的接收者是同一个变量。

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

    // 借用仍然存活：正常退出需要 `&mut coordinator`，预期 E0502。
    let declared: Vec<core::ref_id::RefId> = Vec::new();
    let mut outputs: Vec<core::scope::ExportSlot> = Vec::new();
    coordinator.finalize(&root, &declared, &mut outputs).unwrap();

    // 把借用使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = borrowed.0;
}