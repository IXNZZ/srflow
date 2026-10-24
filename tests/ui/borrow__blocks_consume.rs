//! C23 借用冲突负例：resolve 得到的借用仍在使用时，Consume（需要 `&mut self`）被拒绝。
//!
//! 单独编译并确认在 `consume_item` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/borrow__blocks_consume.rs \
//!   -o /tmp/c23_borrow_while_consuming.rmeta
//! ```
//!
//! 夹具装配真实内部协调组件；resolve 与 consume_item 的接收者是同一个变量。
//! V22-T03 迁移：源 `tests/ui/c23_borrow_while_consuming.rs`。本夹具只以 `#[path]` 装配真实 src/core，不触及 Root／Flow 公开
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
    let each = coordinator.create_child(&root).unwrap();
    let collector = coordinator.begin_collector::<Item>(&each).unwrap();

    let item = coordinator.create_child(&each).unwrap();
    let out = ids.allocate().unwrap();
    coordinator.register_owned(&item, &out, Item(1)).unwrap();

    let borrowed = coordinator.resolve::<Item>(&item, &out).unwrap();

    // 借用仍然存活：Consume 需要 `&mut coordinator`，预期 E0502。
    coordinator.consume_item(&item, &out, &collector).unwrap();

    // 把借用使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = borrowed.0;
}