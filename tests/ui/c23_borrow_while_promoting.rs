//! C23 借用冲突负例：resolve 得到的借用仍在使用时，Promote（需要 `&mut self`）被拒绝。
//!
//! 单独编译并确认在 `promote` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/c23_borrow_while_promoting.rs \
//!   -o /tmp/c23_borrow_while_promoting.rmeta
//! ```
//!
//! 夹具装配真实内部协调组件：registry、Container、状态登记与 resolve 的接收者是同一个变量。

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
    let controller = coordinator.create_child(&root).unwrap();

    let seed = ids.allocate().unwrap();
    coordinator
        .register_owned(&controller, &seed, Item(1))
        .unwrap();
    let state = coordinator
        .register_state::<Item>(&controller, &seed)
        .unwrap();

    let round = coordinator.create_child(&controller).unwrap();
    let out = ids.allocate().unwrap();
    coordinator.register_owned(&round, &out, Item(2)).unwrap();

    let borrowed = coordinator.resolve::<Item>(&round, &out).unwrap();

    // 借用仍然存活：Promote 需要 `&mut coordinator`，预期 E0502。
    coordinator.promote(&round, &out, &state).unwrap();

    // 把借用使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = borrowed.0;
}