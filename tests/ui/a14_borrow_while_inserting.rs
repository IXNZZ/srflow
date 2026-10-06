//! A14 借用冲突负例：借用仍在使用时不能向容器插入新 entry（同为可变操作）。
//!
//! 单独编译并确认在 `insert_owned` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/a14_borrow_while_inserting.rs \
//!   -o /tmp/a14_borrow_while_inserting.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use core::data_container::DataContainer;
use core::identity::ExecutionIdentity;

struct Item(u32);

fn main() {
    let mut container = DataContainer::with_identity(ExecutionIdentity::new());
    let id = container.insert_owned(Item(1)).unwrap();
    let borrowed = container.borrow::<Item>(&id).unwrap();

    // 借用仍然存活：插入需要 `&mut container`，预期 E0502。
    let _second = container.insert_owned(Item(2)).unwrap();

    let _still_borrowed = borrowed.0;
}