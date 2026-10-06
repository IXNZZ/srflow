//! A14 借用冲突负例：借用仍在使用时不能可变移除对应 Data。
//!
//! 单独编译并确认在 `remove_owned` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/a14_borrow_while_removing.rs \
//!   -o /tmp/a14_borrow_while_removing.rmeta
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

    // 借用仍然存活：移出 owned 值需要 `&mut container`，预期 E0502。
    let _taken = container.remove_owned::<Item>(&id).unwrap();

    // 把借用使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = borrowed.0;
}