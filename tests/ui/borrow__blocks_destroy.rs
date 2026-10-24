//! A14 借用冲突负例：借用仍在使用时不能可变销毁对应 Data。
//!
//! 本文件不是 Cargo target（`tests/ui/` 下没有 `main.rs`，Cargo 不会自动登记），
//! 需单独编译并确认在 `destroy` 处因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/borrow__blocks_destroy.rs \
//!   -o /tmp/a14_borrow_while_destroying.rmeta
//! ```
//!
//! `#[path]` 装配的是真实实现源码，而不是简化副本；负例必须命中借用冲突本身，
//! 而不是缺失导入、不可见类型或无关约束。
//! V22-T03 迁移：源 `tests/ui/a14_borrow_while_destroying.rs`。本夹具只以 `#[path]` 装配真实 src/core，不触及 Root／Flow 公开
//! 签名（T03 只改 `then`/Root 输出声明），因此判据与拒绝码逐字承接：E0502。
#[path = "../../src/core/mod.rs"]
mod core;

use core::data_container::DataContainer;
use core::identity::ExecutionIdentity;

struct Item(u32);

fn main() {
    let mut container = DataContainer::with_identity(ExecutionIdentity::new());
    let id = container.insert_owned(Item(1)).unwrap();
    let borrowed = container.borrow::<Item>(&id).unwrap();

    // 借用仍然存活：此处需要 `&mut container`，预期 E0502。
    container.destroy(&id).unwrap();

    // 把借用使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = borrowed.0;
}