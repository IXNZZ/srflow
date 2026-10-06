//! C12 边界负例：`CollectorId` 不是 `DataId`，未完成 collector 不能走普通 Data borrow。
//!
//! 单独编译并确认在 `borrow` 调用处因 E0308 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/c12_collector_id_is_not_data_id.rs \
//!   -o /tmp/c12_collector_id_is_not_data_id.rmeta
//! ```
//!
//! 预期失败：collector 身份与普通 Data 身份是两个类型，普通 borrow 只接受 `&DataId`。

#[path = "../../src/core/mod.rs"]
mod core;

use std::sync::Arc;

use core::data_container::DataContainer;
use core::identity::{CollectorIdAllocator, ExecutionIdentity};

fn main() {
    let identity = ExecutionIdentity::new();
    let mut container = DataContainer::with_identity(Arc::clone(&identity));
    let collector = CollectorIdAllocator::new(identity).allocate().unwrap();
    container.begin_collector::<u32>(&collector).unwrap();

    // CollectorId 不能定位普通 entry：预期 E0308（expected `&DataId`）。
    let _ = container.borrow::<u32>(&collector);
}