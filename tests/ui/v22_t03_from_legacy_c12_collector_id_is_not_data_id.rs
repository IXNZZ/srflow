//! C12 边界负例：`CollectorId` 不是 `DataId`，未完成 collector 不能走普通 Data borrow。
//!
//! 单独编译并确认在 `borrow` 调用处因 E0308 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t03_from_legacy_c12_collector_id_is_not_data_id.rs \
//!   -o /tmp/c12_collector_id_is_not_data_id.rmeta
//! ```
//!
//! 预期失败：collector 身份与普通 Data 身份是两个类型，普通 borrow 只接受 `&DataId`。
//! V22-T03 迁移：源 `tests/ui/c12_collector_id_is_not_data_id.rs`。本夹具只以 `#[path]` 装配真实 src/core，不触及 Root／Flow 公开
//! 签名（T03 只改 `then`/Root 输出声明），因此判据与拒绝码逐字承接：E0308。

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