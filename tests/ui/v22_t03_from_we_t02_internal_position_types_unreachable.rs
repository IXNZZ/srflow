//! WE-T02 B22 负例：内部位置／类型描述类型不在 crate 根可达。
//!
//! `PositionHandle`／`PositionDesc`／`TypeDesc` 属于 crate 内部装配细节，不作为公开
//! 入口导出；使用者不能命名、构造它们，也就不能伪造一个无来源的逻辑位置。
//!
//! 期望主诊断：E0432（`srflow` 根下不存在这些名字的导入）。
//! V22-T03 迁移：源 `tests/ui/we_t02_internal_position_types_unreachable.rs`。本夹具不使用 Root／Flow 公开签名，判据与
//! 拒绝码逐字承接：E0432。

use srflow::{PositionDesc, PositionHandle, TypeDesc};

fn main() {
    // 内部类型在 crate 根不可达。
    let _ = PositionHandle::default();
    let _ = PositionDesc::default();
    let _ = TypeDesc::default();
}
