//! WE-T02 B22 负例：内部位置／类型描述类型不在 crate 根可达。
//!
//! `PositionHandle`／`PositionDesc`／`TypeDesc` 属于 crate 内部装配细节，不作为公开
//! 入口导出；使用者不能命名、构造它们，也就不能伪造一个无来源的逻辑位置。
//!
//! 期望主诊断：E0432（`srflow` 根下不存在这些名字的导入）。

use srflow::{PositionDesc, PositionHandle, TypeDesc};

fn main() {
    // 内部类型在 crate 根不可达。
    let _ = PositionHandle::default();
    let _ = PositionDesc::default();
    let _ = TypeDesc::default();
}
