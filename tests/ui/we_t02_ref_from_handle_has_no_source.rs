//! WE-T02 B22 负例：内部 Ref 构造入口对外不可用，无来源位置无法凭空产生。
//!
//! `Ref` 只提供复制与业务读取；把「逻辑位置句柄 → Ref」的构造保留为 crate 内部构建
//! 路径（`pub(crate)`），外部既不能取得句柄，也不能直接调用构造入口。句柄类型本身
//! 也没有 `Default`，所以两条路都不成立。
//!
//! 期望主诊断：E0624（`Ref::from_handle` 是私有 associated function）；同一行的实参
//! `Default::default()` 另报 E0277（`PositionHandle: Default` 未满足）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

fn main() {
    // 期望主诊断：E0624——内部构造入口不是公开 API。
    let _ = Ref::<Amount>::from_handle(Default::default());
}
