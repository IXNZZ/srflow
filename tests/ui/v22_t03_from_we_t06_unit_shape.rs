//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_unit_shape.rs`）：unit signature 不可表达。
//!
//! 原目的（X24／C13／C15）：`Ref<T: Data>` 的 `Data` 边界排除 unit：unit 不分配业务槽，
//! 也不产生 `Ref<()>`（SPEC §3.2／§5.3）。以函数签名固定该拒绝位置，不构造任何值。
//!
//! 迁移手法：新的 Root 契约仍然只以 owned business Data 为槽载体，`Ref<()>` 的存在性判定
//! 完全落在 `Data` 边界上，与 Root/Node 语法无关。本夹具**逐字保留**，只更新文件头的
//! 迁移说明。
//!
//! 归因：**rustc 编译拒绝**。
//!
//! 期望主诊断：E0277（`(): Data` 未满足）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

fn takes_unit(_: Ref<()>) {}

fn main() {
    let _ = takes_unit;
    let _ = Amount(1);
}
