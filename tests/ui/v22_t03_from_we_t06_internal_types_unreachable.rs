//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_internal_types_unreachable.rs`）：内部许可／构造类型不可命名。
//!
//! 原目的（X24／X06／C13／C15）：`ItemGroupPermit`／`CollectionRoot`／`Permit`／`Target` 等
//! 内部许可与目标类型只在 crate 内可见，不作为 crate 根导出：外部消费者连**命名**都做不到
//! （SPEC §2「使用者不操作内部 Signature／Import／Export／Collector／Scope／CallSite 或
//! Runtime Target」）。
//!
//! 迁移手法：T03 只新增了公开 Node/Root 入口与 `AutoCallable`／`AutoFunction` 等隐藏桥，
//! 原 Core 许可／Target 的可见性不变（仍为 `pub(crate)`、无根导出）。本夹具**逐字保留**，
//! 只更新文件头的迁移说明。
//!
//! 归因：**rustc 编译拒绝**（不在 crate 根，导入无法解析）。该夹具不使用任何内部 `#[path]`
//! 装配，因此「名字不可达」就是目标语义本身，不是装配失败。
//!
//! 期望主诊断：E0432（crate 根下不存在这些名字）。

use srflow::{CollectionRoot, ItemGroupPermit, Permit, Target};

fn main() {
    let _ = std::mem::size_of::<Option<CollectionRoot>>();
    let _ = std::mem::size_of::<Option<ItemGroupPermit>>();
    let _ = std::mem::size_of::<Option<Permit>>();
    let _ = std::mem::size_of::<Option<Target>>();
}
