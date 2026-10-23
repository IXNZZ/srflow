//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_ref_not_constructible.rs`）：`Ref` 不可从外部伪造。
//!
//! 原目的（X24／X06／C13／C15）：`Ref<T>` 是框架发出的强类型逻辑依赖身份；它的构造入口
//! `Ref::from_handle` 是 crate 内可见的 associated function，外部消费者无法命名，也就无法
//! 伪造位置来源（SPEC §6）。这是「跨执行 Ref」唯一可能的人为来源，且只有这一条入口。
//!
//! 迁移手法：T03 公开面新增 `provide_output`／`ref_of` 等**受管**取 Ref 入口，但都不接受
//! 外部句柄；`Ref::from_handle` 保持内部。本夹具**逐字保留**，只更新文件头的迁移说明。
//!
//! 归因：**rustc 编译拒绝**（私有 associated function 不可调用）。
//!
//! 期望主诊断：E0624（associated function `from_handle` 为私有）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

fn fabricate() -> Ref<Amount> {
    Ref::from_handle(todo!())
}

fn main() {
    let _ = fabricate;
    let _ = Amount(1);
}
