//! V22-T03 承接 WE-T03 C22 负例：公开面不能制造**无来源的 `Ref`**。
//!
//! `Ref<T>` 是框架发出的强类型逻辑依赖身份，其构造入口（`Ref::from_handle`）是 crate 内
//! 可见的 associated function。外部消费者因此无法伪造位置：`choose` 的 selector 与分支的
//! 数据输入都只能来自真实执行（对比 `v22_t03_from_we_t03_c06_foreign_ref_rejected.rs`
//! 的构建期拒绝）。本夹具不经过 Root 定义闭包，因此不需要 T03 契约迁移。
//!
//! 期望主诊断：E0624（associated function `from_handle` 为私有）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

fn probe_ref() -> Ref<Amount> {
    // 期望主诊断：E0624——`Ref::from_handle` 不是公开入口。
    Ref::from_handle(todo!())
}

fn main() {
    let _ = probe_ref;
    let _ = Amount(1);
}
