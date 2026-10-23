//! V22-T03 承接 WE-T03 C22 负例：不存在 `choose` macro。
//!
//! SPEC §9 明确不引入 choose macro：唯一入口是 `Flow::choose` 方法。本夹具同时固定
//! 「不存在可导入的宏名」与「不存在可调用的宏」两种拒绝。本夹具不经过 Root 定义闭包，
//! 因此不需要 T03 契约迁移。
//!
//! 期望主诊断：E0432（`srflow::choose_macro` 不可解析的导入）；附加诊断：E0433
//! （`srflow::choose` 宏不存在）。

use srflow::choose_macro;
use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

fn main() {
    let route = Route(0);
    let _: Ref<Amount> = srflow::choose!(route, { _ => todo!() });
    let _ = choose_macro;
}
