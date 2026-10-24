//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_x03_nested_ref_input.rs`）：nested Ref tuple 作为 Root Input。
//!
//! 原目的（X03／X24／C13／C15）：SPEC §3.2 Root Input 支持 `()`、单 Data 值与 2～16 个
//! Data 值的 **flat** tuple；native tuple 也不是默认 Data 叶子（SPEC §3.3），因此
//! `((Amount, Amount), Amount)` 这类嵌套输入形状不被接受。本夹具**只证明该形状不被接受**，
//! 主诊断落在 `execute` 调用点（真实调用点）。
//!
//! 迁移手法：Root 闭包改为固定返回 `()`，并补上显式 owned `Out = ()` 泛型；嵌套输入实参
//! 原样保留，Shape 拒绝范围不变（不是「缺来源」「unsupported」等更早的无关拒绝）。
//!
//! 期望主诊断：E0277（嵌套元组未实现 Root 输入形状）。
use srflow::{Data, Runtime};

#[derive(Data)]
struct Amount(u32);

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow, input| {
            let _ = (flow, input);
        },
        ((Amount(0), Amount(1)), Amount(2)),
    );
}
