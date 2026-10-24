//! T06 新增拒绝样本 ③：unit 状态（`()`）没有 `RefShape`／`StateShape` impl，编译拒绝。
//!
//! SPEC §8.5：状态形状支持单 Ref 与 2～16 个 flat Ref tuple，**不支持 unit**。把 `()` 作为 initial
//! 传入 Iter 会在绑定处失败（E0277）。
//!
//! 期望主诊断：E0277。
use srflow::Flow;

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>) {
    let _ = flow.iter((), 5, |_round| {});
}

fn main() {}
