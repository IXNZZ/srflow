//! T06 新增拒绝样本 ④：嵌套 tuple 状态不在支持范围内，编译拒绝。
//!
//! SPEC §8.5：状态形状只实现**flat** tuple（2～16 槽）。`((Ref<A>, Ref<A>), (Ref<A>, Ref<A>))`
//! 既不是单 Ref 也不是 flat tuple，绑定失败（E0277）。
//!
//! 期望主诊断：E0277。

use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct State(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, a: Ref<State>, b: Ref<State>) {
    let _ = flow.iter(((a, b), (a, b)), 5, |_round| {});
}

fn main() {
    let _ = State(1);
}
