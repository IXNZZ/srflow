//! T06 新增拒绝样本 ⑤：17 槽 flat tuple 状态超出实现上限，编译拒绝。
//!
//! SPEC §8.5：状态形状实现到 16 槽。17 槽即使全为同一类型也不满足 `StateShape`／`RefShape`
//! 绑定（E0277）。
//!
//! 期望主诊断：E0277。
use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct State(u32);

#[allow(clippy::type_complexity)]
#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(
    flow: &mut Flow<'n>,
    slots: (
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
        Ref<State>,
    ),
) {
    let _ = flow.iter(slots, 5, |_round| {});
}

fn main() {
    let _ = State(1);
}
