//! T06 新增拒绝样本 ⑥：`round.next::<()>()` —— unit 状态本来就不支持，编译拒绝。
//!
//! SPEC §8.5：`next::<T>()` 要求 `T::Refs: StateShape`，`()` 既不满足 `RootInput` 也不满足
//! `StateShape`；因此 unit Next 在编译期被拒（E0277），而不是运行期。
//!
//! 期望主诊断：E0277。
use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct State(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, initial: Ref<State>) {
    let _ = flow.iter(initial, 5, |round| {
        round.next::<()>();
    });
}

fn main() {
    let _ = State(1);
}
