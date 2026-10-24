//! T06 新增拒绝样本 ①：旧双参 `flow.iter(initial, N, |current, round| …)` 在新契约下编译拒绝。
//!
//! 新契约 `Flow::iter<S, F>(&mut self, initial: S, max_iterations: usize, definition: F)`，其中
//! `F: FnOnce(&mut Flow<'n>)`：Round Body 只接收一个 `&mut Flow`（Current 经
//! `round.current::<T>()` 取得、Next 经 `round.next`／`round.next_with` 声明）。旧双参闭包会被
//! rustc 以 E0593（闭包参数个数不符）拒绝。
//!
//! 期望主诊断：E0593。
use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct State(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, initial: Ref<State>) {
    let _ = flow.iter(initial, 5, |_current, _round| {});
}

fn main() {
    let _ = State(1);
}
