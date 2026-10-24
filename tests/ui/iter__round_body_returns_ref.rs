//! T06 新增拒绝样本 ②：Round Body 返回 Ref（旧写法“返回值即 Next”）在新 unit Body 下编译拒绝。
//!
//! 新契约 Round Body 是 `FnOnce(&mut Flow<'n>) -> ()`：Next 必须经 `round.next`／`round.next_with`
//! 显式声明，闭包尾表达式返回 `Ref<State>` 会得到 E0308（expected `()`）。
//!
//! 期望主诊断：E0308。
use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct State(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, initial: Ref<State>) {
    let _ = flow.iter(initial, 5, |round| {
        let current = round.current::<State>();
        current // 旧写法：闭包返回值即 Next 声明
    });
}

fn main() {
    let _ = State(1);
}
