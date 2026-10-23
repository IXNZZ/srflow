//! T03 迁移自 `we_t04_e02_iter_state_shape_mismatch.rs`（WE-T04 E02 负例，编译期拒绝）：
//! Iter 的 next 与 initial **Shape 不一致**。
//!
//! `Flow::iter<S, F>` 要求 initial 与 round 闭包的声明输出**同一个** `S`（任务书 §3.3：
//! “Body 返回同 Shape 的 next；各槽类型与顺序一致”）。initial 是 2 元 flat tuple，而 round
//! 闭包返回 3 元 tuple，类型系统必须拒绝：这是编译期诊断，不是运行期来源校验。
//!
//! 期望主诊断：E0308（round 闭包返回类型与 initial 驱动的 `S` 不匹配）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct A(u32);
#[derive(Data)]
struct B(u32);
#[derive(Data)]
struct C(u32);

async fn step(query: Query<&A>) -> Result<A, BodyError> {
    Ok(A(query.get().0 + 1))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, a: Ref<A>, b: Ref<B>, c: Ref<C>) {
    let _ = flow.iter(
        (a, b), // initial：2 元 State
        3,
        |(current_a, current_b), round| {
            let _ = round.then_with_output(&step, current_a);
            // 期望主诊断：E0308——round 声明 3 元 next，与 2 元 initial 不一致。
            (current_a, current_b, c)
        },
    );
}

fn main() {
    let _ = (A(1), B(2), C(3));
}
