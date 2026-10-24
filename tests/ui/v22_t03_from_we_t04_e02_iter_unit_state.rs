//! T03 迁移自 `we_t04_e02_iter_unit_state.rs`（WE-T04 E02 负例，编译期拒绝）：Iter 的 State
//! **不允许 unit**（`()`）。
//!
//! `Flow::iter` 的 State 参数要求 `S: RefShape + StateShape`；`StateShape` 是 crate 内部协议，
//! 只为单个 `Ref` 与 2～16 元 flat `Ref` tuple 实现，`()` 刻意**不**实现它（任务书 §1：
//! “Iter 支持单 Ref、2～16 flat tuple State …… 不支持 unit State”）。因此外部消费者写
//! `flow.iter((), …)` 应在类型检查阶段被拒绝，而不是运行期。
//!
//! 期望主诊断：E0277（trait bound `(): StateShape` 未满足）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Amount(u32);

async fn produce(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, amount: Ref<Amount>) {
    // 期望主诊断：E0277——`()` 不实现 Iter 的 StateShape。
    flow.iter((), 3, |round| {
        let _ = round.then_with_output(&produce, amount);
        round.next_with(());
    });
}

fn main() {
    let _ = Amount(1);
}
