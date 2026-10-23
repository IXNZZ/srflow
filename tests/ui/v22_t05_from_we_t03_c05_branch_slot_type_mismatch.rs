//! T05 迁移自 `v22_t03_from_we_t03_c05_branch_slot_type_mismatch.rs`（承接 WE-T03 C05 负例）：分支间
//! **逐槽类型不一致**必须在构建期拒绝。
//!
//! T05 阶段变化：新 unit Body 下分支输出由声明给出，两支同为 2 元 tuple 但第 1 槽 `Ref<Other>` vs
//! `Ref<Amount>` 的逐槽不一致由 Definition 收口核对拒绝（`RunError::Definition`），业务 Node 零调用。
//!
//! T05 迁移点：`flow.choose(selector, …)` → `flow.choose_with_output::<Route, (Ref<Amount>, Ref<Other>), _>(selector, …)`；
//! 分支闭包改为声明式 unit Body。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `t03-c05-branch-slot-type-mismatch-reject-ok`。

use srflow::{BodyError, Data, Node, Query, Ref, RunError, Runtime};

/// T05 夹具自带的极小 executor：本版引擎的单次 Root 执行在有限次 poll 内完成。
fn drive<F: core::future::Future>(future: F) -> F::Output {
    use core::task::{Context, Poll, Waker};
    let mut future = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
            }
        }
    }
}

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Other(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

async fn route_of(query: Query<&Amount>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 2))
}

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

struct ToOther;

impl Node for ToOther {
    type Input = Amount;
    type Output = Other;

    async fn run(&self, query: Query<&Amount>) -> Result<Other, BodyError> {
        Ok(Other(query.get().0))
    }
}

fn main() {
    let runtime = Runtime::new();
    let error = drive(runtime.execute::<_, Amount, _>(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then_with_output(route_of, amount);
            // 期望：两支同为 2 元 tuple，但第 1 槽类型不一致（Ref<Other> vs Ref<Amount>）。
            flow.choose_with(selector, |choice| {
                choice.case(Route(0), |branch| {
                    let a = branch.then_with_output(id_amount, amount);
                    let b = branch.then_with_output(ToOther, amount);
                    branch.output_with((a, b));
                });
                choice.otherwise(|branch| {
                    let a = branch.then_with_output(id_amount, amount);
                    let b = branch.then_with_output(id_amount, amount);
                    branch.output_with((a, b)); // Ref<Amount> in slot 1
                });
            });
            let out = flow.then_with_output(id_amount, amount);
            flow.output_with(out);
        },
        Amount(1),
    ))
    .err()
    .expect("分支逐槽类型不一致应在 Definition 构建期被拒绝");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );

    println!("t03 c05 branch slot type mismatch rejected: {error}");
    println!("t03-c05-branch-slot-type-mismatch-reject-ok");
}
