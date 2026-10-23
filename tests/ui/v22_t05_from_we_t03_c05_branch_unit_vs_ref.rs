//! T05 迁移自 `v22_t03_from_we_t03_c05_branch_unit_vs_ref.rs`（承接 WE-T03 C05 负例）：分支间
//! **unit 与非 unit 不一致**必须在构建期拒绝。
//!
//! T05 阶段变化（同 `v22_t04_from_we_t04_e02_retry_output_shape_mismatch.rs`）：新 unit Body 下
//! `case`／`otherwise` 的输出由分支 Body 内的 `output`／`output_with` 声明给出，分支闭包都返回
//! `()`。因此“case 声明 unit、otherwise 声明 `Ref<Amount>`”不再由 rustc 以 E0308 拒绝，而由
//! Definition 收口时的共同 Shape 核对拒绝（`RunError::Definition`），业务 Node 零调用。
//!
//! T05 迁移点：`flow.choose(selector, …)` → `flow.choose_with_output::<Route, Ref<Amount>, _>(selector, …)`；
//! 分支闭包改为声明式 unit Body。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `t03-c05-branch-unit-vs-ref-reject-ok`。

use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

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

#[derive(Data, PartialEq)]
struct Route(u32);

async fn route_of(query: Query<&Amount>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 2))
}

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

async fn unit_out(query: Query<&Amount>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

fn main() {
    let runtime = Runtime::new();
    let error = drive(runtime.execute::<_, Amount, _>(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then_with_output(route_of, amount);
            // 期望：case 分支声明 unit，otherwise 声明 Ref<Amount>——共同 Shape 冲突。
            flow.choose_with(selector, |choice| {
                choice.case(Route(0), |branch| {
                    branch.then_with_output(unit_out, amount);
                });
                choice.otherwise(|branch| {
                    let value = branch.then_with_output(id_amount, amount);
                    branch.output_with(value);
                });
            });
            let out = flow.then_with_output(id_amount, amount);
            flow.output_with(out);
        },
        Amount(1),
    ))
    .err()
    .expect("分支 unit／非 unit 混用应在 Definition 构建期被拒绝");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );

    println!("t03 c05 branch unit vs ref rejected: {error}");
    println!("t03-c05-branch-unit-vs-ref-reject-ok");
}
