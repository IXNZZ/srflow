//! V22-T03 承接 WE-T03 C22 负例：不存在 predicate case 或新的 Choose 执行协议。
//!
//! SPEC §9 明确：**不存在 predicate case 或 choose macro**。复杂路由判断必须由上游 Node
//! 产生 selector。公开面因此没有 `case_if`／`when`／`choose_if` 这类谓词入口，本夹具在
//! 真实的 `Flow`／`Choice` 上逐一调用它们以固定“方法不存在”的拒绝。
//!
//! T03 契约迁移：旧两参 `flow.then(node, refs)` 改为 `flow.then_with_output(node, refs)`；
//! Root 闭包固定返回 `()`，业务输出由 `Flow::output_with` 声明（owned `Out = Amount`
//! 以显式泛型给出）。
//!
//! 期望主诊断：E0599（`Choice` 上无 `case_if` 方法）。

use srflow::{BodyError, Data, Query, Ref, Runtime};

/// T03 夹具自带的极小 executor：保持与正例相同的装配形态；本项在编译期被拒绝。
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

fn main() {
    let runtime = Runtime::new();
    let _ = drive(runtime.execute::<_, Amount, _>(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then_with_output(route_of, amount);
            let picked = flow.choose(selector, |choice| {
                // 期望主诊断：E0599——不存在 predicate case。
                choice.case_if(|route: &Route| route.0 == 0, |branch| {
                    branch.then_with_output(id_amount, amount)
                });
                // 附加诊断：E0599——不存在按业务值谓词的 `when`。
                choice.when(|amount: &Amount| amount.0 > 0, |branch| {
                    branch.then_with_output(id_amount, amount)
                });
                choice.otherwise(|branch| branch.then_with_output(id_amount, amount));
            });
            flow.output_with(picked);
        },
        Amount(1),
    ));
}
