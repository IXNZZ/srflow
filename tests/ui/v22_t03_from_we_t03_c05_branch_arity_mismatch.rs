//! V22-T03 承接 WE-T03 C05 负例：分支间 **arity 不一致**必须在编译期拒绝。
//!
//! 所有 case／otherwise 的 OutputShape 及各位置数必须一致。本夹具的 `case` 分支返回
//! 2 元 flat tuple，`otherwise` 分支返回 3 元 flat tuple；共同输出泛型 `O` 只能取其一，
//! 另一处由编译器拒绝（SPEC §9）。
//!
//! T03 契约迁移：旧两参 `flow.then(node, refs)` 改为 `flow.then_with_output(node, refs)`；
//! Root 闭包固定返回 `()`，业务输出由 `Flow::output_with` 声明（owned `Out`
//! 以显式泛型给出）。分支闭包仍返回原 RefShape，不递归改写。
//!
//! 期望主诊断：E0308（分支闭包返回 tuple arity 与已确定的共同输出不一致）。

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
    let _ = drive(runtime.execute::<_, (Amount, Amount), _>(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then_with_output(route_of, amount);
            let picked = flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| {
                    (branch.then_with_output(id_amount, amount), branch.then_with_output(id_amount, amount))
                });
                choice.otherwise(|branch| {
                    (branch.then_with_output(id_amount, amount), branch.then_with_output(id_amount, amount), branch.then_with_output(id_amount, amount)) // tuple arity: 3 vs 2
                });
            });
            flow.output_with(picked);
        },
        Amount(1),
    ));
}
