//! V22-T03 承接 WE-T03 C05 负例：键类型缺少 `PartialEq` 必须在编译期拒绝。
//!
//! `K` 只要求 `Data + PartialEq`（不要求 `Eq`/`Hash`/`Ord`/`Clone`/`Copy`/`Debug`），
//! 但 `PartialEq` 本身是必需。本夹具的 `Opaque` 只有 `#[derive(Data)]`，selector 是
//! 类型正确的 `Ref<Opaque>`；`choose` 调用点因 `Opaque: PartialEq` 未满足而拒绝。
//!
//! T03 契约迁移：旧两参 `flow.then(node, refs)` 改为 `flow.then_with_output(node, refs)`；
//! Root 闭包固定返回 `()`，业务输出由 `Flow::output_with` 声明（owned `Out = Amount`
//! 以显式泛型给出）。
//!
//! 期望主诊断：E0277（`Opaque: PartialEq` 未满足），落在 `flow.choose(selector, …)` 行；
//! 因 `Choice<K>` 的约束未满足，`case`／`otherwise` 追加 E0599。

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

/// 键类型：是 `Data` 但没有 `PartialEq`。
#[derive(Data)]
struct Opaque(u32);

async fn opaque_of(query: Query<&Amount>) -> Result<Opaque, BodyError> {
    Ok(Opaque(query.get().0))
}

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    let _ = drive(runtime.execute::<_, Amount, _>(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then_with_output(opaque_of, amount);
            // 期望主诊断：E0277——Opaque 未实现 PartialEq。
            let picked = flow.choose(selector, |choice| {
                choice.case(Opaque(0), |branch| branch.then_with_output(id_amount, amount));
                choice.otherwise(|branch| branch.then_with_output(id_amount, amount));
            });
            flow.output_with(picked);
        },
        Amount(1),
    ));
}
