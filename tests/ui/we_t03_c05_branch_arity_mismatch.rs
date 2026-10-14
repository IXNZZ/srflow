//! WE-T03 C05 负例：分支间 **arity 不一致**必须在编译期拒绝。
//!
//! 所有 case／otherwise 的 OutputShape 及各位置数必须一致。本夹具的 `case` 分支返回
//! 2 元 flat tuple，`otherwise` 分支返回 3 元 flat tuple；共同输出泛型 `O` 只能取其一，
//! 另一处由编译器拒绝（SPEC §9）。
//!
//! 期望主诊断：E0308（分支闭包返回 tuple arity 与已确定的共同输出不一致）。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

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
    let _ = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then(route_of, amount);
            flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| {
                    (branch.then(id_amount, amount), branch.then(id_amount, amount))
                });
                choice.otherwise(|branch| {
                    (
                        branch.then(id_amount, amount),
                        branch.then(id_amount, amount),
                        branch.then(id_amount, amount),
                    )
                });
            })
        },
        Amount(1),
    ));
}
