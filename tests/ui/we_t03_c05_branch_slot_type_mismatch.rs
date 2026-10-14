//! WE-T03 C05 负例：分支间 **逐槽类型不一致**必须在编译期拒绝。
//!
//! 两个分支都返回 2 元 flat tuple，但第 1 槽类型不同（`Ref<Amount>` vs `Ref<Other>`）。
//! 共同输出泛型 `O` 要求逐槽类型一致，编译器只能接受其一（SPEC §9）。
//!
//! 期望主诊断：E0308（元组槽位类型不一致）。

use futures::executor::block_on;
use srflow::{BodyError, Data, Node, Query, Ref, Runtime};

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
    let _ = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then(route_of, amount);
            flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| {
                    (branch.then(id_amount, amount), branch.then(ToOther, amount))
                });
                choice.otherwise(|branch| {
                    (branch.then(id_amount, amount), branch.then(id_amount, amount))
                });
            })
        },
        Amount(1),
    ));
}
