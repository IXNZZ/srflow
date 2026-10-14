//! WE-T03 C05 负例：分支间 **unit 与非 unit 不一致**必须在编译期拒绝。
//!
//! 所有 case／otherwise 必须返回同一个输出 Shape `O`。本夹具的 `case` 分支返回 `()`
//! （unit Node），`otherwise` 分支返回 `Ref<Amount>`；`O` 由共同输出泛型固定，两处不一致
//! 只能由编译器拒绝（SPEC §9；任务书 §3.3 不以 Any 下转型补救）。
//!
//! 期望主诊断：E0308（分支闭包返回类型与已确定的共同输出不一致）。

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

async fn unit_out(query: Query<&Amount>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

fn main() {
    let runtime = Runtime::new();
    let _ = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then(route_of, amount);
            flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| {
                    branch.then(unit_out, amount);
                });
                choice.otherwise(|branch| branch.then(id_amount, amount));
            })
        },
        Amount(1),
    ));
}
