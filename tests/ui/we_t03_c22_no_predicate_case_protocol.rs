//! WE-T03 C22 负例：不存在 predicate case 或新的 Choose 执行协议。
//!
//! SPEC §9 明确：**不存在 predicate case 或 choose macro**。复杂路由判断必须由上游 Node
//! 产生 selector。公开面因此没有 `case_if`／`when`／`choose_if` 这类谓词入口，本夹具在
//! 真实的 `Flow`／`Choice` 上逐一调用它们以固定“方法不存在”的拒绝。
//!
//! 期望主诊断：E0599（`Choice` 上无 `case_if` 方法）。

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
                // 期望主诊断：E0599——不存在 predicate case。
                choice.case_if(|route: &Route| route.0 == 0, |branch| {
                    branch.then(id_amount, amount)
                });
                // 附加诊断：E0599——不存在按业务值谓词的 `when`。
                choice.when(|amount: &Amount| amount.0 > 0, |branch| {
                    branch.then(id_amount, amount)
                });
                choice.otherwise(|branch| branch.then(id_amount, amount));
            })
        },
        Amount(1),
    ));
}
