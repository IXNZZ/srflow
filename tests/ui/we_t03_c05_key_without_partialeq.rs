//! WE-T03 C05 负例：键类型缺少 `PartialEq` 必须在编译期拒绝。
//!
//! `K` 只要求 `Data + PartialEq`（不要求 `Eq`/`Hash`/`Ord`/`Clone`/`Copy`/`Debug`），
//! 但 `PartialEq` 本身是必需。本夹具的 `Opaque` 只有 `#[derive(Data)]`，selector 是
//! 类型正确的 `Ref<Opaque>`；`choose` 调用点因 `Opaque: PartialEq` 未满足而拒绝。
//!
//! 期望主诊断：E0277（`Opaque: PartialEq` 未满足），落在 `flow.choose(selector, …)` 行。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

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
    let _ = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| {
            let selector = flow.then(opaque_of, amount);
            flow.choose(selector, |choice| {
                choice.case(Opaque(0), |branch| branch.then(id_amount, amount));
                choice.otherwise(|branch| branch.then(id_amount, amount));
            })
        },
        Amount(1),
    ));
}
