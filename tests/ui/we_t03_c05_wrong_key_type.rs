//! WE-T03 C05 负例：selector 与 case 键类型不一致必须在编译期拒绝。
//!
//! `choose(selector: Ref<K>, …)` 固定键类型 `K`。本夹具的 selector 是 `Ref<Amount>`，
//! 而 `case` 传的是 `Route`；`Amount` 已实现 `PartialEq`，因此这里唯一的目标诊断是
//! 键类型不匹配，而不是缺少 `PartialEq`（对比 `we_t03_c05_key_without_partialeq.rs`）。
//!
//! 期望主诊断：E0308（`case` 的键实参类型与 `K` 不一致）。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data, PartialEq)]
struct Amount(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    let _ = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| {
            flow.choose(amount, |choice| {
                choice.case(Route(0), |branch| branch.then(id_amount, amount));
                choice.otherwise(|branch| branch.then(id_amount, amount));
            })
        },
        Amount(1),
    ));
}
