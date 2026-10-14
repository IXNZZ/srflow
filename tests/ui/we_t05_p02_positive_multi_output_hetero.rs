//! WE-T05 P02 正例：多输出 Each 的逐槽 Vec（异构槽 + 同类型不同逻辑位置）。
//!
//! 覆盖任务书 §1 输出映射表、§8 的 P02／P10（公开部分）与 SPEC §3.2 的“方法名称不按 arity
//! 分类”：同一个 `each` 方法直接承载 3 个 flat 输出槽，不出现 `each3` 之类的 arity 命名。
//!
//! body 返回 `(Ref<Score>, Ref<Score>, Ref<Report>)`：
//! - 槽 1／槽 2 是**同一 Data 类型**（`Score`）的**不同逻辑位置**（`base_score`／`bumped_score`），
//!   因此必须各自形成独立的 `Ref<Vec<Score>>`，不能按 `TypeId` 合并；
//! - 槽 3 是**异构**类型（`Report`）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Report(u32);

/// 槽 1：item -> Score。
async fn base_score(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0 * 10))
}

/// 槽 2：同类型（Score）但不同逻辑位置，依赖槽 1 的结果。
async fn bumped_score(query: Query<(&Item, &Score)>) -> Result<Score, BodyError> {
    let (item, score) = query.get();
    Ok(Score(score.0 + item.0))
}

/// 槽 3：异构类型，依赖槽 1 的结果。
async fn report_of(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    let (item, score) = query.get();
    Ok(Report(item.0 * 100 + score.0))
}

fn main() {
    let runtime = Runtime::new();

    let (scores, bumped, reports) = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&base_score, item);
                let bumped = each.then(&bumped_score, (item, score));
                let report = each.then(&report_of, (item, score));
                (score, bumped, report)
            })
        },
        vec![Item(1), Item(2)],
    ))
    .expect("3-slot each runs");

    let numbers = |values: &[Score]| values.iter().map(|value| value.0).collect::<Vec<_>>();
    assert_eq!(numbers(&scores), vec![10, 20], "slot 1 values and order");
    assert_eq!(numbers(&bumped), vec![11, 22], "slot 2 values and order");
    assert_eq!(
        reports.iter().map(|value| value.0).collect::<Vec<_>>(),
        vec![110, 220],
        "heterogeneous slot 3 values and order"
    );
    // 同类型两个槽各自独立收集（不按 TypeId 合并成一份 Vec）。
    assert_ne!(numbers(&scores), numbers(&bumped), "same-type slots stay distinct");

    println!("we_t05-positive-ok");
}
