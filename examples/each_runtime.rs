//! Each 运行期示例：多输出聚合、unit／空集合、业务 Failure 与 Control 传播。
//!
//! 运行：`cargo run --offline --example each_runtime`
//!
//! 覆盖四条真实路径（SPEC §8；任务书 §8 的 P39“正常多输出、空／unit、失败／Control 示例”）：
//! - `Flow::each`：item body 产生 `(Ref<Score>, Ref<Report>)`，逐槽形成对应的
//!   `Ref<Vec<Score>>`／`Ref<Vec<Report>>`，元素按输入顺序；
//! - unit：body 输出 `()`，每项仍真实执行，但 Each 不产生假业务槽；
//! - 空集合：不调用 body（业务节点零执行），unit 得到 `()`、多输出得到每槽空 Vec；
//! - 业务 Failure：item body 内节点返回 `BodyError::fail(...)` → 停止剩余 item，Root 返回
//!   `RunError::Body`，动态原因与 source chain 保留；
//! - Control：item body 内节点返回 `BodyError::iter_break()` 而 Root 没有匹配边界 → 停止剩余
//!   item，Root 返回 `RunError::UnhandledControl`，**不产生部分正常集合**（SPEC §8／§12）。

use std::cell::Cell;
use std::error::Error;
use std::fmt;

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

thread_local! {
    /// item body 内业务节点的执行计数：用于观察“停止剩余 item”的真实行为。
    static BODY_CALLS: Cell<u32> = const { Cell::new(0) };
}

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Report(u32);

/// 明确失败理由：携带动态 Error 对象，不被字符串化重建。
#[derive(Debug)]
struct TooSmall {
    value: u32,
}

impl fmt::Display for TooSmall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "item {} is too small", self.value)
    }
}

impl Error for TooSmall {}

fn body_calls() -> u32 {
    BODY_CALLS.with(|counter| counter.get())
}

fn reset_calls() {
    BODY_CALLS.with(|counter| counter.set(0));
}

/// 评分节点：每个 item 产生一个新的 owned `Score`。
async fn score(query: Query<&Item>) -> Result<Score, BodyError> {
    BODY_CALLS.with(|counter| counter.set(counter.get() + 1));
    Ok(Score(query.get().0 * 10))
}

/// 汇总节点：依赖 item 与 score，产生异构 `Report`。
async fn report(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    BODY_CALLS.with(|counter| counter.set(counter.get() + 1));
    let (item, score) = query.get();
    Ok(Report(score.0 + item.0))
}

/// unit 节点：只做业务（计数），不产生业务槽。
async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    let item = query.get();
    assert!(item.0 > 0, "item value stays readable");
    BODY_CALLS.with(|counter| counter.set(counter.get() + 1));
    Ok(())
}

/// 低于阈值即普通 Failure：用于失败传播路径。
async fn score_checked(query: Query<&Item>) -> Result<Score, BodyError> {
    BODY_CALLS.with(|counter| counter.set(counter.get() + 1));
    let value = query.get().0;
    if value < 2 {
        Err(BodyError::fail(TooSmall { value }))
    } else {
        Ok(Score(value * 10))
    }
}

/// 到达第二项即返回 `IterBreak`：Root 没有匹配的 Iter 边界，形成 UnhandledControl。
async fn score_until_control(query: Query<&Item>) -> Result<Score, BodyError> {
    BODY_CALLS.with(|counter| counter.set(counter.get() + 1));
    let value = query.get().0;
    if value >= 2 {
        Err(BodyError::iter_break())
    } else {
        Ok(Score(value * 10))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new();

    // 1) 正常多输出：逐槽 Vec，元素按输入顺序。
    reset_calls();
    let (scores, reports) = block_on(runtime.execute::<_, (Vec<Score>, Vec<Report>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| {
                let score = each.then_with_output(&score, item);
                let report = each.then_with_output(&report, (item, score));
                (score, report)
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ))?;
    assert_eq!(
        scores.iter().map(|score| score.0).collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(
        reports.iter().map(|report| report.0).collect::<Vec<_>>(),
        vec![11, 22, 33]
    );
    assert_eq!(body_calls(), 6, "3 items x 2 body nodes");
    println!(
        "each multi-output: scores={:?} reports={:?}",
        scores.iter().map(|score| score.0).collect::<Vec<_>>(),
        reports.iter().map(|report| report.0).collect::<Vec<_>>()
    );

    // 2) unit：空集合不调用 body（业务节点零执行），输出为 ()。
    reset_calls();
    block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let () = flow.each(items, |item, each| {
                each.then_with_output(&audit, item);
            });
            flow.output_with(());
        },
        Vec::<Item>::new(),
    ))?;
    assert_eq!(body_calls(), 0, "an empty collection runs no body step");
    println!("each unit empty: output=() body_calls={}", body_calls());

    // 2b) unit：非空集合每项真实执行，Each 不产生假业务槽。
    reset_calls();
    block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let () = flow.each(items, |item, each| {
                each.then_with_output(&audit, item);
            });
            flow.output_with(());
        },
        vec![Item(1), Item(2)],
    ))?;
    assert_eq!(body_calls(), 2, "unit body runs once per item");
    println!("each unit non-empty: output=() body_calls={}", body_calls());

    // 3) 业务 Failure：第二项失败后不再执行第三项，Root 返回 RunError::Body。
    reset_calls();
    let error = match block_on(runtime.execute::<_, Vec<Score>, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| {
                each.then_with_output(&score_checked, item)
            });
            flow.output_with(out_refs);
        },
        vec![Item(2), Item(1), Item(3)],
    )) {
        Ok(_) => panic!("an item failure must escape Each"),
        Err(error) => error,
    };
    match &error {
        RunError::Body(failure) => {
            let reason = failure
                .source()
                .expect("the dynamic failure reason stays reachable");
            assert!(reason.to_string().contains("too small"));
            println!("each failure: {failure} (reason={reason})");
        }
        other => panic!("expected RunError::Body, got {other}"),
    }
    assert_eq!(
        body_calls(),
        2,
        "the third item did not run after the failure"
    );

    // 4) Control：无匹配边界的 IterBreak 停止剩余 item，且不产生部分正常集合。
    reset_calls();
    let error = match block_on(runtime.execute::<_, Vec<Score>, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| {
                each.then_with_output(&score_until_control, item)
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    )) {
        Ok(_) => panic!("an unmatched control signal must escape the Root"),
        Err(error) => error,
    };
    match &error {
        RunError::UnhandledControl(signal) => println!("each control: {error} (signal={signal})"),
        other => panic!("expected RunError::UnhandledControl, got {other}"),
    }
    assert_eq!(
        body_calls(),
        2,
        "the third item did not run after the control signal"
    );

    Ok(())
}
