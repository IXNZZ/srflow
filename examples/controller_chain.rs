//! 控制器组合示例：Each 收集 → Choose 路由（跨两类控制器的真实调用链）。
//!
//! 运行：`cargo run --offline --example controller_chain`
//!
//! 同时演示：Iter 多轮推进与业务值表达 Continue／Break、以及**定义期拒绝**（非法 Choose 配置）
//! 可被调用方在 `Runtime::execute` 处以 [`RunError::Definition`] 观察。
//!
//! 历史对照：旧 `examples/controller_chain.rs` 使用 `srflow::legacy` 的 `EachBuilder`／
//! `MatchBuilder`／`LoopBuilder`／完成态 `Flow`（SPEC §1 明确其不适用于新模型）。本文件把同一
//! 教学意图（Each→路由跨控制器链、Iter 多轮、调用方可见的构建期拒绝）改写为新冻结面：
//! Each 由 [`Flow::each`] 表达、路由由 [`Flow::choose`]（值匹配 [`Choice`]）表达、多轮由
//! [`Flow::iter`] 表达；旧 “普通 unit 函数在构建期被拒” 的形态在新契约下已不适用（unit 输出
//! 是合法 Shape），故改以 “空 Choose 配置” 这一新契约定义期拒绝作为对应演示。

use futures::executor::block_on;

use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Span(u32);

#[derive(Data)]
struct Length(u32);

#[derive(Data, Debug, PartialEq, Eq)]
struct Counted(u32);

/// Choose 的值匹配键：只声明 `Data` 并手写 `PartialEq`（最小约束）。
#[derive(Data)]
struct Route {
    code: u8,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}

#[derive(Data, Debug, PartialEq, Eq)]
struct Budget {
    spent: u32,
    rounds: u32,
}

/// Each item body：`Span → Length`。
async fn length(query: Query<&Span>) -> Result<Length, BodyError> {
    Ok(Length(query.get().0 + 1))
}

/// Choose case 分支：把 Each 收集的 `Vec<Length>` 求和。
async fn total(query: Query<&Vec<Length>>) -> Result<Counted, BodyError> {
    Ok(Counted(query.get().iter().map(|item| item.0).sum()))
}

/// Choose otherwise 分支：改为计数。
async fn count(query: Query<&Vec<Length>>) -> Result<Counted, BodyError> {
    Ok(Counted(query.get().len() as u32))
}

/// Iter round body：推进一轮预算（每轮 +10 花费、轮次 +1）。
async fn spend(query: Query<&Budget>) -> Result<Budget, BodyError> {
    let budget = query.get();
    Ok(Budget {
        spent: budget.spent + 10,
        rounds: budget.rounds + 1,
    })
}

/// Iter round 判断：读取**进入本轮时**的 current，达到 3 轮即发出 `IterBreak`。
async fn stop_after_three(query: Query<&Budget>) -> Result<(), BodyError> {
    if query.get().rounds >= 3 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new();

    // 组合链：Root(Vec<Span>, Route) → Each 收集 → Choose 路由。
    let sum: Counted = runtime
        .execute(
            |flow, (spans, route): (Ref<Vec<Span>>, Ref<Route>)| {
                let lengths = flow.each(spans, |span, each| each.then(length, span));
                flow.choose(route, |choice| {
                    choice.case(Route { code: 1 }, |branch| branch.then(total, lengths));
                    choice.otherwise(|branch| branch.then(count, lengths));
                })
            },
            (vec![Span(1), Span(2)], Route { code: 1 }),
        )
        .await?;
    assert_eq!(sum, Counted(5));
    println!("sum: {}", sum.0);

    let count: Counted = runtime
        .execute(
            |flow, (spans, route): (Ref<Vec<Span>>, Ref<Route>)| {
                let lengths = flow.each(spans, |span, each| each.then(length, span));
                flow.choose(route, |choice| {
                    choice.case(Route { code: 1 }, |branch| branch.then(total, lengths));
                    choice.otherwise(|branch| branch.then(count, lengths));
                })
            },
            (vec![Span(1), Span(2)], Route { code: 9 }),
        )
        .await?;
    assert_eq!(count, Counted(2));
    println!("count: {}", count.0);

    // Iter：每轮推进 current，业务值在进入第 4 轮时表达 Break（交付进入该轮时的 current）。
    let budget: Budget = runtime
        .execute(
            |flow, budget: Ref<Budget>| {
                flow.iter(budget, 5, |current, round| {
                    round.then(stop_after_three, current);
                    round.then(spend, current)
                })
            },
            Budget {
                spent: 0,
                rounds: 0,
            },
        )
        .await?;
    assert_eq!(
        budget,
        Budget {
            spent: 30,
            rounds: 3
        }
    );
    println!("budget: {budget:?}");

    // 定义期拒绝可观察：空的 Choose 配置（既无 case 也无 otherwise）在任何业务 Node 之前被拒。
    let outcome = runtime
        .execute(
            |flow, route: Ref<Route>| {
                let _: () = flow.choose(route, |_choice| {});
            },
            Route { code: 1 },
        )
        .await;
    match outcome {
        Err(RunError::Definition(error)) => {
            println!("empty choose rejected at definition build: {error}");
        }
        Err(other) => return Err(format!("unexpected error: {other}").into()),
        Ok(()) => return Err("an empty choose configuration unexpectedly ran".into()),
    }

    Ok(())
}
