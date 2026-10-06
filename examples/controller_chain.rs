//! 控制器组合示例：Each 收集 → Match 路由（跨两类控制器的真实调用链）。
//!
//! 运行：`cargo run --offline --example controller_chain`
//!
//! 同时演示：Loop（Iter）多轮推进与业务值表达 Continue／Finish、以及构建期拒绝
//! （普通 unit 函数）可被调用方观察。

use futures::executor::block_on;
use srflow::{
    BodyError, BuildError, Data, DataRef, Each, EachBuilder, EachOnly, Flow, FlowBuilder, Iter1,
    Loop, LoopBuilder, LoopControl, LoopDecision, Match, MatchBuilder, Runtime,
};

#[derive(Debug, PartialEq, Eq)]
struct Span(u32);

#[derive(Debug, PartialEq, Eq)]
struct Length(u32);

#[derive(Debug, PartialEq, Eq)]
struct Budget {
    spent: u32,
    rounds: u32,
}
impl LoopControl for Budget {
    fn loop_decision(&self) -> LoopDecision {
        if self.rounds >= 3 {
            LoopDecision::Finish
        } else {
            LoopDecision::Continue
        }
    }
}

fn length(span: &Span) -> Result<Length, BodyError> {
    Ok(Length(span.0 + 1))
}

fn spend(budget: &Budget) -> Result<Budget, BodyError> {
    Ok(Budget {
        spent: budget.spent + 10,
        rounds: budget.rounds + 1,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Each：Vec<Span> → Vec<Length>（Node body）
    let mut each = EachBuilder::<EachOnly<Span>, Length>::start()?;
    each.then_body(length)?;
    let each: Each<EachOnly<Span>, Length> = each.finish()?;

    // Match：路由 1 求和、default 计数（业务输入就是 Each 的 Vec<Length>）
    let mut matcher = MatchBuilder::<u32, Vec<Length>, Data<u32>>::start()?;
    matcher.branch(1, |values: &Vec<Length>| {
        Ok(values.iter().map(|value| value.0).sum())
    })?;
    matcher.default(|values: &Vec<Length>| Ok(values.len() as u32))?;
    let matcher: Match<u32, Vec<Length>, Data<u32>> = matcher.finish()?;

    // 组合链：Root(Vec<Span>, u32) → Each → Match
    let (mut root, (spans, route)) = FlowBuilder::<(Vec<Span>, u32)>::start()?;
    let lengths: DataRef<Vec<Length>> = root.then(each, spans)?;
    let routed: DataRef<u32> = root.then(matcher, (route, lengths))?;
    let root: Flow<(Vec<Span>, u32), Data<u32>> = root.finish::<Data<u32>, _>(routed)?;

    let sum = Runtime::execute(&root, (vec![Span(1), Span(2)], 1)).await?;
    assert_eq!(sum, 5);
    println!("sum: {sum}");
    let count = Runtime::execute(&root, (vec![Span(1), Span(2)], 9)).await?;
    assert_eq!(count, 2);
    println!("count: {count}");

    // Loop（Iter）：current-state 每轮推进，业务值表达何时 Finish
    let mut loop_builder = LoopBuilder::<Iter1<Budget>>::start()?;
    loop_builder.then_body(spend)?;
    let looped: Loop<Iter1<Budget>> = loop_builder.finish()?;
    let (mut loop_root, budget) = FlowBuilder::<(Budget,)>::start()?;
    let final_budget: DataRef<Budget> = loop_root.then(looped, budget)?;
    let loop_root: Flow<(Budget,), Data<Budget>> =
        loop_root.finish::<Data<Budget>, _>(final_budget)?;
    let final_budget = Runtime::execute(
        &loop_root,
        (Budget {
            spent: 0,
            rounds: 0,
        },),
    )
    .await?;
    assert_eq!(
        final_budget,
        Budget {
            spent: 30,
            rounds: 3
        }
    );
    println!("budget: {final_budget:?}");

    // 构建期拒绝可观察：普通 unit 函数不产生业务 Data。
    let (mut rejected, input) = FlowBuilder::<(Span,)>::start()?;
    let outcome = rejected.then(
        (|span: &Span| {
            let _ = span;
            Ok(())
        }) as fn(&Span) -> Result<(), BodyError>,
        input,
    );
    match outcome {
        Err(BuildError::UnsupportedFunctionUnitOutput) => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err("unit function unexpectedly produced a Flow step".into()),
    }
    println!("unit function rejected at definition build");

    Ok(())
}
