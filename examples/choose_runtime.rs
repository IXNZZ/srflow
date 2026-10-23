//! Choose 运行期示例：前序 Node 产生 selector，按值命中 case／otherwise。
//!
//! 运行：`cargo run --offline --example choose_runtime`
//!
//! 业务键 `Route` 只声明 `Data` 并手写 `PartialEq`：不派生 `Eq`／`Hash`／`Clone`／
//! `Copy`／`Debug`，其中一个字段使用 `std::rc::Rc`。selector 由前序 Node 产生，
//! 运行期只执行选中的一个分支；无匹配且无 otherwise 时返回 `RunError::NoMatchingCase`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};
use std::rc::Rc;

/// 业务路由键：只实现 `Data + PartialEq`；`source` 用 `Rc` 承载共享标签。
#[derive(Data)]
struct Route {
    source: Rc<str>,
    code: u8,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && self.source == other.source
    }
}

/// 根输入与各分支样本。
#[derive(Data)]
struct Seed {
    value: u32,
}

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

/// 前序 Node：由输入值产生 selector（`Ref<Route>`）。
async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(Route {
        source: Rc::from("selector"),
        code: query.get().value as u8,
    })
}

/// case 1 命中时返回单 `Ref<Amount>`（双倍）。
async fn double(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value * 2))
}

/// case 2 命中时返回单 `Ref<Amount>`（三倍）。
async fn triple(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value * 3))
}

/// otherwise 回退时返回单 `Ref<Amount>`。
async fn fallback(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value + 100))
}

/// 后续 parent Node：读取 Choose 选中的 `Ref<Amount>` 结果。
async fn report(query: Query<&Amount>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

/// 匹配键：与新构造的 `Route` 按值比较。
fn route(code: u8) -> Route {
    Route {
        source: Rc::from("selector"),
        code,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new();

    // 情形一：case 命中（value=1 → code=1 → double）。
    let matched = runtime
        .execute::<_, Report, _>(
            |flow, seed| {
                let selector = flow.then_with_output(route_of, seed);
                let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                    choice.case(route(1), |branch| {
                        let out_refs = branch.then_with_output(double, seed);
                        branch.output_with(out_refs);
                    });
                    choice.case(route(2), |branch| {
                        let out_refs = branch.then_with_output(triple, seed);
                        branch.output_with(out_refs);
                    });
                    choice.otherwise(|branch| {
                        let out_refs = branch.then_with_output(fallback, seed);
                        branch.output_with(out_refs);
                    });
                });
                let out_refs = flow.then_with_output(report, picked);
                flow.output_with(out_refs);
            },
            Seed { value: 1 },
        )
        .await?;
    println!("matched: {}", matched.0);

    // 情形二：无 case 命中，otherwise 回退（value=7 → code=7）。
    let fallback = runtime
        .execute::<_, Report, _>(
            |flow, seed| {
                let selector = flow.then_with_output(route_of, seed);
                let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                    choice.case(route(1), |branch| {
                        let out_refs = branch.then_with_output(double, seed);
                        branch.output_with(out_refs);
                    });
                    choice.case(route(2), |branch| {
                        let out_refs = branch.then_with_output(triple, seed);
                        branch.output_with(out_refs);
                    });
                    choice.otherwise(|branch| {
                        let out_refs = branch.then_with_output(fallback, seed);
                        branch.output_with(out_refs);
                    });
                });
                let out_refs = flow.then_with_output(report, picked);
                flow.output_with(out_refs);
            },
            Seed { value: 7 },
        )
        .await?;
    println!("fallback: {}", fallback.0);

    // 情形三：只有 case 且不匹配 → RunError::NoMatchingCase。
    let no_match = runtime
        .execute::<_, Report, _>(
            |flow, seed| {
                let selector = flow.then_with_output(route_of, seed);
                let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                    choice.case(route(1), |branch| {
                        let out_refs = branch.then_with_output(double, seed);
                        branch.output_with(out_refs);
                    });
                    choice.case(route(2), |branch| {
                        let out_refs = branch.then_with_output(triple, seed);
                        branch.output_with(out_refs);
                    });
                });
                let out_refs = flow.then_with_output(report, picked);
                flow.output_with(out_refs);
            },
            Seed { value: 7 },
        )
        .await;
    match no_match {
        Ok(report) => println!("no matching case: false ({})", report.0),
        Err(RunError::NoMatchingCase) => println!("no matching case: true"),
        Err(other) => return Err(format!("unexpected error: {other}").into()),
    }

    Ok(())
}
