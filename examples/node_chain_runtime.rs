//! 新契约最小主链示例：Node → Chain → owned Result，以及零输入 Root + unit Chain。
//!
//! 运行：`cargo run --offline --example node_chain_runtime`
//!
//! 业务 Data 用 `#[derive(Data)]` 声明；Node 只接收声明输入的只读 `Query`，返回新的
//! owned 值。Chain 建立真实 child 数据边界：child 的中间值在交给 parent 之前清理。
// 示例刻意保留冻结用法里的显式 unit 形状（`let _: () = …` / 末尾 `()`）。
#![allow(clippy::unused_unit, clippy::let_unit_value)]

use futures::executor::block_on;
use srflow::{BodyError, Data, Node, Query, RunError, Runtime};

#[derive(Data)]
struct Prompt(u32);

#[derive(Data)]
struct Draft(u32);

#[derive(Data)]
struct Report(u32);

/// 动态业务错误：失败时原对象与 source chain 由 [`RunError::Body`] 保留。
#[derive(Debug)]
struct InvalidPrompt;

impl std::fmt::Display for InvalidPrompt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("prompt must be positive")
    }
}

impl std::error::Error for InvalidPrompt {}

/// 函数 Node：自然异步签名；条件失败时返回不可恢复 Failure。
async fn require_prompt(query: Query<&Prompt>) -> Result<Prompt, BodyError> {
    let prompt = query.get();
    if prompt.0 == 0 {
        return Err(BodyError::fail(InvalidPrompt));
    }
    Ok(Prompt(prompt.0))
}

/// 函数 Node：自然异步签名。
async fn generate(query: Query<&Prompt>) -> Result<Draft, BodyError> {
    Ok(Draft(query.get().0 * 2))
}

/// 结构体 Node：`type Input`／`type Output` 协议。
struct Review;

impl Node for Review {
    type Input = Draft;
    type Output = Report;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Report, BodyError> {
        Ok(Report(query.get().0 + 1))
    }
}

/// 零输入 Node：不登记占位业务值，成功输出是显式 `()`。
async fn stamp(query: Query<()>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // 单值 Root 输入（不使用 singleton tuple）；Chain 返回 child 的声明输出。
    let runtime = Runtime::new();
    let review = Review;
    let report: Report = runtime
        .execute(
            |flow, prompt| {
                let checked = flow.then(require_prompt, prompt);
                let draft = flow.then(generate, checked);
                flow.chain(|sub| sub.then(&review, draft))
            },
            Prompt(20),
        )
        .await?;
    assert_eq!(report.0, 41);
    println!("report: {}", report.0);

    // 失败不返回部分输出：动态业务错误对象经 RunError::Body 保留。
    let runtime = Runtime::new();
    match runtime
        .execute(
            |flow, prompt| {
                let checked = flow.then(require_prompt, prompt);
                let draft = flow.then(generate, checked);
                flow.chain(|sub| sub.then(&review, draft))
            },
            Prompt(0),
        )
        .await
    {
        Ok(report) => println!("unexpected success: {}", report.0),
        Err(RunError::Body(failure)) => {
            let source = std::error::Error::source(&failure)
                .and_then(|error| error.downcast_ref::<InvalidPrompt>());
            assert!(source.is_some(), "original dynamic object preserved");
            println!("rejected: {failure}");
        }
        Err(other) => println!("unexpected error: {other}"),
    }

    // 零输入 Root 保留 `()` 第二参数；unit Chain 同样建立并清理 child 边界。
    let runtime = Runtime::new();
    let () = runtime
        .execute(
            |flow, ()| {
                flow.chain(|sub| {
                    let _: () = sub.then(stamp, ());
                    ()
                });
                ()
            },
            (),
        )
        .await?;
    println!("unit chain done");
    Ok(())
}
