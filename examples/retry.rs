//! 示例：Retry —— 用同一个业务 Input “生成 → 检查 → 重做”。
//!
//! Body 是一个 Flow：`Generator` 产出候选，`Checker` 检查候选并把**检查结论放进正常 Output**。
//! Retry 的 Condition 只读取该结论。示例演示：第一轮之后早停、一直不被接受直到用满上限（返回
//! 最后一次正常 Output）、以及把 Retry 当作父 Flow 的普通 child。
//!
//! 运行：`cargo run --example retry`

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use srflow::{ExecutionError, Flow, FlowBuilder, Node, Retry, RetryDecision, Runtime, consume};

/// 生成出来的候选。`round` 是第几次生成，仅用于让示例每次得到不同的候选文本。
struct Candidate {
    round: usize,
    text: String,
}

/// 检查结论：候选 + 是否接受。它是正常业务 Output，不是执行错误。
struct Checked {
    candidate: Candidate,
    accepted: bool,
}

/// 假生成器：每次产出一个不同的候选。
///
/// 它持有的计数器只用来**模拟每次生成不同的候选**（否则同一 Input 会得到同一份文本），
/// 不是跨轮传递业务数据的通道——业务数据只经 Input／Output 流动。
struct Generator {
    runs: AtomicUsize,
}

impl Node for Generator {
    type Input = String;
    type Output = Candidate;

    async fn run(&self, input: String) -> Result<Candidate, ExecutionError> {
        let round = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Candidate {
            round,
            text: format!("{input} (draft {round})"),
        })
    }
}

/// 假检查器：把“是否接受”作为独立字段写进 Output（业务判断在这里发生，Condition 只读取它）。
struct Checker {
    accept_from: usize,
}

impl Node for Checker {
    type Input = Candidate;
    type Output = Checked;

    async fn run(&self, candidate: Candidate) -> Result<Checked, ExecutionError> {
        let accepted = candidate.round >= self.accept_from;
        Ok(Checked {
            candidate,
            accepted,
        })
    }
}

/// 把被采用的候选渲染成最终文本。
struct Render;

impl Node for Render {
    type Input = Checked;
    type Output = String;

    async fn run(&self, input: Checked) -> Result<String, ExecutionError> {
        Ok(input.candidate.text)
    }
}

/// Body：`String → Checked`，由生成与检查两步组成。
fn attempt_flow(accept_from: usize) -> Flow<String, Checked> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let candidate = flow
        .then(
            Generator {
                runs: AtomicUsize::new(0),
            },
            input,
        )
        .expect("连接失败");
    let checked = flow
        .then(Checker { accept_from }, consume(candidate))
        .expect("连接失败");
    flow.output(checked).expect("连接失败")
}

fn main() {
    let runtime = Runtime::new();
    // Condition 只读取已形成的检查结论；同一个无捕获闭包可被多个 Retry 复用。
    let condition = |checked: &Checked| {
        if checked.accepted {
            RetryDecision::Stop
        } else {
            RetryDecision::Retry
        }
    };

    // 1) 早停：第 2 次生成的候选被接受。
    let early = Retry::with_limit(
        attempt_flow(2),
        condition,
        NonZeroUsize::new(3).expect("上限非零"),
    );
    let accepted = futures::executor::block_on(runtime.execute(&early, String::from("topic")))
        .expect("执行失败");
    println!(
        "早停：{}（accepted={}）",
        accepted.candidate.text, accepted.accepted
    );

    // 2) 耗尽：一直不被接受，用满 3 次后返回最后一次正常 Output，而不是 Error。
    let exhausted = Retry::with_limit(
        attempt_flow(usize::MAX),
        condition,
        NonZeroUsize::new(3).expect("上限非零"),
    );
    let unchecked = futures::executor::block_on(runtime.execute(&exhausted, String::from("topic")))
        .expect("执行失败");
    println!(
        "耗尽：{}（accepted={}）",
        unchecked.candidate.text, unchecked.accepted
    );

    // 3) 作为父 Flow 的普通 child：Binding 把 Flow Input 接给 Retry，之后照常连接下游。
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let retried = flow
        .then(
            Retry::with_limit(
                attempt_flow(1),
                condition,
                NonZeroUsize::new(3).expect("上限非零"),
            ),
            input,
        )
        .expect("连接失败");
    // Retry 的 Output 没有实现 `Clone`，用消费读取交给下游。
    let rendered = flow.then(Render, consume(retried)).expect("连接失败");
    let flow = flow.output(rendered).expect("连接失败");

    let text = futures::executor::block_on(runtime.execute(&flow, String::from("topic")))
        .expect("执行失败");
    println!("作为 Flow child：{text}");
}
