//! 示例：Each —— 顺序逐项执行，并按顺序收集结果。
//!
//! 两条路径：
//!
//! 1. 直接经 `Runtime` 执行 `Each`（Body 是一个 Flow）；
//! 2. 作为父 Flow 的 child：集合由 Flow Input 提供，用消费读取把整个 `Vec<Claim>` 交给 Each，
//!    下游继续处理 `Vec<Rendered>`。
//!
//! `Claim` 与 `Rendered` 都没有实现 `Clone`：Each 逐项**移动** Item、按值收集 Output，不需要复制。
//!
//! 为什么这里用 Each 而不是 Iter：每一项的处理只依赖它自己，不需要上一项执行后形成的状态。
//!
//! 运行：`cargo run --example each`

use srflow::{Each, ExecutionError, Flow, FlowBuilder, Node, Runtime};

/// 输入项。刻意不实现 `Clone`。
struct Claim(String);

/// 输出项。刻意不实现 `Clone`。
struct Rendered(String);

/// Body 的第一步：把一条 Claim 规范成一行文本。
struct Normalize;

impl Node for Normalize {
    type Input = Claim;
    type Output = String;

    async fn run(&self, input: Claim) -> Result<String, ExecutionError> {
        let width = input.0.chars().count();
        Ok(format!("{}({width} 字)", input.0))
    }
}

/// Body 的第二步：渲染成最终输出。
struct Wrap;

impl Node for Wrap {
    type Input = String;
    type Output = Rendered;

    async fn run(&self, input: String) -> Result<Rendered, ExecutionError> {
        Ok(Rendered(format!("<{input}>")))
    }
}

/// Body：一个 Flow，因此链路是 `Runtime → Each → Runtime → Flow → Runtime → Node`。
fn render_flow() -> Flow<Claim, Rendered> {
    let mut flow = FlowBuilder::<Claim>::new();
    let claim = flow.input();
    // `Claim` 不是 `Clone`：消费读取把它交给第一步。
    let normalized = flow.then_move(Normalize, claim).expect("连接失败");
    let rendered = flow.then_move(Wrap, normalized).expect("连接失败");
    flow.output(rendered).expect("连接失败")
}

/// 下游 Node：把 `Vec<Rendered>` 汇成一行，序号即输入顺序。
struct Join;

impl Node for Join {
    type Input = Vec<Rendered>;
    type Output = String;

    async fn run(&self, input: Vec<Rendered>) -> Result<String, ExecutionError> {
        let joined = input
            .into_iter()
            .enumerate()
            .map(|(index, rendered)| format!("{}. {}", index + 1, rendered.0))
            .collect::<Vec<_>>()
            .join(" | ");
        Ok(joined)
    }
}

fn claims() -> Vec<Claim> {
    vec![
        Claim(String::from("第一项")),
        Claim(String::from("第二项")),
        Claim(String::from("第三项")),
    ]
}

fn main() {
    let runtime = Runtime::new();

    // 1) 直接经 Runtime 执行：Items 按顺序处理，Outputs 按同一顺序返回。
    let each = Each::new(render_flow());
    let rendered = futures::executor::block_on(runtime.execute(&each, claims())).expect("执行失败");
    let joined = rendered
        .into_iter()
        .enumerate()
        .map(|(index, rendered)| format!("{}. {}", index + 1, rendered.0))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("直接执行：{joined}");

    // 2) 作为父 Flow 的 child：整个 `Vec<Claim>` 用消费读取进入 Each，`Vec<Rendered>` 交给下游。
    let mut flow = FlowBuilder::<Vec<Claim>>::new();
    let input = flow.input();
    let rendered = flow
        .then_move(Each::new(render_flow()), input)
        .expect("连接失败");
    let joined = flow.then_move(Join, rendered).expect("连接失败");
    let flow = flow.output(joined).expect("连接失败");

    let joined = futures::executor::block_on(runtime.execute(&flow, claims())).expect("执行失败");
    println!("作为 Flow child：{joined}");

    // 空集合是正常输入：Body 不执行，返回空结果。
    let empty = futures::executor::block_on(runtime.execute(&each, Vec::new())).expect("执行失败");
    println!("空集合：{} 项（Body 执行 0 次）", empty.len());
}
