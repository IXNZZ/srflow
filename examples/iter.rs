//! 示例：Iter —— 让上一轮形成的正文成为下一轮的输入状态。
//!
//! 每条 `Key`（关键节点）按顺序推进同一份 `ProseState`：`plan` 是每轮仍需要的不变上下文，`prose` 按
//! 节点顺序累积（顺序敏感），`needs_revision` 是 Body 给出的**正常业务状态**——它出现时迭代不会提前
//! 停止，节点仍会全部处理完。
//!
//! 两条路径：
//!
//! 1. 直接经 `Runtime` 执行；
//! 2. 作为父 Flow 的 child：集合与初始状态来自**两个不同的数据位置**，用 tuple Binding +
//!    `consume` 组合成 `(Vec<Key>, ProseState)`（两者都没有实现 `Clone`）。
//!
//! 为什么这里用 Iter 而不是 Each：后一个关键节点需要看到前面节点已经形成的正文。
//! 示例用 `?` 传播 Flow 构建错误和执行错误，让接线与数据流保持清晰。
//!
//! 运行：`cargo run --example iter`

use std::error::Error;

use srflow::{
    ExecutionError, Flow, FlowBuildError, FlowBuilder, Iter, Node, Runtime, consume, field,
};

/// 关键节点（Item）。刻意不实现 `Clone`。
struct Key(String);

/// 跨轮携带的状态。刻意不实现 `Clone`，也不实现 `Default`。
struct ProseState {
    plan: String,
    prose: String,
    needs_revision: bool,
}

/// Body 第一步：把当前关键节点按顺序追加进正文。
struct Append;

impl Node for Append {
    type Input = (ProseState, Key);
    type Output = ProseState;

    async fn run(&self, input: (ProseState, Key)) -> Result<ProseState, ExecutionError> {
        let (mut state, key) = input;
        state.prose.push_str(&key.0);
        Ok(state)
    }
}

/// Body 第二步：按业务规则给出正常状态标记（不是技术错误，也不会让迭代停下）。
struct Review;

impl Node for Review {
    type Input = ProseState;
    type Output = ProseState;

    async fn run(&self, mut state: ProseState) -> Result<ProseState, ExecutionError> {
        state.needs_revision = state.prose.chars().count() > 4;
        Ok(state)
    }
}

/// Body：一个 Flow，因此链路是 `Runtime → Iter → Runtime → Flow → Runtime → Node`。
fn advance_flow() -> Result<Flow<(ProseState, Key), ProseState>, FlowBuildError> {
    let mut flow = FlowBuilder::<(ProseState, Key)>::new();
    let input = flow.input();
    // `(ProseState, Key)` 都不是 `Clone`：消费读取把整个元组交给第一步。
    let appended = flow.then_move(Append, input)?;
    let reviewed = flow.then_move(Review, appended)?;
    flow.output(reviewed)
}

/// 父 Flow 的输入：节点主题与每轮都要用的计划。
struct Brief {
    topics: Vec<String>,
    plan: String,
}

/// 把主题列表变成非 `Clone` 的 `Key` 集合。
struct MakeKeys;

impl Node for MakeKeys {
    type Input = Vec<String>;
    type Output = Vec<Key>;

    async fn run(&self, topics: Vec<String>) -> Result<Vec<Key>, ExecutionError> {
        Ok(topics.into_iter().map(Key).collect())
    }
}

/// 形成初始状态。
struct MakeInitial;

impl Node for MakeInitial {
    type Input = String;
    type Output = ProseState;

    async fn run(&self, plan: String) -> Result<ProseState, ExecutionError> {
        Ok(ProseState {
            plan,
            prose: String::new(),
            needs_revision: false,
        })
    }
}

/// 下游 Node：把最终状态汇报成一行文本。
struct Report {
    name: String
}

impl Node for Report {
    type Input = ProseState;
    type Output = String;

    async fn run(&self, state: ProseState) -> Result<String, ExecutionError> {
        Ok(format!(
            "plan={} prose={} needs_revision={}, name={}",
            state.plan, state.prose, state.needs_revision, self.name
        ))
    }
}

fn initial_state() -> ProseState {
    ProseState {
        plan: String::from("三幕结构"),
        prose: String::new(),
        needs_revision: false,
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    futures::executor::block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new();

    // 1) 直接执行：逐轮把上一轮形成的正文带进下一轮。
    let keys = vec![
        Key(String::from("开场")),
        Key(String::from("冲突")),
        Key(String::from("转折")),
    ];
    let state = runtime
        .execute(&Iter::new(advance_flow()?), (keys, initial_state()))
        .await?;
    println!(
        "直接执行：plan={} prose={} needs_revision={}",
        state.plan, state.prose, state.needs_revision
    );

    // 2) 作为父 Flow 的 child：集合与初始状态来自两个不同位置，用 tuple Binding + consume 组合。
    let mut flow = FlowBuilder::<Brief>::new();
    let brief = flow.input();



    let keys = flow.then(MakeKeys, field!(brief.topics))?;
    
    let initial = flow.then(MakeInitial, field!(brief.plan))?;
    let advanced = flow.then(
        Iter::new(advance_flow()?),
        (consume(keys), consume(initial)),
    )?;

    let report1 = Report { name: String::from("John") };

    let report = flow.then_move(report1, advanced)?;
    let flow = flow.output(report)?;

    let brief = Brief {
        topics: vec![String::from("开场"), String::from("冲突")],
        plan: String::from("三幕结构"),
    };
    let report = runtime.execute(&flow, brief).await?;
    println!("作为 Flow child：{report}");

    // 3) 空集合：Body 执行 0 次，初始状态按值返回。
    let state = runtime
        .execute(&Iter::new(advance_flow()?), (Vec::new(), initial_state()))
        .await?;
    println!(
        "空集合：Body 执行 0 次，原样返回初始状态（plan={}）",
        state.plan
    );
    Ok(())
}
