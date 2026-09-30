//! 示例：命名结构装配与嵌套装配。
//!
//! 四个来源（两个字段投影、两个 Node 输出）经 [`bind!`] 装配成 `ProgressionInput`；外层再用
//! 一个 [`bind!`] 把该装配结果与另一个投影嵌套成 `NestedInput`。Binding 只做结构装配，产出
//! 正文仍然由 Node 完成。
//!
//! 运行：`cargo run --example binding_assembly`

use srflow::{ExecutionError, FlowBuilder, Node, Runtime, bind, field};

/// Flow Input：故事设定与背景。
struct StoryInput {
    plan: String,
    key_nodes: Vec<String>,
    background: String,
}

/// 从候选 key 中取第一个，作为下一步的 key（业务判断留在 Node）。
struct FirstKey;

impl Node for FirstKey {
    type Input = Vec<String>;
    type Output = String;

    async fn run(&self, input: Vec<String>) -> Result<String, ExecutionError> {
        Ok(input.into_iter().next().unwrap_or_default())
    }
}

/// 由 plan 生成一段种子正文。
struct SeedProse;

impl Node for SeedProse {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        Ok(format!("seed({input})"))
    }
}

/// 下游业务结构：字段分别来自当前 Flow 的不同数据位置。
struct ProgressionInput {
    plan: String,
    key_node: String,
    background: String,
    previous_prose: String,
}

/// 嵌套结构：其中一个字段本身是 `ProgressionInput`。
struct NestedInput {
    progression: ProgressionInput,
    tag: String,
}

/// 正文状态（Flow Output）。
struct ProseState {
    text: String,
}

/// 真正的业务步骤：它接收完整装配好的 Input，产生新的正文。
struct Compose;

impl Node for Compose {
    type Input = NestedInput;
    type Output = ProseState;

    async fn run(&self, input: NestedInput) -> Result<ProseState, ExecutionError> {
        let progression = input.progression;
        Ok(ProseState {
            text: format!(
                "{} | {} | {} | {} | {}",
                progression.plan,
                progression.key_node,
                progression.background,
                progression.previous_prose,
                input.tag,
            ),
        })
    }
}

fn main() {
    let mut flow = FlowBuilder::<StoryInput>::new();
    let story = flow.input();

    let key = flow
        .then(FirstKey, field!(story.key_nodes))
        .expect("连接失败");
    let seed = flow.then(SeedProse, field!(story.plan)).expect("连接失败");

    // 四来源命名装配：plan / key_node / background / previous_prose。
    let progression = bind!(ProgressionInput {
        plan: field!(story.plan),
        key_node: key,
        background: field!(story.background),
        previous_prose: seed,
    });

    // 嵌套装配：把上面的装配结果整体作为一个字段。
    let nested = bind!(NestedInput {
        progression: progression,
        tag: field!(story.background),
    });

    let prose = flow.then(Compose, nested).expect("连接失败");
    let flow = flow.output(prose).expect("连接失败");

    let runtime = Runtime::new();
    let story = StoryInput {
        plan: String::from("plan-A"),
        key_nodes: vec![String::from("key-1"), String::from("key-2")],
        background: String::from("bg"),
    };
    let prose = futures::executor::block_on(runtime.execute(&flow, story)).expect("执行失败");
    println!("{}", prose.text);
}
