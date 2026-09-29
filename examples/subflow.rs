//! 示例：SubFlow —— 一个 Flow 作为另一个 Flow 的普通 child。
//!
//! SubFlow 不是额外的执行原语：`Flow` 本身就实现了 `Executable`，所以可以直接用
//! `then_move` 连接。父级只能看到子 Flow 的 Input／Output，子 Flow 内部的中间 `Ref`
//! 不会泄漏出来；嵌套执行仍然逐层经过同一个 `Runtime`。
//!
//! 示例用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor。
//!
//! 运行：`cargo run --example subflow`

use srflow::{ExecutionError, Flow, FlowBuilder, Node, Runtime};

/// 折叠空白，得到规范化文本。
struct Normalize;

impl Node for Normalize {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        Ok(input.split_whitespace().collect::<Vec<_>>().join(" "))
    }
}

/// 统计空白分隔的词数。
struct WordCount;

impl Node for WordCount {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.split_whitespace().count())
    }
}

/// 把数值翻倍。
struct Double;

impl Node for Double {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input * 2)
    }
}

/// 子 Flow：规范化文本并统计词数。
///
/// 内部的 `normalized` 只是子 Flow 的中间 Ref，函数返回后父级也拿不到它。
fn normalize_and_count() -> Flow<String, usize> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let normalized = flow.then_move(Normalize, input).expect("连接失败");
    let words = flow.then_move(WordCount, normalized).expect("连接失败");
    flow.output(words).expect("连接失败")
}

fn main() {
    let mut parent = FlowBuilder::<String>::new();
    let input = parent.input();

    // 子 Flow 就是一个普通 child：父级只连接它的 Input／Output。
    let counted = parent
        .then_move(normalize_and_count(), input)
        .expect("连接失败");
    let doubled = parent.then_move(Double, counted).expect("连接失败");
    let parent = parent.output(doubled).expect("连接失败");

    let runtime = Runtime::new();
    let text = String::from("SRFlow connects executables through one runtime");
    let result = futures::executor::block_on(runtime.execute(&parent, text)).expect("执行失败");
    println!("doubled word count = {result}");
}
