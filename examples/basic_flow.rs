//! 示例：基础 Flow —— Ref 复用与显式 Output。
//!
//! T02 只提供整值连接：一个位置可以被多个步骤复用，但还不能把多个值装配成同一个 Input
//! （那是 T03 Binding 的能力）。因此这里两个步骤读取同一份规范化文本，Flow 只输出其中之一。
//!
//! 示例用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor。
//!
//! 运行：`cargo run --example basic_flow`

use srflow::{ExecutionError, FlowBuilder, Node, Runtime};

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

/// 统计字符数。
struct CharCount;

impl Node for CharCount {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

fn main() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();

    // 消费读取：原始文本交给 Normalize 之后不再被读取。
    let normalized = flow.then_move(Normalize, input).expect("连接失败");

    // 复用读取：规范化文本被两个步骤共用。它是这个位置的第 1、2 次读取，
    // 因此 `WordCount` 拿到一份副本，最后一次读取（`CharCount`）直接取走原值：
    // 一次复用只发生一次复制，而不是两次。
    let words = flow.then(WordCount, normalized).expect("连接失败");
    let _char_count = flow.then(CharCount, normalized).expect("连接失败");

    // 显式声明输出：选词数。未被选中的分支值不会成为这个 Flow 的输出。
    let flow = flow.output(words).expect("连接失败");

    let runtime = Runtime::new();
    let text = String::from("  SRFlow   connects\n executables through one runtime  ");
    let words = futures::executor::block_on(runtime.execute(&flow, text)).expect("执行失败");
    println!("word count = {words}");
}
