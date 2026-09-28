//! 示例：普通使用者只实现 `Node`，即可通过 `Runtime` 执行。
//!
//! 这里用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor，
//! 使用者需要在自己的项目里选择并添加一个。
//!
//! 运行：`cargo run --example node_only`

use srflow::{ExecutionError, Node, Runtime};

/// 统计一段文本中空白分隔的词数。
struct WordCount;

impl Node for WordCount {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.split_whitespace().count())
    }
}

fn main() {
    let runtime = Runtime::new();
    let text = String::from("SRFlow 让所有 Executable 经过同一个 Runtime");
    let words = futures::executor::block_on(runtime.execute(&WordCount, text)).expect("执行失败");
    println!("word count = {words}");
}
