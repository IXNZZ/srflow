//! 示例：高级扩展者直接实现组合型 `Executable`，并通过同一个 `Runtime` 执行 child。
//!
//! 这里只有一个用于演示执行协议的组合：它不是生产语义上的 Flow、Retry 或 Match，
//! 只是把两个 child 串起来，因此不引入 T01 之外的新概念。
//!
//! 这里用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor，
//! 使用者需要在自己的项目里选择并添加一个。
//!
//! 运行：`cargo run --example composite_executable`

use srflow::{Executable, ExecutionError, Node, Runtime};

/// 叶子：统计一段文本中空白分隔的词数。
struct WordCount;

impl Node for WordCount {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.split_whitespace().count())
    }
}

/// 叶子：把数值翻倍。
struct Double;

impl Node for Double {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input * 2)
    }
}

/// 组合型 Executable：先统计词数，再把词数翻倍。
///
/// 它对父级只暴露 `String → usize`。两个 child 的实际执行都重新经过父级传入的 `Runtime`：
///
/// ```text
/// Runtime::execute(&WordCountDoubled, text)
///   └─ WordCountDoubled::execute(&runtime, text)
///        ├─ runtime.execute(&WordCount, text)
///        └─ runtime.execute(&Double, words)
/// ```
struct WordCountDoubled;

impl Executable for WordCountDoubled {
    type Input = String;
    type Output = usize;

    async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
        let words = runtime.execute(&WordCount, input).await?;
        runtime.execute(&Double, words).await
    }
}

fn main() {
    let runtime = Runtime::new();
    let text = String::from("SRFlow connects executables through one runtime");
    let doubled =
        futures::executor::block_on(runtime.execute(&WordCountDoubled, text)).expect("执行失败");
    println!("doubled word count = {doubled}");
}
