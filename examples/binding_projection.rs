//! 示例：字段投影 —— 非 `Clone` 根结构 + tuple Binding。
//!
//! 根结构 `Story` 没有实现 `Clone`，但它的不同字段可以被分别投影给下游 Node，并用 tuple
//! 把两个已声明结果组合成一次 Input。投影只借用根、复制目标字段，不会复制根上的其他大字段。
//!
//! 运行：`cargo run --example binding_projection`

use srflow::{ExecutionError, FlowBuilder, Node, Runtime, field};

/// Flow Input：一个没有实现 `Clone` 的故事结构。
struct Story {
    title: String,
    // 大字段：投影 title 时不应被复制。
    body: String,
}

/// 统计标题字符数。
struct TitleLength;

impl Node for TitleLength {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

/// 统计正文字符数。
struct BodyLength;

impl Node for BodyLength {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

/// 把一个 tuple 组合成摘要：tuple Binding 把两个已声明结果接成一次 Input。
struct Summary;

impl Node for Summary {
    type Input = (usize, usize);
    type Output = String;

    async fn run(&self, (title, body): (usize, usize)) -> Result<String, ExecutionError> {
        Ok(format!("title={title}, body={body}"))
    }
}

fn main() {
    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();

    // 两次字段投影：各取一个字段，Story 整体没有被复制，也没有被消费。
    let title = flow
        .then(TitleLength, field!(story.title))
        .expect("连接失败");
    let body = flow.then(BodyLength, field!(story.body)).expect("连接失败");

    // tuple Binding：组合两个已声明结果。
    let summary = flow.then(Summary, (title, body)).expect("连接失败");
    let flow = flow.output(summary).expect("连接失败");

    let runtime = Runtime::new();
    let story = Story {
        title: String::from("SRFlow"),
        body: String::from("connects executables through one runtime"),
    };
    let summary = futures::executor::block_on(runtime.execute(&flow, story)).expect("执行失败");
    println!("{summary}");
}
