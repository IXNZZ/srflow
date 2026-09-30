//! 示例：Match —— 判断与路由分离。
//!
//! `Judge` 先产出路由值，父 Flow 再用 tuple Binding 把该值和已有业务 Input 组装成 `(K, I)` 交给
//! `Match`。示例包含：两个不同具体类型的分支（Node 与 Flow）、未命中时的 default、以及一条
//! **非 `Clone`** 的数据路径（`Route` 与 `Draft` 都没有实现 `Clone`，因此在 tuple 内用 `consume`
//! 显式交出）。
//!
//! 运行：`cargo run --example match`

use srflow::{ExecutionError, Flow, FlowBuilder, Match, Node, Runtime, consume, field};

/// 路由值：由 `Judge` 判断得到。Match 只消费它，因此不需要 `Clone`。
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Quick,
    Deep,
    Unknown,
}

/// Flow Input：待处理的主题。根结构不需要 `Clone`。
struct Brief {
    topic: String,
}

/// 被选分支共享的业务 Input。Match 只把它交给分支一次，因此不需要 `Clone`。
struct Draft {
    text: String,
}

/// 形成路由值：这是业务判断，属于 Node，不属于 Match。
struct Judge;

impl Node for Judge {
    type Input = String;
    type Output = Route;

    async fn run(&self, topic: String) -> Result<Route, ExecutionError> {
        Ok(match topic.chars().count() {
            0..=7 => Route::Quick,
            8..=15 => Route::Deep,
            _ => Route::Unknown,
        })
    }
}

/// 产出业务 Input `Draft`。只读取主题文本，主题位置随后仍然可用。
struct Compose;

impl Node for Compose {
    type Input = String;
    type Output = Draft;

    async fn run(&self, topic: String) -> Result<Draft, ExecutionError> {
        Ok(Draft {
            text: format!("围绕“{topic}”的草稿"),
        })
    }
}

/// 分支一（Node）：短主题直接压缩。
struct Shorten;

impl Node for Shorten {
    type Input = Draft;
    type Output = String;

    async fn run(&self, input: Draft) -> Result<String, ExecutionError> {
        let head: String = input.text.chars().take(8).collect();
        Ok(format!("[quick] {head}"))
    }
}

/// 分支二（Flow）的内部步骤：先数长度。
struct Count;

impl Node for Count {
    type Input = Draft;
    type Output = usize;

    async fn run(&self, input: Draft) -> Result<usize, ExecutionError> {
        Ok(input.text.chars().count())
    }
}

/// 分支二（Flow）的内部步骤：再包装。
struct Wrap;

impl Node for Wrap {
    type Input = usize;
    type Output = String;

    async fn run(&self, input: usize) -> Result<String, ExecutionError> {
        Ok(format!("[deep] {input} 字"))
    }
}

/// 分支二：一个 Flow，与分支一同为 `Draft → String` 契约，但具体类型不同。
fn deep_flow() -> Flow<Draft, String> {
    let mut flow = FlowBuilder::<Draft>::new();
    let draft = flow.input();
    // `Draft` 没有实现 `Clone`：消费读取把它交给第一个步骤。
    let count = flow.then_move(Count, draft).expect("连接失败");
    let text = flow.then_move(Wrap, count).expect("连接失败");
    flow.output(text).expect("连接失败")
}

/// default：唯一的“未命中路径”，不是被选分支失败后的备用路径。
struct Fallback;

impl Node for Fallback {
    type Input = Draft;
    type Output = String;

    async fn run(&self, input: Draft) -> Result<String, ExecutionError> {
        Ok(format!("[fallback] {}", input.text))
    }
}

fn main() {
    let runtime = Runtime::new();

    let mut builder = Match::<Route, Draft, String>::builder();
    builder.case(Route::Quick, Shorten).expect("登记失败");
    builder.case(Route::Deep, deep_flow()).expect("登记失败");
    builder.default(Fallback).expect("登记失败");
    let matcher = builder.build();

    // 父 Flow：Judge 先产出路由值，再用 tuple Binding 组装 (K, I)。
    let mut flow = FlowBuilder::<Brief>::new();
    let brief = flow.input();
    let route = flow.then(Judge, field!(brief.topic)).expect("连接失败");
    let draft = flow.then(Compose, field!(brief.topic)).expect("连接失败");
    // `Route` 与 `Draft` 都没有实现 `Clone`：用 consume 显式交出这两个位置。
    let outcome = flow
        .then(matcher, (consume(route), consume(draft)))
        .expect("连接失败");
    let flow = flow.output(outcome).expect("连接失败");

    for topic in [
        "短文",
        "一个长度适中的主题描述",
        "一个明显超过十六个字符的、非常长的主题描述文本",
    ] {
        let brief = Brief {
            topic: String::from(topic),
        };
        match futures::executor::block_on(runtime.execute(&flow, brief)) {
            Ok(text) => println!("{topic} -> {text}"),
            Err(error) => println!("{topic} -> 执行失败：{error}"),
        }
    }
}
