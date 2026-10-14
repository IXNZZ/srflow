//! WE-T02 B03 正例：Fragment 保持**非 static** Node 依赖的外部样本。
//!
//! `Suffixer<'a>` 借用一个只在 main 局部存活的字符串，因此 Node 配置类型本身不是
//! `'static`。它仍可作为 `&'n Suffixer<'a>`（`'a: 'n`）经 Fragment 登记进 Chain，
//! 证明公开 Fragment 只需定义期 Node 句柄的借用寿命，不额外要求配置 `'static`。
//!
//! 由主包真实 rlib 以外部消费者身份编译运行（`--extern srflow=...`）。
//!
//! 期望：编译成功，运行退出码 0，断言 `"7-suffix"`，stdout 含 `we_t02-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Text(String);

/// 非 static Node 配置：持有对局部字符串的借用。
struct Suffixer<'a> {
    suffix: &'a str,
}

impl<'a> Node for Suffixer<'a> {
    type Input = Amount;
    type Output = Text;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Text, BodyError> {
        Ok(Text(format!("{}{}", query.get().0, self.suffix)))
    }
}

/// Fragment 按普通 Rust lifetime 描述其非 static Node 依赖：`'a: 'n`，无需第二 lifetime。
fn suffixing<'n, 'a: 'n>(
    node: &'n Suffixer<'a>,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Text> + 'n {
    move |sub| sub.then(node, input)
}

fn main() {
    let owned_suffix = String::from("-suffix");
    let node = Suffixer {
        suffix: owned_suffix.as_str(),
    };
    let output = block_on(async {
        let runtime = Runtime::new();
        runtime
            .execute(
                |flow, amount| flow.chain(suffixing(&node, amount)),
                Amount(7),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, "7-suffix", "non-static node dependency survives");

    println!("we_t02-positive-ok");
}
