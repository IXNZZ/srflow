//! T03 迁移自 `tests/ui/we_t02_nonstatic_node_borrow.rs`（WE-T02 B03 正例）：Fragment 保持
//! **非 static** Node 依赖的外部样本在 T03 契约下仍然成立。
//!
//! `Suffixer<'a>` 借用一个只在 main 局部存活的字符串，因此 Node 配置类型本身不是 `'static`。
//! 它仍可作为 `&'n Suffixer<'a>`（`'a: 'n`）经 Fragment 登记进 Chain，证明公开 Fragment 只需
//! 定义期 Node 句柄的借用寿命，不额外要求配置 `'static`。
//!
//! T03 迁移点（只改 Root 输出声明与显式连接名）：fragment 内旧两参 `then(node, refs)` 改为
//! `then_with_output(node, refs)`；Root 定义闭包固定返回 `()`，输出改由 `flow.output_with(refs)`
//! 声明；owned Out 由 `execute::<_, Text, _>` 显式给出。Node、借用关系与断言 `"7-suffix"` 不变。
//!
//! 由主包真实 rlib 以外部消费者身份编译运行；文件内联最小 executor 取代原 `futures::executor::block_on`。
//!
//! 期望：编译成功，运行退出码 0，断言 `"7-suffix"`，stdout 含 `we_t02-positive-ok`。

use std::future::Future;
use std::task::{Context, Poll, Waker};

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
) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |sub| {
        let out_refs = sub.then_with_output(node, input);
        sub.output_with(out_refs);
    }
}

/// 最小 executor：把 `Runtime::execute` 的 Future 驱动到完成。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn main() {
    let owned_suffix = String::from("-suffix");
    let node = Suffixer {
        suffix: owned_suffix.as_str(),
    };
    let output = block_on(async {
        let runtime = Runtime::new();
        runtime
            .execute::<_, Text, _>(
                |flow, amount| {
                    let refs: Ref<Text> = flow.chain_output(suffixing(&node, amount));
                    flow.output_with(refs);
                },
                Amount(7),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, "7-suffix", "non-static node dependency survives");

    println!("we_t02-positive-ok");
}
