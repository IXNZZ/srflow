//! T03 外部公开正例：原 T02「`Flow::then_auto` 未公开（E0624）」判据退场，改为
//! 「自动输入入口 `Flow::then` 已公开且可自然调用」的运行正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_then_auto.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_then_auto
//! /tmp/v22_t03_from_t02_neg_extern_then_auto
//! ```
//!
//! 预期：编译成功、退出码 0、stdout 含标记 `t03-then-auto-ok`。
//!
//! T03 迁移：`flow.then_auto(&add_one)` → `flow.then(add_one)`（自动输入解析）；Root 闭包固定
//! 返回 `()`，原尾值改由 `flow.output::<u32>()` 声明，并以 `execute::<u32, u32, _>` 显式给出
//! owned Out。原“外部可见性”目的由 E0624 拒绝证据转为运行正例证据：同一调用点现在可达并真实执行。
//!
//! 本夹具不依赖 `futures`（ext 装配只给 `--extern srflow`），自带最小 noop-waker 执行器。

use srflow::{BodyError, Flow, Query, Ref, Runtime};

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

/// 最小执行器：`Runtime::execute` 不绑定外部运行时，用 noop waker 轮询到就绪即可。
fn block_on<F: Future>(future: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::hint::spin_loop();
    }
}

fn main() {
    let runtime = Runtime::new();
    let output: u32 = block_on(runtime.execute::<u32, u32, _>(
        |flow: &mut Flow<'_>, _input: Ref<u32>| {
            // 自动输入入口：输入依赖由类型系统连接（此处为 Root 输入槽），无需显式 refs。
            flow.then(add_one);
            flow.output::<u32>();
        },
        41u32,
    ))
    .unwrap();

    assert_eq!(output, 42, "then 自动链结必须产出 owned Out");
    println!("t03-then-auto-ok");
}
