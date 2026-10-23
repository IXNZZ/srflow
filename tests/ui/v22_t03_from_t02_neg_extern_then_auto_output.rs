//! T03 外部公开正例：原 T02「`Flow::then_auto_output` 未公开（E0624）」判据退场，改为
//! 「自动输入取输出入口 `Flow::then_output` 已公开且可自然调用」的运行正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_then_auto_output.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_then_auto_output
//! /tmp/v22_t03_from_t02_neg_extern_then_auto_output
//! ```
//!
//! 预期：编译成功、退出码 0、stdout 含标记 `t03-then-auto-output-ok`。
//!
//! T03 迁移：`let _ = flow.then_auto_output(&add_one)` → `let produced = flow.then_output(add_one)`
//! 并交给 `flow.output_with(produced)` 声明 Root 输出；`execute::<u32, u32, _>` 显式给出 owned Out。
//! 原“外部可见性”目的由 E0624 拒绝证据转为运行正例证据。
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
            // 取输出形式：拿到本次 Node Output 对应的 `Ref<u32>` 句柄。
            let produced: Ref<u32> = flow.then_output(add_one);
            flow.output_with(produced);
        },
        41u32,
    ))
    .unwrap();

    assert_eq!(output, 42, "then_output 句柄必须可用于 Root 输出声明");
    println!("t03-then-auto-output-ok");
}
