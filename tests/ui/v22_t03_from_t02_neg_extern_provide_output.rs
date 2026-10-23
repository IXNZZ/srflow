//! T03 外部公开正例：原 T02「`Flow::provide_output` 未公开（E0624）」判据退场，改为
//! 「同步 Factory 取 Ref 形式 `Flow::provide_output` 已公开且可自然调用」的运行正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_provide_output.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_provide_output
//! /tmp/v22_t03_from_t02_neg_extern_provide_output
//! ```
//!
//! 预期：编译成功、退出码 0、stdout 含标记 `t03-provide-output-ok`。
//!
//! T03 迁移：`let _ = flow.provide_output(|| 1u32)` → `let produced: Ref<u32> = flow.provide_output(|| 41u32)`
//! 并交给 `flow.output_with(produced)` 声明 Root 输出（不再丢弃句柄）；`execute::<(), u32, _>`
//! 显式给出 owned Out。
//!
//! 本夹具不依赖 `futures`（ext 装配只给 `--extern srflow`），自带最小 noop-waker 执行器。

use srflow::{Flow, Ref, Runtime};

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
    let output: u32 = block_on(runtime.execute::<(), u32, _>(
        |flow: &mut Flow<'_>, _input: ()| {
            // 取 Ref 形式：返回该 Provide Step 的输出句柄，供显式连线或 Root 输出声明使用。
            let produced: Ref<u32> = flow.provide_output(|| 41u32);
            flow.output_with(produced);
        },
        (),
    ))
    .unwrap();

    assert_eq!(output, 41, "provide_output 句柄必须可用于 Root 输出声明");
    println!("t03-provide-output-ok");
}
