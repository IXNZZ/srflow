//! T03 外部公开正例：原 T02「`Flow::provide` 未公开（E0624）」判据退场，改为
//! 「同步 Factory 普通形式 `Flow::provide` 已公开且可自然调用」的运行正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_provide.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_provide
//! /tmp/v22_t03_from_t02_neg_extern_provide
//! ```
//!
//! 预期：编译成功、退出码 0、stdout 含标记 `t03-provide-ok`。
//!
//! T03 迁移：调用形状保持 `flow.provide(|| 1u32)`（零输入 Factory → 一步 Node）；普通形式不
//! 返回句柄，Root 输出改由 `flow.output::<u32>()` 声明，并以 `execute::<(), u32, _>` 显式给出
//! owned Out。原“provide 未公开”的私有性判据结束，转为公开运行的正式方法证据。
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
            // 同步 Factory 的普通形式：只在真实执行该 Step 时调用，不要求 'static／Send。
            flow.provide(|| 41u32);
            flow.output::<u32>();
        },
        (),
    ))
    .unwrap();

    assert_eq!(output, 41, "provide Factory 必须产出 owned Out");
    println!("t03-provide-ok");
}
