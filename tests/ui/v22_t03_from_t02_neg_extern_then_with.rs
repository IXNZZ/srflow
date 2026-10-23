//! T03 外部公开正例：原 T02「`Flow::then_with` 未公开（E0624）」判据退场，改为
//! 「显式连接入口 `Flow::then_with` 已公开且可自然调用」的运行正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_then_with.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_then_with
//! /tmp/v22_t03_from_t02_neg_extern_then_with
//! ```
//!
//! 预期：编译成功、退出码 0、stdout 含标记 `t03-then-with-ok`。
//!
//! T03 迁移：外部调用点由「不可见」变为可见，调用形状保持显式连接
//! `flow.then_with(add_one, input)`（`input` 为 Root 输入 `Ref<u32>`）；普通形式不返回句柄，
//! Root 输出改由 `flow.output::<u32>()` 声明，并以 `execute::<u32, u32, _>` 显式给出 owned Out。
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
        |flow: &mut Flow<'_>, input: Ref<u32>| {
            // 显式连接：输入 Shape 由 `input` 的静态类型给出，不做“最近来源”解析。
            flow.then_with(add_one, input);
            flow.output::<u32>();
        },
        41u32,
    ))
    .unwrap();

    assert_eq!(output, 42, "then_with 显式连接必须产出 owned Out");
    println!("t03-then-with-ok");
}
