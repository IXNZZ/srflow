//! T03 迁移自 `tests/ui/we_t02_spec_minimal_example.rs`（WE-T02 B01 正例）：SPEC §14 最小示例
//! 在 T03 公开契约下的等价写法。
//!
//! 原判据（逐字保留）：`Runtime::new()` 建执行入口，定义闭包内 `then` + `chain` 都不要求额外
//! 注解，Node 类型与接线顺序不变，断言 `output.0 == 43`。
//!
//! T03 迁移点（只改 Root 输出声明与显式连接名，不改 Node／接线／断言）：
//! - 显式连接由旧两参 `then(node, refs)` 更名为 `then_with_output(node, refs)`；
//! - Root 定义闭包固定返回 `()`，业务输出改由 `flow.output_with(refs)` 声明；
//! - owned Out 由 `execute::<_, Amount, _>` 显式给出。
//!
//! 由主包真实构建产物以外部消费者身份编译运行（`--extern srflow=target/debug/libsrflow.rlib
//! -L dependency=target/debug/deps`）；本夹具不使用 `#[path]` 装配内部模块。原夹具以
//! `futures::executor::block_on` 驱动，此处改为文件内联的最小 executor（相同驱动语义：
//! poll 到 `Ready` 取输出），避免夹具依赖 dev-dependency。
//!
//! 期望：编译成功，运行退出码 0，断言 43，stdout 含 `we_t02-positive-ok`（原标记不变）。

use std::future::Future;
use std::task::{Context, Poll, Waker};

use srflow::{BodyError, Data, Node, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

struct AddOne;
impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
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
    let output = block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime
            .execute::<_, Amount, _>(
                |flow, amount| {
                    // 21 -> double -> 42
                    let doubled = flow.then_with_output(double, amount);
                    // 42 -> (chain: +1) -> 43
                    let plus_one = flow.chain(|sub| sub.then_with_output(&add, doubled));
                    flow.output_with(plus_one);
                },
                Amount(21),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, 43);

    println!("we_t02-positive-ok");
}
