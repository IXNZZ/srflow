//! WE-T02 B01 正例：SPEC §14 最小示例逐字（仅把 Probe 包名换成正式包名 `srflow`）。
//!
//! 由**主包真实构建产物**编译：`--extern srflow=target/debug/libsrflow.rlib`。
//! 本文件不装配任何内部模块，只以外部消费者身份使用 crate 根公开入口。
//!
//! ```sh
//! rustc --edition=2024 tests/ui/we_t02_spec_minimal_example.rs \
//!   -o /tmp/we_t02_spec_minimal_example \
//!   --extern srflow=target/debug/libsrflow.rlib \
//!   --extern futures=target/debug/deps/libfutures-<hash>.rlib \
//!   -L dependency=target/debug/deps
//! /tmp/we_t02_spec_minimal_example
//! ```
//!
//! 预期：编译成功，运行退出码 0，断言 `output.0 == 43`，stdout 含
//! `we_t02-positive-ok`（驱动标记行，不属于 SPEC §14 原文）。

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

    async fn run(
        &self,
        query: Query<&Self::Input>,
    ) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let output = futures::executor::block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime.execute(
            |flow, amount| {
                let doubled = flow.then(double, amount);
                flow.chain(|sub| sub.then(&add, doubled))
            },
            Amount(21),
        ).await
    }).unwrap();
    assert_eq!(output.0, 43);

    // 以下为驱动标记行；SPEC §14 原文到此为止。
    println!("we_t02-positive-ok");
}
