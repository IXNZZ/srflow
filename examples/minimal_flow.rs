//! 最小 Flow 示例：函数 Node + 结构体 Node + 一次 Root Execution（新契约公开 API）。
//!
//! 运行：`cargo run --offline --example minimal_flow`
//!
//! 业务 Data `Order` 用 `#[derive(Data)]` 声明，刻意**不派生 `Clone`**：新契约下 Node 只
//! 接收声明输入的只读 [`Query`]，执行链自己决定是否需要复制，业务类型保持普通所有权语义。
//! `validate`（函数 Node）与 `Settle`（结构体 [`Node`]）演示两种 Node 形态；`settle` 的业务
//! 失败经 [`RunError::Body`] 保留原动态对象，且**不返回部分输出**。
//!
//! 历史对照：旧 `examples/minimal_flow.rs` 描述的是 `srflow::legacy` 的 `FlowBuilder`／完成态
//! `Flow`／`Runtime::execute(&flow, input)` 模型（SPEC §1 明确其不适用于新模型）。本文件是同一
//! 教学意图（最小接线 + 一次执行 + 可观察失败）在新冻结面下的改写。

use futures::executor::block_on;
use std::fmt;

use srflow::{BodyError, Data, Node, Query, RunError, Runtime};

/// 业务 Data：只声明 `Data`，不派生 `Clone`。
#[derive(Data)]
struct Order {
    id: u32,
    amount: u32,
}

#[derive(Data, Debug, PartialEq, Eq)]
struct Receipt {
    id: u32,
    total: u32,
}

/// 明确失败理由：携带动态 Error 对象，经 `RunError::Body` 的 source chain 保留。
#[derive(Debug)]
struct InvalidOrder {
    id: u32,
}

impl fmt::Display for InvalidOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "order {} has a non-positive amount", self.id)
    }
}

impl std::error::Error for InvalidOrder {}

/// 函数 Node：自然异步签名，输入只读借用，返回新的 owned Data。
async fn validate(query: Query<&Order>) -> Result<Order, BodyError> {
    let order = query.get();
    if order.amount == 0 {
        return Err(BodyError::fail(InvalidOrder { id: order.id }));
    }
    Ok(Order {
        id: order.id,
        amount: order.amount,
    })
}

/// 结构体 Node：显式 `Input`／`Output` 协议；与函数 Node 可混用。
struct Settle;

impl Node for Settle {
    type Input = Order;
    type Output = Receipt;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Receipt, BodyError> {
        let order = query.get();
        Ok(Receipt {
            id: order.id,
            total: order.amount * 2,
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new();

    // 单输入 Root：函数 Node 与结构体 Node 依次读取前一步产物。
    let receipt: Receipt = runtime
        .execute(
            |flow, order| {
                let validated = flow.then(validate, order);
                flow.then(Settle, validated)
            },
            Order { id: 7, amount: 21 },
        )
        .await?;
    assert_eq!(receipt, Receipt { id: 7, total: 42 });
    println!("receipt: {receipt:?}");

    // 失败不返回部分输出：业务错误经公开分类与 source chain 可观察。
    match runtime
        .execute(
            |flow, order| {
                let validated = flow.then(validate, order);
                flow.then(Settle, validated)
            },
            Order { id: 8, amount: 0 },
        )
        .await
    {
        Ok(_) => return Err("invalid order unexpectedly succeeded".into()),
        Err(RunError::Body(failure)) => println!("rejected: {failure}"),
        Err(other) => return Err(format!("unexpected error: {other}").into()),
    }

    Ok(())
}
