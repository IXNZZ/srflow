//! 最小 Flow 示例：同步／异步函数 Node + 一次 Root Execution。
//!
//! 运行：`cargo run --offline --example minimal_flow`
//!
//! 业务 Data `Order` 刻意不实现 `Clone`：接线与执行只借用 `&A`、返回新的 owned 值，
//! 复制要求由业务自己决定。

use futures::executor::block_on;
use srflow::{BodyError, Data, DataRef, Flow, FlowBuilder, Runtime};

#[derive(Debug, PartialEq, Eq)]
struct Order {
    id: u32,
    amount: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Receipt {
    id: u32,
    total: u32,
}

/// 普通同步函数 Node：输入只读借用，返回新的 owned Data。
fn validate(order: &Order) -> Result<Order, BodyError> {
    if order.amount == 0 {
        return Err(BodyError::new("order amount must be positive"));
    }
    Ok(Order {
        id: order.id,
        amount: order.amount,
    })
}

/// 异步函数 Node：可与同步 Node 混用；调用方决定 executor。
async fn settle(order: &Order) -> Result<Receipt, BodyError> {
    Ok(Receipt {
        id: order.id,
        total: order.amount * 2,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (mut body, order) = FlowBuilder::<(Order,)>::start()?;
    let validated: DataRef<Order> = body.then(validate, order)?;
    let receipt: DataRef<Receipt> = body.then(settle, validated)?;
    let flow: Flow<(Order,), Data<Receipt>> = body.finish(receipt)?;

    let receipt = Runtime::execute(&flow, (Order { id: 7, amount: 21 },)).await?;
    assert_eq!(receipt, Receipt { id: 7, total: 42 });
    println!("receipt: {receipt:?}");

    // 失败不返回部分输出：业务错误经公开分类可观察。
    match Runtime::execute(&flow, (Order { id: 8, amount: 0 },)).await {
        Ok(_) => return Err("invalid order unexpectedly succeeded".into()),
        Err(error) => println!("rejected: {error}"),
    }

    Ok(())
}
