//! WE-T02 B22 负例：同步 `Result` 函数不能替代异步 Node 协议传给 `then`。
//!
//! Function Node 要求满足相同异步签名的 `AsyncFn`（SPEC §5.1）；普通同步
//! `fn(Query<&A>) -> Result<O, BodyError>` 不符合该协议。
//!
//! 期望主诊断：E0277（函数项未实现 `Callable<_, _>`）。

use srflow::{BodyError, Data, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

/// 同步函数：签名形状正确，但不是异步协议。
fn sync_double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute(
        |flow, amount| {
            // 期望主诊断：E0277——同步函数项不实现异步调用协议。
            flow.then(sync_double, amount)
        },
        Amount(21),
    );
}
