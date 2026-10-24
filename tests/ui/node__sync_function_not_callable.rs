//! T03 迁移自 `tests/ui/we_t02_sync_result_function.rs`（WE-T02 B22 负例）：同步 `Result`
//! 函数仍不能替代异步 Node 协议传给 `then`。
//!
//! Function Node 要求满足相同异步签名的 `AsyncFn`（SPEC §5.1）；普通同步
//! `fn(Query<&A>) -> Result<O, BodyError>` 不符合该协议。
//!
//! T03 迁移点：旧两参 `then(sync_double, amount)` 更名并取输出为
//! `then_with_output(sync_double, amount)`——输入仍由调用点的 `Ref<Amount>` 显式给出，拒绝
//! 点仍是这次连接本身。owned Out 由 `execute::<_, Amount, _>` 显式给出，闭包尾仍是该连接。
//!
//! 期望主诊断：E0277（函数项未实现 `Callable<_, _>`），落在本夹具连接行。
use srflow::{BodyError, Data, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

/// 同步函数：签名形状正确，但不是异步协议。
fn sync_double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, Amount, _>(
        |flow, amount| {
            // 期望主诊断：E0277——同步函数项不实现异步调用协议。
            flow.then_with_output(sync_double, amount)
        },
        Amount(21),
    );
}
