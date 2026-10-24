//! T03 迁移自 `tests/ui/we_t02_arity_mismatch.rs`（WE-T02 B22 负例）：Node 输入 arity 与声明
//! 不符（只给 1 个 Ref，声明需要 2 个）。
//!
//! 双输入函数 Node 声明 `Query<(&Amount, &Other)>`（即 Input 为 `(Amount, Other)`），
//! 却只收到 `Ref<Amount>`；视图 tuple 形状与单输入借用视图不匹配。
//!
//! T03 迁移点：旧两参 `then(add, amount)` 更名并取输出为 `then_with_output(add, amount)`——
//! 实参仍是同一个单槽 `Ref<Amount>`，拒绝点仍是这次连接本身。owned Out 由
//! `execute::<_, Amount, _>` 显式给出（该函数 Node 声明的输出类型）；Root 闭包固定返回 `()`，
//! 因此连接结果先绑定局部 `refs` 再交给 `flow.output_with(refs)`（求值一次，顺序不变）。
//!
//! 期望主诊断：E0271（借用视图 arity 不匹配），落在本夹具连接行。
use srflow::{BodyError, Data, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Other(u32);

async fn add(query: Query<(&Amount, &Other)>) -> Result<Amount, BodyError> {
    let (a, b) = query.get();
    Ok(Amount(a.0 + b.0))
}

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, Amount, _>(
        |flow, amount| {
            // 期望主诊断：E0271——2 输入声明只给了 1 个 Ref。
            let refs = flow.then_with_output(add, amount);
            flow.output_with(refs);
        },
        Amount(21),
    );
}
