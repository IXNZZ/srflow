//! WE-T02 B22 负例：Node 输入 arity 与声明不符（只给 1 个 Ref，声明需要 2 个）。
//!
//! 双输入函数 Node 声明 `Query<(&Amount, &Other)>`（即 Input 为 `(Amount, Other)`），
//! 却只收到 `Ref<Amount>`；视图 tuple 形状与单输入借用视图不匹配。
//!
//! 期望主诊断：E0271（借用视图 arity 不匹配）。

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
    let _ = runtime.execute(
        |flow, amount| {
            // 期望主诊断：E0271——2 输入声明只给了 1 个 Ref。
            flow.then(add, amount)
        },
        Amount(21),
    );
}
