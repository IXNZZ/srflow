//! C01：尾表达式是 `&mut Flow` 时不得被静默丢弃：闭包必须显式声明输出。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/builder__flow_tail_must_not_be_discarded.rs -o /tmp/x.rmeta
//! ```
//!
//! 预期：编译拒绝——闭包返回 `&mut Flow<'_>` 而不是 `()`。正确写法是在末尾写
//! `flow.output_with(refs);`（带分号），既不丢登记也不丢声明。
use srflow::{BodyError, Data, Flow, Query, Runtime};

struct Amount(u32);
impl Data for Amount {}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    let _future = runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, input| {
            let _registered = flow.then_with_output(double, input);
            flow // 旧写法：尾表达式是 &mut Flow；新契约要求 unit
        },
        Amount(21),
    );
}
