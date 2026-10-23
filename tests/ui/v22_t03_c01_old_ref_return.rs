//! C01：Root 闭包固定返回 `()`；旧写法（返回 RefShape）不被接受。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_c01_old_ref_return.rs -o /tmp/x.rmeta
//! ```
//!
//! 预期：编译拒绝——闭包尾表达式 `produced` 是 `Ref<Amount>`，而 `F: FnOnce(&mut Flow<'n>, In::Refs)`
//! 要求返回 `()`。正确写法是 `flow.output_with(produced);` 并让闭包自然返回 unit。

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
            let produced = flow.then_with_output(double, input);
            produced // 旧写法：闭包返回 Ref<Amount>；新契约要求 unit
        },
        Amount(21),
    );
}
