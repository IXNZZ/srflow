//! C01：完全没有 owned Out 上下文时，`Runtime::execute` 的 `Out` 不能从调用侧推导。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/builder__missing_out_context.rs -o /tmp/x.rmeta
//! ```
//!
//! 预期：编译拒绝（类型参数 `Out` 需要注解）。正例见迁移后的各集成/示例调用：
//! 局部 owned 注解、外层 `Result<Out, RunError>`、显式 `execute::<_, Out, _>` 三种上下文。
use srflow::{BodyError, Data, Query, Runtime};

struct Amount(u32);
impl Data for Amount {}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    // 既没有局部 owned 注解，也没有显式泛型：`Out` 无法推断。
    let _future = runtime.execute( // 这里没有任何 owned Out 上下文
        |flow, input| {
            let doubled = flow.then_with_output(double, input);
            flow.output_with(doubled);
        },
        Amount(21),
    );
}
