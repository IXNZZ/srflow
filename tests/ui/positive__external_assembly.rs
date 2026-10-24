//! T03 外部装配编译正例：只使用 crate 根公开面，Root 闭包固定返回 `()`。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/positive__external_assembly.rs -o /tmp/v22_t03_from_t02_ok_extern.rmeta
//! ```
//!
//! 预期：编译成功。用于证明 `--extern`／依赖装配有效，后续外部负例的失败不是装配错误。
//!
//! T03 迁移：旧两参 `then(node, refs)` 改为显式取输出的 `then_with_output(node, refs)`；
//! Root 闭包不再返回 RefShape，原尾值（`add_one` 的输出 `Ref<u32>`）改由 `output_with` 声明，
//! 并以 `execute::<(), u32, _>` 显式给出 owned Out。原“只用现有公开 API 即可装配并执行”的目的
//! 与零输入/单槽 Node 形状不变。
use srflow::{BodyError, Flow, Query, Ref, Runtime};

async fn load(query: Query<()>) -> Result<u32, BodyError> {
    let _ = query.get();
    Ok(41)
}

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

/// T03 公开签名：`then_with_output(node, refs) -> Ref<T>` 与 `Flow::output_with` 声明 Root 输出。
fn wire(runtime: &Runtime) {
    let _future = runtime.execute::<(), u32, _>(
        |flow: &mut Flow<'static>, _input: ()| {
            let produced: Ref<u32> = flow.then_with_output(load, ());
            let added: Ref<u32> = flow.then_with_output(add_one, produced);
            flow.output_with(added);
        },
        (),
    );
}

fn main() {
    let _ = wire;
}
