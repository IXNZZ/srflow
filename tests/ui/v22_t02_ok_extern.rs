//! T02 外部装配编译正例：只使用 crate 根现有公开面。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_ok_extern.rs -o /tmp/v22_t02_ok_extern.rmeta
//! ```
//!
//! 预期：编译成功。用于证明 --extern／依赖装配有效，后续外部负例的失败不是装配错误。

use srflow::{BodyError, Flow, Query, Ref, Runtime};

async fn load(query: Query<()>) -> Result<u32, BodyError> {
    let _ = query.get();
    Ok(41)
}

async fn add_one(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

/// 旧公开签名：`Flow::then(node, refs)` 与 Root 闭包返回 RefShape 仍然可用。
fn wire(runtime: &Runtime) {
    let _future = runtime.execute(
        |flow: &mut Flow<'static>, _input: ()| {
            let produced: Ref<u32> = flow.then(load, ());
            flow.then(add_one, produced)
        },
        (),
    );
}

fn main() {
    let _ = wire;
}
