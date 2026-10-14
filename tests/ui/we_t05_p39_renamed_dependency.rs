// we-t05-probe-extern-alias: srflow_renamed
//! WE-T05 P39 正例（公开部分）：**重命名依赖**下的外部消费者用法。
//!
//! 首行指令 `we-t05-probe-extern-alias: srflow_renamed` 告诉驱动：本夹具**不**以 `srflow`
//! 为 extern 名编译，而是以 `--extern srflow_renamed=<真实 rlib>` 编译（模拟下游在
//! `Cargo.toml` 中把依赖重命名，例如 `srflow_renamed = { package = "srflow" }`）。
//!
//! **为什么这里手写 `impl Data`**：`#[derive(Data)]` 经 `proc-macro-crate` 从**消费者自己的
//! Cargo.toml** 解析依赖名（`srflow-data-derive/src/lib.rs::srflow_path`），而本驱动用 raw
//! `rustc` 直接编译夹具、不带消费者清单，解析只能回退到 `::srflow`，因此“派生宏 + 重命名依赖”
//! 无法在本机制内表达（实测为 E0433 `cannot find \`srflow\` in the crate root`）。SPEC §3.1
//! 明文允许手写实现（“手写 `impl Data for Prose {}` 仍合法”），故本夹具用等价手写实现验证
//! **重命名依赖下的公开执行面**（`Runtime`／`Flow::each`／`Query`／`Ref`／`RunError`）；
//! 派生宏与重命名的组合需真实 Cargo 消费者工程，已登记为未在本目录驱动（README §7）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use futures::executor::block_on;
use srflow_renamed::{BodyError, Data, Query, Ref, Runtime};

struct Amount(u32);

// SPEC §3.1：手写实现与派生等价，均通过同名 Public 导出提供 Data。
impl Data for Amount {}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();

    let doubled = block_on(runtime.execute(
        |flow, items: Ref<Vec<Amount>>| {
            flow.each(items, |item, each| each.then(&double, item))
        },
        vec![Amount(1), Amount(2), Amount(3)],
    ))
    .expect("renamed dependency each run");

    assert_eq!(
        doubled.iter().map(|amount| amount.0).collect::<Vec<_>>(),
        vec![2, 4, 6],
        "public each surface works under a renamed dependency"
    );
    println!("we_t05-positive-ok");
}
