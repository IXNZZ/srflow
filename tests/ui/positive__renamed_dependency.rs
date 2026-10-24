// we-t06-probe-extern-alias: srflow_renamed
//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_p09_renamed_dependency.rs`）：重命名依赖的公开执行面。
//!
//! 原目的（X24/X32／C13／C15）：公开执行面（`Runtime`／`Flow::then_with_output`／`Query`／
//! `Ref`／`Data`）在**依赖重命名**后正常编译运行，不暴露任何内部许可；断言 `output = 42`。
//!
//! 迁移手法：T03 驱动固定以 `--extern srflow=<rlib>` 装配（不接受 `--extern srflow_renamed=…`），
//! 因此重命名改在夹具内完成：`extern crate srflow as srflow_renamed;` 后再以 `srflow_renamed::`
//! 命名空间引用全部公开面，等价保持「消费者以非默认依赖名访问公开面」的原目的。Root 闭包改为
//! 固定返回 `()` 并经 `output_with` 声明 owned `Out = Amount`。
//!
//! 本夹具按 SPEC §3.1 **手写 `impl Data`**：`#[derive(Data)]` 经 `proc-macro-crate` 从消费者
//! 自己的 `Cargo.toml` 解析依赖名，而 raw `rustc` 机制没有消费者清单；**派生宏＋重命名的组合**
//! 由独立 Cargo 消费者（296 行 `consumer/src/main.rs`，主协调者承接）承担。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-positive-ok`。
extern crate srflow as srflow_renamed;

use futures::executor::block_on;
use srflow_renamed::{BodyError, Data, Query, Ref, Runtime};

struct Amount(u32);

impl Data for Amount {}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    let output: Amount = block_on(runtime.execute(
        |flow: &mut srflow_renamed::Flow<'_>, amount: Ref<Amount>| {
            let out_refs = flow.then_with_output(double, amount);
            flow.output_with(out_refs);
        },
        Amount(21),
    ))
    .expect("renamed dependency still exposes the public surface");
    assert_eq!(output.0, 42);
    println!("we_t06 p09 output={}", output.0);
    println!("we_t06-positive-ok");
}
