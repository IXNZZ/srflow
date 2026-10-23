//! T05 迁移自 `v22_t03_from_we_t05_p39_renamed_dependency.rs`（原 WE-T05 P39 正例，公开部分）：
//! **重命名依赖**下的外部消费者用法。Each 契约改为 item Body 单参数：item 由
//! `sub.ref_of::<Amount>()` 取回，聚合输出经 `flow.each_output::<Amount, Ref<Vec<Amount>>, _>`
//! 显式声明；Root 输出仍由 `Flow::output_with` 声明（定义闭包固定返回 `()`）。
//!
//! **迁移说明（对齐 T03/T05 驱动能力）**：原夹具首行指令
//! `we-t05-probe-extern-alias: srflow_renamed` 由 WE-T05 自带驱动解析，以
//! `--extern srflow_renamed=<rlib>` 编译；本驱动只装配 `--extern srflow=<rlib>`，不认识该指令。
//! 本夹具因此改为在**源码层**重命名外部 crate：`extern crate srflow as srflow_renamed;`，再以
//! `use srflow_renamed::{…}` 使用公开面。这仍然验证“crate 以**非 `srflow` 标识**被消费者引用时，
//! 公开执行面（`Runtime`／`Flow::each_output`／`Query`／`Ref`）照常可用”这同一目的。
//!
//! **为什么这里手写 `impl Data`**：`#[derive(Data)]` 经 `proc-macro-crate` 从**消费者自己的
//! Cargo.toml** 解析依赖名，而本驱动用 raw `rustc` 直接编译夹具、不带消费者清单，解析只能回退到
//! `::srflow`，因此“派生宏 + 重命名依赖”无法在本机制内表达。SPEC §3.1 明文允许手写实现，
//! 故本夹具用等价手写实现验证**重命名引用下的公开执行面**。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

extern crate srflow as srflow_renamed;

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

    let doubled = block_on(runtime.execute::<_, Vec<Amount>, _>(
        |flow: &mut srflow_renamed::Flow<'_>, items: Ref<Vec<Amount>>| {
            let out_refs =
                flow.each_output::<Amount, Ref<Vec<Amount>>, _>(items, |sub| {
                    let item = sub.ref_of::<Amount>();
                    let doubled = sub.then_with_output(&double, item);
                    sub.output_with(doubled);
                });
            flow.output_with(out_refs);
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
