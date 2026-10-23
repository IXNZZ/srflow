//! WE-T01 A01 负例 6：`Ref<()>` 不可构造。
//!
//! 意图：`Ref<T: Data>` 与 `T: Data`（`Any`／`'static` 空 marker）已排除 unit，
//! 声明层不产生 `Ref<()>`；显式 unit 是零输出，不是 `Ref<()>` 位置。
//!
//! 预期主诊断：E0277（`()` 未实现 `Data`，`Ref<()>` 不是可表达的类型），
//! 落在本文件末尾 `take_unit_ref` 的签名（`Ref<()>` 参数）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_ref_of_unit.rs -o /tmp/v22_t03_from_we_t01_ref_of_unit.rmeta
//! ```
//!
//! T03 迁移：旧两参 `then(load, ())`／`then(sink, produced)` 改为
//! `then_with_output`；Root 闭包固定返回 `()`，原尾值 `amount`（`Ref<Amount>`）改由
//! `output_with` 声明，并补 owned Out 泛型 `Amount`。`Ref<()>` 的拒绝点与目的不变。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;
use crate::core::engine::error::BodyError;
use crate::core::engine::shape::{Query, Ref};

struct Amount(u32);

impl Data for Amount {}

/// 零输入 Node：Result 的成功类型是 `()`。
async fn load(query: Query<()>) -> Result<Amount, BodyError> {
    let _ = query.get();
    Ok(Amount(0))
}

/// 零输出 Node：Result 的成功类型是 `()`。
async fn sink(query: Query<&Amount>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

fn main() {
    let _ = build_definition::<Amount, Amount, _>(|flow, amount| {
        let produced = flow.then_with_output(load, ());
        // 零输出 Node 的结果是显式 `()`：已完成、零输出，不是 `Ref<()>` 位置。
        let _unit: () = flow.then_with_output(sink, produced);
        flow.output_with(amount);
    });
}

// 期望主诊断：E0277——`()` 不是 Data，`Ref<()>` 不是可表达的类型。
// 只声明签名、不构造也不求值任何值；函数签名的类型边界会被编译期检查。
fn take_unit_ref(_: Ref<()>) {}
