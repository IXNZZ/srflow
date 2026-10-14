//! WE-T01 负例 5：`Ref` 不提供任何访问器——调用 `.data_id()` 被拒绝。
//!
//! 意图：`Ref<T>` 只是强类型逻辑位置身份；不提供读值、字段投影、`DataId`／`ScopeId`
//! 访问，也不提供公开位置查询。本夹具同时确认 `.position()` 一类猜测的名称不存在。
//!
//! 预期主诊断：E0599（`Ref<Amount>` 上无 `data_id` 方法），
//! 落在本文件第 27 行 `let _ = amount.data_id();`（2026-10-11 实测）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t01_ref_has_no_accessor.rs -o /tmp/we_t01_ref_has_no_accessor.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;

struct Amount(u32);

impl Data for Amount {}

fn main() {
    let definition = build_definition::<Amount, _, _>(|_flow, amount| {
        // 期望主诊断：E0599——Ref 没有 data_id() 访问器。
        let _ = amount.data_id();
        amount
    });
    let _ = definition;
}
