//! A04 类型分离负例：`ScopeId` 不能当作 `DataId` 使用。
//!
//! `DataId` 与 `ScopeId` 是两个不同的内部类型，混用应在类型位置编译失败（E0308）：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/a04_scope_id_is_not_data_id.rs \
//!   -o /tmp/a04_scope_id_is_not_data_id.rmeta
//! ```
//!
//! 该负例是"类型不符"而非"可见性缺失"，因此用 `#[path]` 装配真实源码，
//! 让 `ScopeId` 在夹具内可见，确保失败原因确实来自类型。

#[path = "../../src/core/mod.rs"]
mod core;

use core::identity::{DataId, ExecutionIdentity, ScopeIdAllocator};

fn takes_data_id(_id: &DataId) {}

fn main() {
    let scopes = ScopeIdAllocator::new(ExecutionIdentity::new());
    let scope_id = scopes.allocate().unwrap();

    // 预期 E0308：传入 `&ScopeId`，需要 `&DataId`。
    takes_data_id(&scope_id);
}