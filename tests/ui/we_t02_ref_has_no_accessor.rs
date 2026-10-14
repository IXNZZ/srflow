//! WE-T02 B22 负例：`Ref<T>` 不提供读值／身份／字段入口。
//!
//! `Ref<T>` 只是强类型逻辑依赖身份（SPEC §6）：没有 `get`／`borrow`、没有字段投影、
//! 没有 `DataId`／`ScopeId`／位置等运行期身份访问器。本夹具以泛型函数固定两处调用，
//! 二者都不存在方法。
//!
//! 期望主诊断：E0599（`Ref<T>` 上无 `get`／`data_id` 方法）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

/// Ref 无读值／身份入口：以下两行都必须被拒绝。
fn probe<T: Data>(value: Ref<T>) {
    let _ = value.get();
    let _ = value.data_id();
}

fn main() {
    let _ = probe::<Amount>;
}
