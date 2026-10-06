//! V21-12 外部负例：普通函数不存在显式 Unit 输出签名（无对应 `BuildSite` impl）。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_unit_signature_is_not_expressible.rs -o /tmp/v21_12_unit_sig.rmeta
//! ```
//! 预期失败：E0277——`SyncFnSig<(u32,), Unit>` 对函数 item 没有构建实现；
//! 推断形态 `then(unit_fn, x)` 才是 Definition 构建期返回 `UnsupportedFunctionUnitOutput`。

use srflow::{BodyError, Data, FlowBuilder, SyncFnSig, Unit};

fn unit_fn(_a: &u32) -> Result<(), BodyError> {
    Ok(())
}

fn main() {
    let (mut flow, input) = FlowBuilder::<u32>::start().expect("start");
    let _ = flow.then::<_, SyncFnSig<(u32,), Unit>, _>(
        unit_fn as fn(&u32) -> Result<(), BodyError>,
        input,
    );
    let _ = std::mem::size_of::<Data<u32>>();
}
