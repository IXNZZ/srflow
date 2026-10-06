//! V21-12 外部负例：接线输入类型与函数签名不符，编译期拒绝。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_wrong_wiring_type.rs -o /tmp/v21_12_wrong_wiring.rmeta
//! ```
//! 预期失败：E0277／E0308——`DataRef<u32>` 不满足 `DataRef<u64>` 位置。

use srflow::{BodyError, Data, FlowBuilder, SyncFnSig};

fn main() {
    let (mut flow, input) = FlowBuilder::<u64>::start().expect("start");
    let _ = flow.then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
        (|v: &u32| Ok(*v)) as fn(&u32) -> Result<u32, BodyError>,
        input,
    );
}
