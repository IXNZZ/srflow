//! V21-12 外部负例：完成的 Flow 不能按不匹配的 Root 形状执行。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_wrong_root_shape_is_not_executable.rs -o /tmp/v21_12_wrong_root.rmeta
//! ```
//! 预期失败：E0308——双输入 tuple 与单输入 Root 形状不匹配（`OrchCall` 无对应实现）。

use srflow::{Data, Flow, FlowBuilder, Runtime, SyncFnSig};

fn main() {
    let (mut flow, input) = FlowBuilder::<u32>::start().expect("start");
    let out = flow
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            (|v: &u32| Ok(*v)) as fn(&u32) -> Result<u32, srflow::BodyError>,
            input,
        )
        .expect("step");
    let flow: Flow<(u32,), Data<u32>> = flow.finish::<Data<u32>, _>(out).expect("finish");
    let _ = Runtime::execute(&flow, (1u32, 2u32));
}
