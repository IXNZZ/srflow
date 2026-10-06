//! K23 负例：Root owned 输出形态错接在编译期拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/k03_wrong_owned_output_type.rs -o /tmp/k03.rmeta
//! ```
//! 预期：E0308——`Data<u32>` 的 owned 结果是 `u32`，不是 `u64`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::flow::{Flow, FlowBuilder};
use core::runtime::Runtime;
use core::signature::{Data, SyncFnSig};

struct Left(u32);

fn to_u32(left: &Left) -> Result<u32, BodyError> {
    Ok(left.0)
}

fn main() {
    let (mut flow, handle) = FlowBuilder::<(Left,)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            to_u32 as fn(&Left) -> Result<u32, BodyError>,
            handle,
        )
        .expect("then");
    let root: Flow<(Left,), Data<u32>> = flow.finish::<Data<u32>, _>(out).expect("finish");
    // 期望：E0308——返回值类型不符。
    let _: u64 = test_support_drive(Runtime::execute::<_, _, Data<u32>>(&root, (Left(1),)));
}

fn test_support_drive<F: std::future::Future>(_future: F) -> F::Output {
    unimplemented!("compile-time sample only")
}
