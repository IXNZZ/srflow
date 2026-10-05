//! K23 负例：Root 输入类型／arity 错接在编译期拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/k02_wrong_root_input_type.rs -o /tmp/k02.rmeta
//! ```
//! 预期：E0308／E0277——单份 tuple 输入不能被拆成两个输入。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::flow::{Flow, FlowBuilder};
use core::runtime::Runtime;
use core::signature::{Data, SyncFnSig};

struct Left(u32);
struct Right(u32);

fn pair(value: &(Left, Right)) -> Result<u32, BodyError> {
    Ok(value.0.0 + value.1.0)
}

fn main() {
    let (mut flow, handle) = FlowBuilder::<((Left, Right),)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, SyncFnSig<((Left, Right),), Data<u32>>, _>(
            pair as fn(&(Left, Right)) -> Result<u32, BodyError>,
            handle,
        )
        .expect("then");
    let root: Flow<((Left, Right),), Data<u32>> = flow.finish::<Data<u32>, _>(out).expect("finish");
    // 期望：E0308／E0277——一份 tuple Data 不能拆成两个输入。
    let _ = Runtime::execute::<_, _, Data<u32>>(&root, (Left(1), Right(2)));
}
