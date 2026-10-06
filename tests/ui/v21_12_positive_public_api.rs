//! V21-12 合法对照：只通过公开导入编译并运行（由驱动用 rlib + futures 编译执行）。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --extern srflow=target/debug/libsrflow.rlib \
//!   --extern futures=target/debug/deps/libfutures-<hash>.rlib \
//!   tests/ui/v21_12_positive_public_api.rs -o /tmp/v21_12_positive
//! /tmp/v21_12_positive
//! ```
//! 预期输出：`ok: 42`。

use futures::executor::block_on;
use srflow::{BodyError, Data, DataRef, Flow, FlowBuilder, Runtime, SyncFnSig};

#[derive(Debug, PartialEq, Eq)]
struct Value(u32);

fn double(value: &Value) -> Result<Value, BodyError> {
    Ok(Value(value.0 * 2))
}

fn main() {
    let (mut body, value) = FlowBuilder::<(Value,)>::start().expect("start");
    let doubled: DataRef<Value> = body
        .then::<_, SyncFnSig<(Value,), Data<Value>>, _>(
            double as fn(&Value) -> Result<Value, BodyError>,
            value,
        )
        .expect("step");
    let flow: Flow<(Value,), Data<Value>> = body.finish::<Data<Value>, _>(doubled).expect("finish");
    let out = block_on(Runtime::execute(&flow, (Value(21),))).expect("execute");
    assert_eq!(out, Value(42));
    println!("ok: {}", out.0);
}
