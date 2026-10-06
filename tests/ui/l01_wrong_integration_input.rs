//! L23 负例：整合定义的 Root 输入形态错接在编译期拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/l01_wrong_integration_input.rs -o /tmp/l01.rmeta
//! ```
//! 预期：E0308／E0277——两份 Root 输入（`Vec<State>`、`Rules`）不可缺少。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::each::{Each, EachBuilder, EachOnly};
use core::flow::{Flow, FlowBuilder};
use core::runtime::Runtime;
use core::signature::{Data, OrchSig, SyncFnSig};

struct State(u32);
struct Rules(u32);
struct ItemResult(u32);

fn advance(state: &State) -> Result<State, BodyError> {
    Ok(State(state.0 + 1))
}

fn result(state: &State) -> Result<ItemResult, BodyError> {
    Ok(ItemResult(state.0))
}

fn main() {
    let (mut body, state) = FlowBuilder::<(State,)>::start().expect("body");
    let progressed: DataRef<State> = body
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            advance as fn(&State) -> Result<State, BodyError>,
            state,
        )
        .expect("advance step");
    let out: DataRef<ItemResult> = body
        .then::<_, SyncFnSig<(State,), Data<ItemResult>>, _>(
            result as fn(&State) -> Result<ItemResult, BodyError>,
            progressed,
        )
        .expect("result step");
    let body: Flow<(State,), Data<ItemResult>> = body.finish::<Data<ItemResult>, _>(out).expect("finish");
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(body)
        .expect("each body");
    let each: Each<EachOnly<State>, ItemResult> = each.finish().expect("each finish");
    let (mut root, states) = FlowBuilder::<(Vec<State>, Rules)>::start().expect("root");
    let collected: DataRef<Vec<ItemResult>> = root
        .then::<_, OrchSig<(Vec<State>, Rules), Data<Vec<ItemResult>>>, _>(each, states)
        .expect("each step");
    let root: Flow<(Vec<State>, Rules), Data<Vec<ItemResult>>> =
        root.finish::<Data<Vec<ItemResult>>, _>(collected).expect("root finish");
    // 期望：E0308／E0277——缺少 `Rules` 输入。
    let _ = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(&root, (vec![State(0)],));
}
