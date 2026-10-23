//! WE-T01 正例：装配真实 `src/core`，构建含函数 Node／结构体 Node／Chain 的 Definition。
//!
//! ```sh
//! rustc --edition=2024 tests/ui/v22_t03_from_we_t01_positive_assembly.rs -o /tmp/v22_t03_from_we_t01_positive_assembly
//! /tmp/v22_t03_from_we_t01_positive_assembly
//! ```
//!
//! 预期：编译成功，运行退出码 0，stdout 含标记 `we_t01-positive-ok`。
//!
//! 本夹具只做声明层的同步登记与结构检查，不执行任何业务 Node（无 async 运行时、
//! 无 `futures` 依赖）。它以 `#[path]` 引用真实 `src/core/mod.rs`，不复制替身实现。
//!
//! T03 迁移：旧两参 `then(node, refs)` 改为显式取输出的 `then_with_output(node, refs)`；
//! Root 闭包固定返回 `()`，原尾值（Chain 输出的 `Ref<Amount>`）改由 `output_with` 声明，
//! 并补上准确的 owned Out 泛型 `build_definition::<Amount, Amount, _>`。
//! Chain child 契约保持原样（child 闭包仍返回 RefShape）。断言与阶段不变。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::{StepKind, build_definition};
use crate::core::engine::error::BodyError;
use crate::core::engine::node::Node;
use crate::core::engine::shape::{Query, Ref};

/// 非 Clone／Copy 的业务 Data：登记与 Ref 复制不要求业务类型可复制。
struct Amount(u32);

impl Data for Amount {}

struct Doubled(u32);

impl Data for Doubled {}

/// 函数 Node：自然异步签名。
async fn double(query: Query<&Amount>) -> Result<Doubled, BodyError> {
    Ok(Doubled(query.get().0 * 2))
}

/// 结构体 Node：`type Input`／`type Output` 声明协议。
struct AddOne;

impl Node for AddOne {
    type Input = Doubled;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        // 函数 Node：Ref<Amount> → Ref<Doubled>。
        let doubled: Ref<Doubled> = flow.then_with_output(double, amount);
        // Chain：子 Body 独立登记（内部为结构体 Node），parent 获得新的输出 Ref。
        let chained: Ref<Amount> = flow.chain(|sub| sub.then_with_output(AddOne, doubled));
        // Root 输出只由 `output_with` 声明；闭包本身返回 `()`。
        flow.output_with(chained);
    })
    .expect("definition should build");

    // Body 结构：Root + 一个 Chain child。
    assert_eq!(definition.bodies().len(), 2, "root body plus one chain body");

    let root = definition.root();
    assert_eq!(root.steps().len(), 2, "one node step and one chain step");
    assert_eq!(root.steps()[0].kind(), StepKind::Node);
    assert_eq!(root.steps()[1].kind(), StepKind::Chain);
    assert!(root.steps()[0].callable_name().is_some(), "real callable name");
    assert_eq!(root.steps()[0].inputs().len(), 1);
    assert_eq!(root.steps()[1].inputs().len(), 0, "chain has no explicit inputs");

    let chain = root.steps()[1].chain().expect("chain step view");
    let child = definition.body(chain.child());
    assert_eq!(child.parent(), Some(root.id()));
    assert_eq!(child.steps().len(), 1);
    assert_eq!(child.steps()[0].kind(), StepKind::Node);

    // Chain 输出与 child 输出是**不同**的逻辑位置：parent 得到新的 Ref。
    assert_eq!(root.steps()[1].outputs().len(), 1);
    assert_eq!(child.output().len(), 1);
    assert_ne!(
        root.steps()[1].outputs()[0],
        child.output()[0].handle.position,
        "parent chain output must be a new position"
    );
    // 唯一 child 出口位置与 child 声明输出一致。
    assert_eq!(chain.exits().len(), 1);
    assert_eq!(chain.exits()[0], child.output()[0].handle.position);

    // Root 输入 1 + 函数 Node 输出 1 + Chain 内结构体 Node 输出 1 + Chain parent 输出 1
    // + 该槽位的 child 唯一声明出口 1 = 5（WE-T02 起每个输出槽另有唯一出口位置）。
    assert_eq!(definition.positions().len(), 5);
    assert_eq!(root.output().len(), 1);
    assert!(definition.record(root.output()[0].handle).is_some());

    println!("we_t01-positive-ok");
}
