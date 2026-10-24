//! V22-T07 外部消费者（一）：六种自动调用形式 × 两入口 × 两种输出形态的真实运行证据，
//! 重复类型 Query 拒绝（含非相邻）、显式只读重复合法与输出自有规则。
//!
//! 只用 crate 根公开面（`srflow::{…}`）；每格都经真实 `Runtime::execute`，
//! 断言业务 Node 调用次数与可观察值。运行：`cargo test --offline --test we_t07_external_forms`

#![allow(clippy::unused_unit, clippy::let_unit_value)]

use std::cell::Cell;
use std::sync::Arc;

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, RunError, Runtime};

// ---------------------------------------------------------------- 业务类型

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Amount(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Doubled(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Key(u32);

thread_local! {
    /// 生产节点（发布者）调用计数。
    static PRODUCER: Cell<usize> = const { Cell::new(0) };
    /// 观察节点调用计数。
    static OBSERVERS: Cell<usize> = const { Cell::new(0) };
}

fn reset() {
    PRODUCER.with(|c| c.set(0));
    OBSERVERS.with(|c| c.set(0));
}

fn bump() {
    PRODUCER.with(|c| c.set(c.get() + 1));
}

fn observed() {
    OBSERVERS.with(|c| c.set(c.get() + 1));
}

fn calls() -> usize {
    PRODUCER.with(Cell::get)
}

fn observers() -> usize {
    OBSERVERS.with(Cell::get)
}

/// 观察节点（函数 item 形式）：读取**生产节点发布的** `Amount` 新值（Root 原值为 `Amount(1)`）。
async fn assert_amount(query: Query<&Amount>) -> Result<(), BodyError> {
    observed();
    assert_eq!(
        query.get(),
        &Amount(2),
        "自动下游必须读到生产节点发布的新 Amount，而不是 Root 原值"
    );
    Ok(())
}

/// 观察节点（函数 item 形式）：读取生产节点发布的 `Doubled` 新值。
async fn assert_doubled_fn(query: Query<&Doubled>) -> Result<(), BodyError> {
    observed();
    assert_eq!(query.get(), &Doubled(2), "自动下游必须读到发布的新 Doubled");
    Ok(())
}

/// 观察节点（具体 Struct Node 形式）：输入类型即 `Doubled`，读取生产节点发布的新值。
struct AssertDoubled;

impl Node for AssertDoubled {
    type Input = Doubled;
    type Output = ();

    async fn run(&self, query: Query<&Doubled>) -> Result<(), BodyError> {
        observed();
        assert_eq!(query.get(), &Doubled(2), "自动下游必须读到发布的新 Doubled");
        Ok(())
    }
}

// ---------------------------------------------------------------- 六种形式的业务 Node

/// 函数 item（同时用于函数引用形式）。
async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    bump();
    Ok(Amount(query.get().0 + 1))
}

/// unit 输出函数（同时用于函数引用形式）。
async fn record(query: Query<&Amount>) -> Result<(), BodyError> {
    bump();
    let _ = query.get();
    Ok(())
}

/// 显式双槽（只读重复合法样本）。
async fn pair_key(query: Query<(&Amount, &Amount)>) -> Result<Key, BodyError> {
    bump();
    let (first, second) = query.get();
    Ok(Key(first.0 + second.0))
}

/// 非相邻重复类型（自动入口必须拒绝）。
async fn dup_item(query: Query<(&Amount, &Doubled, &Amount)>) -> Result<Key, BodyError> {
    bump();
    let (first, middle, last) = query.get();
    Ok(Key(first.0 + middle.0 + last.0))
}

/// 具体 Struct Node：单 Data 输出。
struct Bump;

impl Node for Bump {
    type Input = Amount;
    type Output = Doubled;

    async fn run(&self, query: Query<&Amount>) -> Result<Doubled, BodyError> {
        bump();
        Ok(Doubled(query.get().0 * 2))
    }
}

/// 具体 Struct Node：unit 输出。
struct Recorder;

impl Node for Recorder {
    type Input = Amount;
    type Output = ();

    async fn run(&self, query: Query<&Amount>) -> Result<(), BodyError> {
        bump();
        let _ = query.get();
        Ok(())
    }
}

/// 具体 Struct Node：非相邻重复类型输入。
struct DupSink;

impl Node for DupSink {
    type Input = (Amount, Doubled, Amount);
    type Output = Key;

    async fn run(&self, query: Query<&(Amount, Doubled, Amount)>) -> Result<Key, BodyError> {
        bump();
        let (first, middle, last) = query.get();
        Ok(Key(first.0 + middle.0 + last.0))
    }
}

// ---------------------------------------------------------------- 六种形式 × 两入口 × 两形态

#[test]
fn t07_external_form_function_item() {
    let runtime = Runtime::new();

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(add_one);
            let _: () = flow.then_output(assert_amount);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("item/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(record);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("item/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let value = block_on(runtime.execute::<Amount, Amount, _>(
        |flow, _input| {
            let value = flow.then_output(add_one);
            flow.output_with(value);
        },
        Amount(1),
    ))
    .expect("item/then_output/单 Data");
    assert_eq!(value, Amount(2));
    assert_eq!(calls(), 1);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(record);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("item/then_output/unit");
    assert_eq!(calls(), 1);
}

#[test]
// 刻意使用 &fn／&struct 引用形式（形式矩阵证据），借用在此处不是冗余。
#[allow(clippy::needless_borrows_for_generic_args)]
fn t07_external_form_function_reference() {
    let runtime = Runtime::new();

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(&add_one);
            let _: () = flow.then_output(&assert_amount);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&fn/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(&record);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&fn/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let value = block_on(runtime.execute::<Amount, Amount, _>(
        |flow, _input| {
            let value = flow.then_output(&add_one);
            flow.output_with(value);
        },
        Amount(1),
    ))
    .expect("&fn/then_output/单 Data");
    assert_eq!(value, Amount(2));
    assert_eq!(calls(), 1);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(&record);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&fn/then_output/unit");
    assert_eq!(calls(), 1);
}

#[test]
fn t07_external_form_async_closure() {
    let runtime = Runtime::new();

    reset();
    let single_node = async move |query: Query<&Amount>| -> Result<Amount, BodyError> {
        bump();
        Ok(Amount(query.get().0 + 1))
    };
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(single_node);
            let _: () = flow.then_output(assert_amount);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("closure/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    let unit_node = async move |query: Query<&Amount>| -> Result<(), BodyError> {
        bump();
        let _ = query.get();
        Ok(())
    };
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(unit_node);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("closure/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let single_node = async move |query: Query<&Amount>| -> Result<Amount, BodyError> {
        bump();
        Ok(Amount(query.get().0 + 1))
    };
    let value = block_on(runtime.execute::<Amount, Amount, _>(
        |flow, _input| {
            let value = flow.then_output(single_node);
            flow.output_with(value);
        },
        Amount(1),
    ))
    .expect("closure/then_output/单 Data");
    assert_eq!(value, Amount(2));
    assert_eq!(calls(), 1);

    reset();
    let unit_node = async move |query: Query<&Amount>| -> Result<(), BodyError> {
        bump();
        let _ = query.get();
        Ok(())
    };
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(unit_node);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("closure/then_output/unit");
    assert_eq!(calls(), 1);
}

#[test]
fn t07_external_form_struct_owned() {
    let runtime = Runtime::new();

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(Bump);
            let _: () = flow.then_output(AssertDoubled);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("struct/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(Recorder);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("struct/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let value = block_on(runtime.execute::<Amount, Doubled, _>(
        |flow, _input| {
            let value = flow.then_output(Bump);
            flow.output_with(value);
        },
        Amount(3),
    ))
    .expect("struct/then_output/单 Data");
    assert_eq!(value, Doubled(6));
    assert_eq!(calls(), 1);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(Recorder);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("struct/then_output/unit");
    assert_eq!(calls(), 1);
}

#[test]
fn t07_external_form_struct_borrowed() {
    let runtime = Runtime::new();

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(&Bump);
            let _: () = flow.then_output(&assert_doubled_fn);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&struct/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(&Recorder);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&struct/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let value = block_on(runtime.execute::<Amount, Doubled, _>(
        |flow, _input| {
            let value = flow.then_output(&Bump);
            flow.output_with(value);
        },
        Amount(3),
    ))
    .expect("&struct/then_output/单 Data");
    assert_eq!(value, Doubled(6));
    assert_eq!(calls(), 1);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(&Recorder);
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("&struct/then_output/unit");
    assert_eq!(calls(), 1);
}

#[test]
fn t07_external_form_struct_arc() {
    let runtime = Runtime::new();
    let shared_bump = Arc::new(Bump);
    let shared_recorder = Arc::new(Recorder);
    let shared_assert_doubled = Arc::new(AssertDoubled);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(Arc::clone(&shared_bump));
            let _: () = flow.then_output(Arc::clone(&shared_assert_doubled));
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("Arc/struct/then/单 Data＋自动下游读取");
    assert_eq!((calls(), observers()), (1, 1), "生产节点与观察节点各一次");

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _ = flow.then(Arc::clone(&shared_recorder));
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("Arc/struct/then/unit");
    assert_eq!(calls(), 1);

    reset();
    let value = block_on(runtime.execute::<Amount, Doubled, _>(
        |flow, _input| {
            let value = flow.then_output(Arc::clone(&shared_bump));
            flow.output_with(value);
        },
        Amount(3),
    ))
    .expect("Arc/struct/then_output/单 Data");
    assert_eq!(value, Doubled(6));
    assert_eq!(calls(), 1);

    reset();
    block_on(runtime.execute::<Amount, (), _>(
        |flow, _input| {
            let _: () = flow.then_output(Arc::clone(&shared_recorder));
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect("Arc/struct/then_output/unit");
    assert_eq!(calls(), 1);
}

// ---------------------------------------------------------------- 重复类型拒绝（形式 × 入口）

macro_rules! assert_duplicate_query_rejected {
    ($label:literal, $definition:expr) => {{
        reset();
        let outcome = block_on(
            Runtime::new()
                .execute::<(Amount, Doubled), (), _>($definition, (Amount(1), Doubled(2))),
        );
        match outcome {
            Err(RunError::Definition(build)) => assert!(
                build.to_string().contains("more than one input slot"),
                "{}：首因应为重复类型，实际：{build}",
                $label
            ),
            other => panic!("{}：非相邻重复类型必须拒绝，实际：{other:?}", $label),
        }
        assert_eq!(calls(), 0, "{}：构建失败不得执行业务", $label);
    }};
}

#[test]
// 刻意使用 &fn／&struct 引用形式（形式矩阵证据），借用在此处不是冗余。
#[allow(clippy::needless_borrows_for_generic_args)]
fn t07_external_duplicate_query_rejected_for_every_form_and_entry() {
    // 函数 item
    assert_duplicate_query_rejected!("item/then", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(dup_item);
        flow.output_with(());
    });
    assert_duplicate_query_rejected!("item/then_output", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(dup_item);
        flow.output_with(());
    });

    // 函数引用
    assert_duplicate_query_rejected!("ref/then", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(&dup_item);
        flow.output_with(());
    });
    assert_duplicate_query_rejected!("ref/then_output", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(&dup_item);
        flow.output_with(());
    });

    // async closure
    let dup_node =
        async move |query: Query<(&Amount, &Doubled, &Amount)>| -> Result<Key, BodyError> {
            let (first, middle, last) = query.get();
            Ok(Key(first.0 + middle.0 + last.0))
        };
    assert_duplicate_query_rejected!("closure/then", move |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(dup_node);
        flow.output_with(());
    });
    let dup_node =
        async move |query: Query<(&Amount, &Doubled, &Amount)>| -> Result<Key, BodyError> {
            let (first, middle, last) = query.get();
            Ok(Key(first.0 + middle.0 + last.0))
        };
    assert_duplicate_query_rejected!("closure/then_output", move |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(dup_node);
        flow.output_with(());
    });

    // 具体 Struct Node
    assert_duplicate_query_rejected!("struct/then", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(DupSink);
        flow.output_with(());
    });
    assert_duplicate_query_rejected!("struct/then_output", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(DupSink);
        flow.output_with(());
    });

    // 借用 Struct Node
    assert_duplicate_query_rejected!("&struct/then", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(&DupSink);
        flow.output_with(());
    });
    assert_duplicate_query_rejected!("&struct/then_output", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(&DupSink);
        flow.output_with(());
    });

    // Arc<具体 Struct Node>
    let shared = Arc::new(DupSink);
    assert_duplicate_query_rejected!("Arc<struct>/then", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then(Arc::clone(&shared));
        flow.output_with(());
    });
    let shared = Arc::new(DupSink);
    assert_duplicate_query_rejected!("Arc<struct>/then_output", |flow: &mut Flow<'_>, _input| {
        let _ = flow.then_output(Arc::clone(&shared));
        flow.output_with(());
    });
}

// ---------------------------------------------------------------- 显式只读重复与输出规则

#[test]
fn t07_external_explicit_readonly_duplicates_are_legal() {
    let runtime = Runtime::new();

    // 显式 (a,a)：同一 Ref 两次只读输入合法。
    reset();
    let key = block_on(runtime.execute::<Amount, Key, _>(
        |flow, amount| {
            let key = flow.then_with_output(pair_key, (amount, amount));
            flow.output_with(key);
        },
        Amount(5),
    ))
    .expect("显式 (a,a) 只读输入合法");
    assert_eq!(key, Key(10));
    assert_eq!(calls(), 1);

    // 显式 (a,b)：不同来源合法。
    reset();
    let key = block_on(runtime.execute::<Amount, Key, _>(
        |flow, amount| {
            let doubled = flow.then_with_output(Bump, amount);
            let key = flow.then_with_output(mix, (amount, doubled));
            flow.output_with(key);
        },
        Amount(3),
    ))
    .expect("显式 (a,b) 合法");
    assert_eq!(key, Key(9));
    assert_eq!(calls(), 2);
}

/// 显式 (Amount, Doubled) 混合。
async fn mix(query: Query<(&Amount, &Doubled)>) -> Result<Key, BodyError> {
    bump();
    let (amount, doubled) = query.get();
    Ok(Key(amount.0 + doubled.0))
}

#[test]
fn t07_external_duplicate_output_uses_root_rule_not_query_ban() {
    reset();
    let error = match block_on(Runtime::new().execute::<Amount, (Amount, Amount), _>(
        |flow, amount| {
            let value = flow.then_with_output(add_one, amount);
            flow.output_with((value, value));
        },
        Amount(1),
    )) {
        Ok(_) => panic!("重复 Root 输出必须拒绝"),
        Err(error) => error,
    };

    let RunError::Definition(build) = error else {
        panic!("期望 RunError::Definition，实际：{error}");
    };
    let text = build.to_string();
    assert!(
        text.contains("more than once"),
        "重复 Root 输出应走输出自有规则，实际：{text}"
    );
    assert!(
        !text.contains("more than one input slot"),
        "输出不得套用自动 Query 禁令，实际：{text}"
    );
    assert_eq!(calls(), 0, "构建失败不得执行业务");
}
