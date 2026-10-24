//! V22-T04 定向测试：Chain/Retry 新构建接口（unit Body + 显式 output + 可选 O 期望）。
//!
//! 覆盖本任务新增语义的判别性证据：
//! - 普通形式与 `_output` 形式共用同一条 child 构建与登记路径（结构等价 + 实跑一致）；
//! - child 声明状态 Undeclared／Unit／NonUnit 可区分，声明后来源冻结且后续步骤仍执行；
//! - `_output` 的期望 Shape 在收口处核对，命中 Definition 分类（body／slot／expected／found）
//!   并在任何业务 Node 之前拒绝，unit 与未声明的零槽语义互不混同；
//! - child 声明失败保留首因，失败句柄不成为合法来源，不 panic；
//! - Retry 的预算在构建期拒绝 `usize::MAX`，`_output` 期望不符先于任何 attempt。
use crate::core::engine::data::Data;
use crate::core::engine::definition::{Body, HandoverSlot, PositionRecord};
use crate::core::engine::definition::{DeclarationState, Definition, build_definition};
use crate::core::engine::error::{BodyError, BuildErrorDetail, RunError};
use crate::core::engine::identity::{BodyId, PositionHandle, PositionId};
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref, TypeDesc};
use crate::core::ref_id::RefId;
use futures::executor::block_on;
use std::cell::Cell;

thread_local! {
    /// 业务 Node／Factory 的真实调用计数。
    static CALLS: Cell<u32> = const { Cell::new(0) };
}

fn reset_calls() {
    CALLS.with(|calls| calls.set(0));
}

fn calls() -> u32 {
    CALLS.with(Cell::get)
}

struct A(u32);
impl Data for A {}

struct B(u32);
impl Data for B {}

struct Missing;
impl Data for Missing {}

fn make_a() -> A {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    A(1)
}

fn make_alt_a() -> A {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    A(7)
}

async fn a_to_b(query: Query<&A>) -> Result<B, BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(B(query.get().0 + 1))
}

async fn b_sink(query: Query<&B>) -> Result<(), BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    let _ = query.get();
    Ok(())
}

/// 普通业务失败（非 RetrySignal）：用于验证 Retry 不捕获普通 Failure。
async fn fail_always(query: Query<&A>) -> Result<(), BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    let _ = query.get();
    Err(BodyError::fail(std::io::Error::other("plain failure")))
}

/// 结构性画像：Root 与每个 Chain/Retry child 的步骤数、声明状态与声明槽类型。
fn profile(definition: &Definition<'_>) -> Vec<String> {
    let root = definition.root();
    let mut out = vec![format!(
        "root steps={} decl={:?} outputs={}",
        root.steps().len(),
        root.output_declaration().state(),
        root.steps()
            .last()
            .map(|step| step.outputs().len())
            .unwrap_or(0)
    )];
    for step in root.steps() {
        if let Some(chain) = step.chain() {
            let child = definition.body(chain.child());
            out.push(format!(
                "chain steps={} decl={:?} slots={:?} bindings={} exits={} handover={}",
                child.steps().len(),
                child.output_declaration().state(),
                child
                    .output()
                    .iter()
                    .map(|slot| slot.type_desc)
                    .collect::<Vec<_>>(),
                chain.bindings().len(),
                chain.exits().len(),
                chain.slots().len()
            ));
        } else if let Some(retry) = step.retry() {
            let child = definition.body(retry.child());
            out.push(format!(
                "retry steps={} decl={:?} slots={:?} budget={} bridges={} handover={}",
                child.steps().len(),
                child.output_declaration().state(),
                child
                    .output()
                    .iter()
                    .map(|slot| slot.type_desc)
                    .collect::<Vec<_>>(),
                retry.max_retries(),
                retry.bridges().len(),
                retry.slots().len()
            ));
        } else {
            out.push(format!("step kind={:?}", step.kind()));
        }
    }
    out
}

/// 取出第一个 Chain child。
fn first_chain_child<'n>(definition: &Definition<'n>) -> BodyId {
    definition
        .root()
        .steps()
        .iter()
        .find_map(|step| step.chain().map(|chain| chain.child()))
        .expect("chain step with child")
}

// ---------------------------------------------------------------- D02 等价

#[test]
fn t04_chain_plain_and_output_share_registration_and_execution() {
    reset_calls();
    let plain: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.chain(|sub| {
            let seed = sub.provide_output(make_a);
            let derived = sub.then_with_output(a_to_b, seed);
            sub.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("普通 chain 构建成功");

    let explicit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        let _refs: Ref<B> = flow.chain_output(|sub| {
            let seed = sub.provide_output(make_a);
            let derived = sub.then_with_output(a_to_b, seed);
            sub.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("取输出 chain 构建成功");

    assert_eq!(
        profile(&plain),
        profile(&explicit),
        "两种公开形式必须共享同一条 child 构建与登记路径"
    );
    assert_eq!(
        deep_profile(&plain),
        deep_profile(&explicit),
        "深画像（imports/来源/顺序/交接映射）必须逐项一致"
    );

    // 实跑一致：同一 Body 在两种形式下都真实执行一次。
    for build in 0..2 {
        reset_calls();
        let runtime = Runtime::new();
        let produced: B = block_on(runtime.execute::<_, B, _>(
            |flow, ()| {
                if build == 0 {
                    flow.chain(|sub| {
                        let seed = sub.provide_output(make_a);
                        let derived = sub.then_with_output(a_to_b, seed);
                        sub.output_with(derived);
                    });
                    flow.output::<B>();
                } else {
                    let _: Ref<B> = flow.chain_output(|sub| {
                        let seed = sub.provide_output(make_a);
                        let derived = sub.then_with_output(a_to_b, seed);
                        sub.output_with(derived);
                    });
                    flow.output::<B>();
                }
            },
            (),
        ))
        .expect("Chain 交付 B");
        assert_eq!(produced.0, 2, "A(1) 经 a_to_b 得 B(2)");
        assert_eq!(calls(), 2, "Factory 与 Node 各一次");
    }
}

#[test]
fn t04_retry_plain_and_output_share_registration_and_execution() {
    reset_calls();
    let plain: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(2, |attempt| {
            let seed = attempt.provide_output(make_a);
            let derived = attempt.then_with_output(a_to_b, seed);
            attempt.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("普通 retry 构建成功");

    let explicit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        let _refs: Ref<B> = flow.retry_output(2, |attempt| {
            let seed = attempt.provide_output(make_a);
            let derived = attempt.then_with_output(a_to_b, seed);
            attempt.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("取输出 retry 构建成功");

    assert_eq!(
        profile(&plain),
        profile(&explicit),
        "Retry 两种形式结构等价"
    );
    assert_eq!(
        deep_profile(&plain),
        deep_profile(&explicit),
        "Retry 深画像（bridges/来源/顺序/交接映射）必须逐项一致"
    );

    reset_calls();
    let runtime = Runtime::new();
    let produced: B = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            let _: Ref<B> = flow.retry_output(2, |attempt| {
                let seed = attempt.provide_output(make_a);
                let derived = attempt.then_with_output(a_to_b, seed);
                attempt.output_with(derived);
            });
            flow.output::<B>();
        },
        (),
    ))
    .expect("Retry 交付 B");
    assert_eq!(produced.0, 2);
    assert_eq!(calls(), 2, "首次 attempt 即成功：Factory 与 Node 各一次");
}

#[test]
fn t04_plain_chain_still_exports_declared_output_without_taking_ref() {
    reset_calls();
    let runtime = Runtime::new();
    let produced: B = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            // 普通形式不接收 Ref，但已声明输出照常发布给 parent。
            flow.chain(|sub| {
                let seed = sub.provide_output(make_a);
                let derived = sub.then_with_output(a_to_b, seed);
                sub.output_with(derived);
            });
            let read_back = flow.ref_of::<B>();
            let _seen = read_back;
            flow.output::<B>();
        },
        (),
    ))
    .expect("普通 Chain 已发布的输出可被后续步骤解析");
    assert_eq!(produced.0, 2);
    assert_eq!(calls(), 2);
}

// ------------------------------------------- D04 声明状态与冻结

#[test]
fn t04_child_declaration_states_are_distinct() {
    let undeclared: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.chain(|sub| {
            let seed = sub.provide_output(make_a);
            let _ = sub.then_with_output(a_to_b, seed);
        });
        flow.output::<()>();
    })
    .expect("未声明 child 合法（零业务输出）");

    let unit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.chain(|sub| {
            let seed = sub.provide_output(make_a);
            let _ = sub.then_with_output(a_to_b, seed);
            sub.output::<()>();
        });
        flow.output::<()>();
    })
    .expect("显式 unit child 合法");

    let non_unit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.chain(|sub| {
            let seed = sub.provide_output(make_a);
            let derived = sub.then_with_output(a_to_b, seed);
            sub.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("非空声明 child 合法");

    assert_eq!(
        undeclared
            .body(first_chain_child(&undeclared))
            .output_declaration()
            .state(),
        DeclarationState::Undeclared,
        "不写 output 是未声明，不是显式 unit"
    );
    assert_eq!(
        unit.body(first_chain_child(&unit))
            .output_declaration()
            .state(),
        DeclarationState::Unit,
        "output::<()>() 是显式 unit"
    );
    assert_eq!(
        non_unit
            .body(first_chain_child(&non_unit))
            .output_declaration()
            .state(),
        DeclarationState::NonUnit,
        "逐槽声明是非空"
    );
}

#[test]
fn t04_declaration_freezes_the_earlier_source_and_later_steps_still_run() {
    reset_calls();
    let runtime = Runtime::new();
    let produced: A = block_on(runtime.execute::<_, A, _>(
        |flow, ()| {
            let _: Ref<A> = flow.chain_output(|sub| {
                let first = sub.provide_output(make_a);
                sub.output_with(first);
                // 声明之后出现的同类型来源不得替换已冻结的声明来源。
                sub.provide(make_alt_a);
            });
            flow.output::<A>();
        },
        (),
    ))
    .expect("Chain 交付声明时点选定的 A");
    assert_eq!(produced.0, 1, "冻结来源是声明处的第一个 Factory 产出");
    assert_eq!(calls(), 2, "声明后的同类型工厂照常登记并执行");
}

// ------------------------------------------- D05 期望校验

#[test]
fn t04_output_expectation_rejects_undeclared_body_before_any_business() {
    reset_calls();
    let error = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<A> = flow.chain_output(|sub| {
            let seed = sub.provide_output(make_a);
            let _ = sub.then_with_output(a_to_b, seed);
        });
        flow.output::<()>();
    })
    .err()
    .expect("非空期望遇到未声明 child 必须拒绝");

    match error {
        BuildErrorDetail::MissingDeclaration { body, kind } => {
            assert_ne!(body, BodyId::ROOT, "诊断必须指向实际 child Body");
            assert_eq!(
                kind,
                crate::core::engine::definition::DeclarationKind::Output
            );
        }
        other => panic!("期望 MissingDeclaration，实际 {other}"),
    }
    assert_eq!(calls(), 0, "定义期拒绝时业务不得被调用");
}

#[test]
fn t04_output_expectation_rejects_slot_type_mismatch_with_slot_and_found() {
    let error = build_definition::<(), (), _>(|flow, ()| {
        // child 声明 A；调用方期望 B：Rust 可构建，必须在定义期拒绝。
        let _: Ref<B> = flow.chain_output(|sub| {
            let seed = sub.provide_output(make_a);
            let _ = sub.then_with_output(a_to_b, seed);
            sub.output::<A>();
        });
        flow.output::<()>();
    })
    .err()
    .expect("同 arity 错槽类型必须被定义拒绝");

    match error {
        BuildErrorDetail::DeclarationShapeMismatch {
            body,
            slot,
            expected,
            found,
            ..
        } => {
            assert_ne!(body, BodyId::ROOT, "诊断指向 child");
            assert_eq!(slot, 0);
            assert_eq!(expected, TypeDesc::of::<B>(), "期望来自请求的 O");
            assert_eq!(found, Some(TypeDesc::of::<A>()), "实际是 child 声明");
        }
        other => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
    }
}

#[test]
fn t04_output_expectation_accepts_matching_shapes_and_unit_pairs() {
    reset_calls();
    let runtime = Runtime::new();
    let produced: B = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            let _derived: Ref<B> = flow.chain_output(|sub| {
                let seed = sub.provide_output(make_a);
                let derived = sub.then_with_output(a_to_b, seed);
                sub.output_with(derived);
            });
            flow.output::<B>();
        },
        (),
    ))
    .expect("期望与声明一致的 B 必须运行成功");
    assert_eq!(produced.0, 2);
    assert_eq!(calls(), 2);

    // unit 期望与两种零槽 child（未声明、显式 unit）都合法。
    for explicit_unit in [false, true] {
        build_definition::<(), (), _>(|flow, ()| {
            let _: () = flow.chain_output(|sub| {
                let seed = sub.provide_output(make_a);
                let _ = sub.then_with_output(a_to_b, seed);
                if explicit_unit {
                    sub.output::<()>();
                }
            });
            flow.output::<()>();
        })
        .expect("零槽 child 满足 unit 期望");
    }
}

// ------------------------------------------- D06 失败恢复

#[test]
fn t04_failed_child_keeps_first_cause_and_never_publishes_a_real_source() {
    reset_calls();
    let error = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<Missing> = flow.chain_output(|sub| {
            // 无来源的声明：child 自身失败，首因是缺失来源。
            sub.output::<Missing>();
        });
        flow.output::<()>();
    })
    .err()
    .expect("child 声明失败必须整体拒绝");

    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "首因必须是缺失来源，而不是二次 expectation/重复诊断：{error}"
    );
    assert_eq!(calls(), 0, "定义期拒绝时业务不得被调用");

    // 失败句柄继续参与后续控制时不得 panic，且仍被统一拒绝。
    let nested = build_definition::<(), (), _>(|flow, ()| {
        let failed: Ref<Missing> = flow.chain_output(|sub| {
            sub.output::<Missing>();
        });
        flow.chain(|outer| {
            outer.output_with(failed);
        });
        flow.output::<()>();
    })
    .err()
    .expect("失败句柄进入外层 chain 仍必须被拒绝");
    assert!(
        matches!(nested, BuildErrorDetail::MissingSource { .. }),
        "更早的合法首因必须保留：{nested}"
    );
    assert_eq!(calls(), 0);
}

// ------------------------------------------- Retry 预算与期望

#[test]
fn t04_retry_budget_max_is_unrepresentable_at_definition_time() {
    let error = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(usize::MAX, |attempt| {
            attempt.output::<()>();
        });
        flow.output::<()>();
    })
    .err()
    .expect("usize::MAX 无法表达 N+1，必须在构建期拒绝");
    assert!(
        matches!(
            error,
            BuildErrorDetail::RetryBudgetUnrepresentable { body, .. } if body == BodyId::ROOT
        ),
        "期望 RetryBudgetUnrepresentable，实际 {error}"
    );
}

/// `max_retries = 0` 仍执行首次 attempt（新入口）。
#[test]
fn t04_retry_zero_budget_still_runs_the_first_attempt() {
    reset_calls();
    let runtime = Runtime::new();
    let produced: B = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            let _: Ref<B> = flow.retry_output(0, |attempt| {
                let seed = attempt.provide_output(make_a);
                let derived = attempt.then_with_output(a_to_b, seed);
                attempt.output_with(derived);
            });
            flow.output::<B>();
        },
        (),
    ))
    .expect("max=0 仍执行首次 attempt");
    assert_eq!(produced.0, 2);
    assert_eq!(calls(), 2, "首次 attempt 恰好一轮 Factory + Node");
}

/// 普通业务失败不被 Retry 捕获：attempt 只执行一次，错误原样传播。
#[test]
fn t04_retry_does_not_capture_plain_failure() {
    reset_calls();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (), _>(
        |flow, ()| {
            let _: () = flow.retry_output(3, |attempt| {
                let seed = attempt.provide_output(make_a);
                attempt.then_with_output(fail_always, seed);
                attempt.output::<()>();
            });
            flow.output::<()>();
        },
        (),
    ));
    assert!(outcome.is_err(), "普通 Failure 必须作为错误传播");
    assert_eq!(calls(), 2, "普通失败不触发重试：attempt 只跑一次");
}

#[test]
fn t04_retry_output_expectation_rejects_shape_mismatch_before_any_attempt() {
    reset_calls();
    let error = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<B> = flow.retry_output(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            let _ = attempt.then_with_output(a_to_b, seed);
            attempt.output::<A>();
        });
        flow.output::<()>();
    })
    .err()
    .expect("attempt 声明与期望不符必须被定义拒绝");

    match error {
        BuildErrorDetail::DeclarationShapeMismatch {
            body,
            slot,
            expected,
            found,
            ..
        } => {
            assert_ne!(body, BodyId::ROOT, "诊断指向 attempt child");
            assert_eq!(slot, 0);
            assert_eq!(expected, TypeDesc::of::<B>());
            assert_eq!(found, Some(TypeDesc::of::<A>()));
        }
        other => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
    }
    assert_eq!(calls(), 0, "定义期拒绝时 attempt 业务不得被调用");
}

// ------------------------------------------- D12 捕获范围（新入口）

// 保持 RunError 在测试中可见（上面的传播路径可能返回 RunError::Definition）。
#[allow(dead_code)]
fn assert_run_error_is_used(error: RunError) -> String {
    format!("{error}")
}

// --------------------------------------------------- R01 重复声明的失败隔离

/// child 首声明成功、随后重复声明失败后，四个入口都不得发布合法父域来源；
/// 保留第一份声明快照与错误首因，先前合法来源仍可用。
fn duplicate_declaration_case(retry: bool, plain: bool, with_earlier: bool) {
    use crate::core::engine::definition::{BodyRole, Builder};
    use crate::core::engine::flow::Flow;
    use std::cell::RefCell;
    use std::rc::Rc;

    let builder = Rc::new(RefCell::new(Builder::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let mut flow = Flow::open(Rc::clone(&builder), root);

    let earlier: Option<Ref<A>> = with_earlier.then(|| {
        flow.chain_output(|child| {
            let alt = child.provide_output(make_alt_a);
            child.output_with(alt);
        })
    });

    let duplicate = |child: &mut Flow<'_>| {
        let a = child.provide_output(make_a);
        child.output_with(a);
        child.output_with(a);
    };
    if retry {
        if plain {
            flow.retry(0, duplicate);
        } else {
            let _: Ref<A> = flow.retry_output(0, duplicate);
        }
    } else if plain {
        flow.chain(duplicate);
    } else {
        let _: Ref<A> = flow.chain_output(duplicate);
    }

    // 失败隔离的可观测通道：ref_of 不得选中失败 child 发布的合法来源。
    let selected = flow.ref_of::<A>();
    let selected_published = builder
        .borrow()
        .position(selected.handle())
        .map(|record| !record.is_tainted())
        .unwrap_or(false);
    let earlier_published = earlier
        .as_ref()
        .map(|reference| {
            builder
                .borrow()
                .position(reference.handle())
                .map(|record| !record.is_tainted())
                .unwrap_or(false)
        })
        .unwrap_or(true);
    drop(flow);

    let error = Rc::try_unwrap(builder)
        .ok()
        .unwrap()
        .into_inner()
        .finish(root)
        .err()
        .expect("重复声明的 Definition 必须被拒绝");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateDeclaration { .. }),
        "首因应为重复声明：{error}"
    );
    let selected_is_earlier = earlier
        .as_ref()
        .map(|reference| reference.handle().position == selected.handle().position)
        .unwrap_or(false);
    if with_earlier {
        assert!(
            !selected_published || selected_is_earlier,
            "ref_of 只允许选中先前合法来源，不得选中失败 child 的合法来源（retry={retry} plain={plain}）"
        );
    } else {
        assert!(
            !selected_published,
            "ref_of 不得选中失败 child 发布的合法来源（retry={retry} plain={plain}）"
        );
    }
    assert!(
        earlier_published,
        "先前合法来源必须保持可用（retry={retry} plain={plain}）"
    );
}

#[test]
fn t04_duplicate_declaration_isolates_all_four_entries() {
    for (retry, plain) in [(false, false), (false, true), (true, false), (true, true)] {
        duplicate_declaration_case(retry, plain, false);
    }
}

// ---------------------------------------------- R02 深画像（来源/顺序/交接）

fn ord_body(definition: &Definition<'_>, id: BodyId) -> usize {
    definition
        .bodies()
        .iter()
        .position(|body| body.id() == id)
        .unwrap_or(usize::MAX)
}

fn ord_pos(definition: &Definition<'_>, id: PositionId) -> usize {
    definition
        .positions()
        .iter()
        .position(|record| record.handle().position == id)
        .unwrap_or(usize::MAX)
}

fn ord_core(definition: &Definition<'_>, core: &RefId) -> usize {
    let mut seen: Vec<&RefId> = Vec::new();
    for record in definition.positions() {
        if !seen.iter().any(|known| *known == record.core()) {
            seen.push(record.core());
        }
    }
    seen.iter()
        .position(|known| *known == core)
        .unwrap_or(usize::MAX)
}

/// 时点归一化：把声明时点与已发布时间点放在同一集合里排名；0 表示未声明/未发布。
///
/// 不把不同合法时点合并成同一个占位符：集合外的值以 `#<raw>` 保留，便于比较时暴露差异。
fn rank_order(definition: &Definition<'_>, order: u64) -> String {
    if order == 0 {
        return "-".to_string();
    }
    let mut all: Vec<u64> = Vec::new();
    for body in definition.bodies() {
        all.push(body.output_declaration().order);
        all.push(body.next_declaration().order);
    }
    for record in definition.positions() {
        if let Some(published) = record.published_order() {
            all.push(published);
        }
    }
    all.retain(|known| *known != 0);
    all.sort_unstable();
    all.dedup();
    match all.iter().position(|known| *known == order) {
        Some(index) => index.to_string(),
        None => format!("#{order}"),
    }
}

fn fmt_record(definition: &Definition<'_>, record: &PositionRecord) -> String {
    format!(
        "owner#{} src={:?} ty={} pub={} tainted={}",
        ord_body(definition, record.owner()),
        record.source(),
        record.type_desc(),
        rank_order(definition, record.published_order().unwrap_or(0)),
        record.is_tainted()
    )
}

fn pos_str(definition: &Definition<'_>, id: PositionId) -> String {
    match definition
        .positions()
        .iter()
        .find(|record| record.handle().position == id)
    {
        Some(record) => fmt_record(definition, record),
        None => "missing".to_string(),
    }
}

#[allow(dead_code)]
fn record_str(definition: &Definition<'_>, handle: PositionHandle) -> String {
    match definition.record(handle) {
        Some(record) => fmt_record(definition, record),
        None => "missing".to_string(),
    }
}

fn handover_str(definition: &Definition<'_>, slots: &[HandoverSlot]) -> Vec<String> {
    slots
        .iter()
        .map(|slot| {
            format!(
                "  handover src_core#{} exit={} out={} ty={}",
                ord_core(definition, &slot.source),
                ord_core(definition, &slot.exit),
                ord_core(definition, &slot.output),
                slot.type_desc
            )
        })
        .collect()
}

/// 单个 Body 的逐 Step 画像：真实 Query 输入（位置归一化 + 类型）、输出身份与顺序。
fn body_steps_str(definition: &Definition<'_>, body: &Body<'_>, prefix: &str) -> Vec<String> {
    body.steps()
        .iter()
        .enumerate()
        .map(|(index, step)| {
            format!(
                "{prefix}step{index} kind={:?} inputs={:?} outputs={:?}",
                step.kind(),
                step.inputs()
                    .iter()
                    .map(|desc| format!(
                        "pos#{}:{}",
                        ord_pos(definition, desc.handle.position),
                        desc.type_desc
                    ))
                    .collect::<Vec<_>>(),
                step.outputs()
                    .iter()
                    .map(|id| pos_str(definition, *id))
                    .collect::<Vec<_>>()
            )
        })
        .collect()
}

/// 深画像：imports、声明/发布时点、逐槽实际来源、每个 Step 的真实输入接线与输出、
/// 父输出、交接映射全部按身份归一化后比较；两侧接线不同必须得到不同画像。
fn deep_profile(definition: &Definition<'_>) -> Vec<String> {
    let root = definition.root();
    let mut out = vec![format!(
        "root steps={} decl={:?} slots={:?} order={} imports={:?}",
        root.steps().len(),
        root.output_declaration().state(),
        root.output()
            .iter()
            .map(|slot| slot.type_desc)
            .collect::<Vec<_>>(),
        rank_order(definition, root.output_declaration().order),
        root.imports()
            .iter()
            .map(|id| ord_pos(definition, *id))
            .collect::<Vec<_>>()
    )];
    out.extend(body_steps_str(definition, root, "root/"));
    for (index, step) in root.steps().iter().enumerate() {
        if let Some(chain) = step.chain() {
            let child = definition.body(chain.child());
            out.push(format!(
                "step{index} chain child#{} steps={} decl={:?} slots={:?} order={} imports={:?}",
                ord_body(definition, child.id()),
                child.steps().len(),
                child.output_declaration().state(),
                child
                    .output()
                    .iter()
                    .map(|slot| slot.type_desc)
                    .collect::<Vec<_>>(),
                rank_order(definition, child.output_declaration().order),
                child
                    .imports()
                    .iter()
                    .map(|id| ord_pos(definition, *id))
                    .collect::<Vec<_>>()
            ));
            out.push(format!("  bindings={:?}", chain.bindings()));
            out.push(format!(
                "  exits={:?}",
                chain
                    .exits()
                    .iter()
                    .map(|id| ord_pos(definition, *id))
                    .collect::<Vec<_>>()
            ));
            out.extend(handover_str(definition, chain.slots()));
            out.push(format!(
                "  declared_slots={:?}",
                child
                    .output()
                    .iter()
                    .map(|slot| record_str(definition, slot.handle))
                    .collect::<Vec<_>>()
            ));
            out.extend(body_steps_str(definition, child, "  child/"));
        } else if let Some(retry) = step.retry() {
            let child = definition.body(retry.child());
            out.push(format!(
                "step{index} retry child#{} steps={} decl={:?} slots={:?} order={} imports={:?} budget={}",
                ord_body(definition, child.id()),
                child.steps().len(),
                child.output_declaration().state(),
                child.output()
                    .iter()
                    .map(|slot| slot.type_desc)
                    .collect::<Vec<_>>(),
                rank_order(definition, child.output_declaration().order),
                child.imports()
                    .iter()
                    .map(|id| ord_pos(definition, *id))
                    .collect::<Vec<_>>(),
                retry.max_retries()
            ));
            out.push(format!(
                "  bridges={:?}",
                retry
                    .bridges()
                    .iter()
                    .map(|core| ord_core(definition, core))
                    .collect::<Vec<_>>()
            ));
            out.extend(handover_str(definition, retry.slots()));
            out.push(format!(
                "  declared_slots={:?}",
                child
                    .output()
                    .iter()
                    .map(|slot| record_str(definition, slot.handle))
                    .collect::<Vec<_>>()
            ));
            out.extend(body_steps_str(definition, child, "  child/"));
        }
    }
    out
}

// ------------------------------- R02 前提补强：祖先导入、双槽同类型异来源

thread_local! {
    /// 用于普通/取输出 Retry 的同场景比较：首次调用发 RetrySignal。
    static RETRY_ONCE: Cell<bool> = const { Cell::new(false) };
}

async fn retry_once_a(query: Query<&A>) -> Result<A, BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    let value = query.get().0;
    if RETRY_ONCE.with(|flag| flag.replace(false)) {
        Err(BodyError::retry(std::io::Error::other("retry once")))
    } else {
        Ok(A(value + 10))
    }
}

#[test]
fn t04_chain_equivalence_with_ancestor_import_and_dual_sources() {
    let plain: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        flow.chain(|sub| {
            // 两个同类型、不同来源的槽：祖先导入 + child 自产。
            let alt = sub.provide_output(make_alt_a);
            sub.output_with((input, alt));
        });
        flow.output::<()>();
    })
    .expect("普通 chain（双槽＋祖先导入）构建成功");

    let explicit: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        let _refs: (Ref<A>, Ref<A>) = flow.chain_output(|sub| {
            let alt = sub.provide_output(make_alt_a);
            sub.output_with((input, alt));
        });
        flow.output::<()>();
    })
    .expect("取输出 chain（双槽＋祖先导入）构建成功");

    let left = deep_profile(&plain);
    let right = deep_profile(&explicit);
    assert_eq!(left, right, "双槽＋祖先导入下两种形式必须结构等价");
    assert!(
        left.iter()
            .any(|line| line.contains("imports=[") && !line.contains("imports=[]")),
        "前提：child 必须真实导入祖先来源（非空 imports）：{left:?}"
    );
    assert!(
        left.iter()
            .filter(|line| line.contains("handover src_core#"))
            .count()
            == 2,
        "前提：两条槽的交接映射都要登记：{left:?}"
    );
    let child_slots = left
        .iter()
        .find(|line| line.contains("declared_slots="))
        .expect("child 声明槽画像");
    assert!(
        child_slots.matches("owner#").count() == 2
            && child_slots.matches("src=RootInput").count() == 1
            && child_slots.matches("src=NodeOutput").count() == 1,
        "逐槽实际来源必须一个是祖先、一个是 child 自产：{child_slots}"
    );
}

#[test]
fn t04_retry_plain_and_output_run_the_same_scenario() {
    let mut seen = Vec::new();
    for plain in [true, false] {
        reset_calls();
        RETRY_ONCE.with(|flag| flag.set(true));
        let runtime = Runtime::new();
        let produced: A = block_on(runtime.execute::<_, A, _>(
            |flow, ()| {
                if plain {
                    flow.retry(3, |attempt| {
                        let seed = attempt.provide_output(make_a);
                        let derived = attempt.then_with_output(retry_once_a, seed);
                        attempt.output_with(derived);
                    });
                } else {
                    let _: Ref<A> = flow.retry_output(3, |attempt| {
                        let seed = attempt.provide_output(make_a);
                        let derived = attempt.then_with_output(retry_once_a, seed);
                        attempt.output_with(derived);
                    });
                }
                flow.output::<A>();
            },
            (),
        ))
        .expect("首轮 RetrySignal、次轮成功");
        seen.push((produced.0, calls()));
    }
    assert_eq!(
        seen,
        vec![(11, 4), (11, 4)],
        "普通/取输出 Retry 必须同场景、同结果、同计数"
    );
}

/// 取出第一个 Retry child。
fn first_retry_child<'n>(definition: &Definition<'n>) -> BodyId {
    definition
        .root()
        .steps()
        .iter()
        .find_map(|step| step.retry().map(|retry| retry.child()))
        .expect("retry step with child")
}

#[test]
fn t04_chain_published_output_auto_dependency_matches_explicit_form() {
    let auto_chain: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        flow.chain(|sub| {
            let derived = sub.then_with_output(a_to_b, input);
            sub.output_with(derived);
        });
        // D03：后续 parent Node 直接依赖 child 已发布输出（不显式传 Ref）。
        flow.then_with(b_sink, flow.ref_of::<B>());
        flow.output::<()>();
    })
    .expect("普通 Chain 之后可自动依赖已发布输出");

    let explicit_chain: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        let derived: Ref<B> = flow.chain_output(|sub| {
            let derived = sub.then_with_output(a_to_b, input);
            sub.output_with(derived);
        });
        flow.then_with(b_sink, derived);
        flow.output::<()>();
    })
    .expect("取输出 Chain 之后显式接线");

    assert_eq!(
        profile(&auto_chain),
        profile(&explicit_chain),
        "D03：链式上下文与逐句形式的步骤结构一致"
    );
    assert_eq!(
        deep_profile(&auto_chain),
        deep_profile(&explicit_chain),
        "D03：两种写法的来源与顺序一致（C 在 child、D 在 parent）"
    );

    let auto_retry: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        flow.retry(1, |attempt| {
            let derived = attempt.then_with_output(a_to_b, input);
            attempt.output_with(derived);
        });
        flow.then_with(b_sink, flow.ref_of::<B>());
        flow.output::<()>();
    })
    .expect("普通 Retry 之后可自动依赖已发布输出");

    let explicit_retry: Definition<'_> = build_definition::<A, (), _>(|flow, input| {
        let derived: Ref<B> = flow.retry_output(1, |attempt| {
            let derived = attempt.then_with_output(a_to_b, input);
            attempt.output_with(derived);
        });
        flow.then_with(b_sink, derived);
        flow.output::<()>();
    })
    .expect("取输出 Retry 之后显式接线");

    assert_eq!(
        profile(&auto_retry),
        profile(&explicit_retry),
        "D03：Retry 链式与逐句结构一致"
    );
    assert_eq!(
        deep_profile(&auto_retry),
        deep_profile(&explicit_retry),
        "D03：Retry 两种写法的来源与桥接一致"
    );

    // 实跑：两种写法都真实执行 child 与后续 parent Node。
    for auto in [true, false] {
        reset_calls();
        let runtime = Runtime::new();
        block_on(runtime.execute::<_, (), _>(
            |flow, input| {
                if auto {
                    flow.chain(|sub| {
                        let derived = sub.then_with_output(a_to_b, input);
                        sub.output_with(derived);
                    });
                    flow.then_with(b_sink, flow.ref_of::<B>());
                } else {
                    let derived: Ref<B> = flow.chain_output(|sub| {
                        let derived = sub.then_with_output(a_to_b, input);
                        sub.output_with(derived);
                    });
                    flow.then_with(b_sink, derived);
                }
                flow.output::<()>();
            },
            A(1),
        ))
        .expect("链式与逐句都必须真实执行");
        assert_eq!(calls(), 2, "a_to_b 与 b_sink 各一次（auto={auto}）");
    }
}

#[test]
fn t04_retry_declaration_states_and_freeze() {
    let undeclared: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            let _ = attempt.then_with_output(a_to_b, seed);
        });
        flow.output::<()>();
    })
    .expect("未声明 attempt 合法（零业务输出）");

    let unit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            let _ = attempt.then_with_output(a_to_b, seed);
            attempt.output::<()>();
        });
        flow.output::<()>();
    })
    .expect("显式 unit attempt 合法");

    let non_unit: Definition<'_> = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            let derived = attempt.then_with_output(a_to_b, seed);
            attempt.output_with(derived);
        });
        flow.output::<()>();
    })
    .expect("非空声明 attempt 合法");

    assert_eq!(
        undeclared
            .body(first_retry_child(&undeclared))
            .output_declaration()
            .state(),
        DeclarationState::Undeclared,
        "Retry：不写 output 是未声明"
    );
    assert_eq!(
        unit.body(first_retry_child(&unit))
            .output_declaration()
            .state(),
        DeclarationState::Unit,
        "Retry：output::<()>() 是显式 unit"
    );
    assert_eq!(
        non_unit
            .body(first_retry_child(&non_unit))
            .output_declaration()
            .state(),
        DeclarationState::NonUnit,
        "Retry：逐槽声明是非空"
    );

    // 冻结：声明之后出现的同类型来源不得替换声明时点选定的来源，且后续步骤照常执行。
    reset_calls();
    let runtime = Runtime::new();
    let produced: A = block_on(runtime.execute::<_, A, _>(
        |flow, ()| {
            let _: Ref<A> = flow.retry_output(2, |attempt| {
                let first = attempt.provide_output(make_a);
                attempt.output_with(first);
                attempt.provide(make_alt_a);
            });
            flow.output::<A>();
        },
        (),
    ))
    .expect("Retry 交付声明时点选定的 A");
    assert_eq!(produced.0, 1, "冻结来源是声明处的第一个 Factory 产出");
    assert_eq!(calls(), 2, "声明后的同类型工厂照常登记并执行");
}

#[test]
fn t04_retry_output_expectation_covers_missing_arity_and_unit_matrix() {
    use crate::core::engine::definition::DeclarationKind;

    reset_calls();
    let missing = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<A> = flow.retry_output(1, |attempt| {
            let _ = attempt.provide_output(make_a);
        });
        flow.output::<()>();
    })
    .err()
    .expect("attempt 未声明必须被拒绝");
    assert!(
        matches!(
            missing,
            BuildErrorDetail::MissingDeclaration {
                kind: DeclarationKind::Output,
                ..
            }
        ),
        "期望 MissingDeclaration(Output)，实得 {missing}"
    );
    assert_eq!(calls(), 0, "缺声明拒绝不得执行业务");

    reset_calls();
    let too_many = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<A> = flow.retry_output(1, |attempt| {
            let alt = attempt.provide_output(make_alt_a);
            let seed = attempt.provide_output(make_a);
            attempt.output_with((seed, alt));
        });
        flow.output::<()>();
    })
    .err()
    .expect("expect=1 而实得 2 槽必须被拒绝");
    match too_many {
        BuildErrorDetail::DeclarationArityMismatch {
            expected,
            found,
            slot,
            ..
        } => {
            assert_eq!(
                (expected, found, slot),
                (1, 2, 0),
                "arity 方向：期望 1 实得 2"
            );
        }
        other => panic!("期望 DeclarationArityMismatch，实得 {other}"),
    }
    assert_eq!(calls(), 0);

    reset_calls();
    let too_few = build_definition::<(), (), _>(|flow, ()| {
        let _: (Ref<A>, Ref<A>) = flow.retry_output(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            attempt.output_with(seed);
        });
        flow.output::<()>();
    })
    .err()
    .expect("expect=2 而实得 1 槽必须被拒绝");
    match too_few {
        BuildErrorDetail::DeclarationArityMismatch {
            expected,
            found,
            slot,
            ..
        } => {
            assert_eq!(
                (expected, found, slot),
                (2, 1, 0),
                "arity 方向：期望 2 实得 1"
            );
        }
        other => panic!("期望 DeclarationArityMismatch，实得 {other}"),
    }
    assert_eq!(calls(), 0);

    reset_calls();
    let unit_expected = build_definition::<(), (), _>(|flow, ()| {
        let _: () = flow.retry_output(1, |attempt| {
            let seed = attempt.provide_output(make_a);
            attempt.output_with(seed);
        });
        flow.output::<()>();
    })
    .err()
    .expect("期望 unit、实得非空必须被拒绝");
    match unit_expected {
        BuildErrorDetail::DeclarationArityMismatch {
            expected,
            found,
            slot,
            ..
        } => {
            assert_eq!((expected, found, slot), (0, 1, 0), "unit 与非空互不混同");
        }
        other => panic!("期望 DeclarationArityMismatch，实得 {other}"),
    }
    assert_eq!(calls(), 0);

    reset_calls();
    let nonunit_expected = build_definition::<(), (), _>(|flow, ()| {
        let _: Ref<A> = flow.retry_output(1, |attempt| {
            let _ = attempt.provide_output(make_a);
            attempt.output::<()>();
        });
        flow.output::<()>();
    })
    .err()
    .expect("期望非空、实得 unit 必须被拒绝");
    match nonunit_expected {
        BuildErrorDetail::DeclarationArityMismatch {
            expected,
            found,
            slot,
            ..
        } => {
            assert_eq!((expected, found, slot), (1, 0, 0), "非空与 unit 互不混同");
        }
        other => panic!("期望 DeclarationArityMismatch，实得 {other}"),
    }
    assert_eq!(calls(), 0);

    // 正对照：同 Body 匹配的 Shape 必须成功并可运行交付。
    reset_calls();
    let runtime = Runtime::new();
    let produced: A = block_on(runtime.execute::<_, A, _>(
        |flow, ()| {
            let _: Ref<A> = flow.retry_output(1, |attempt| {
                let seed = attempt.provide_output(make_a);
                attempt.output_with(seed);
            });
            flow.output::<A>();
        },
        (),
    ))
    .expect("匹配的 Retry 期望必须成功");
    assert_eq!(produced.0, 1);
    assert_eq!(calls(), 1, "正对照只跑一次 Factory");
}

/// 两份仅“最后一步输入来源”不同的合法 Definition（审查反例的收录形态）。
fn wiring_variant(use_second: bool) -> Definition<'static> {
    build_definition::<(A, A), (), _>(|flow, pair| {
        let (first, second) = pair;
        flow.chain(|sub| {
            // 先各读一次祖先输入：两侧 imports 与顺序相同，只有最后一步的接线不同。
            let _ = sub.then_with_output(a_to_b, first);
            let _ = sub.then_with_output(a_to_b, second);
            let selected = if use_second { second } else { first };
            let result = sub.then_with_output(a_to_b, selected);
            sub.output_with(result);
        });
        flow.output::<()>();
    })
    .expect("两种接线都必须合法")
}

#[test]
fn t04_deep_profile_distinguishes_actual_query_wiring() {
    let left = wiring_variant(false);
    let left_profile = deep_profile(&left);
    assert_eq!(
        left_profile,
        deep_profile(&wiring_variant(false)),
        "相同接线必须得到相同画像"
    );
    assert_ne!(
        left_profile,
        deep_profile(&wiring_variant(true)),
        "不同 Query 接线必须得到不同画像：\n{left_profile:#?}"
    );

    // 实跑区分：只改最后一步的输入来源，交付值必须不同（11 / 21）。
    for (use_second, expected) in [(false, 11u32), (true, 21u32)] {
        reset_calls();
        let runtime = Runtime::new();
        let produced: B = block_on(runtime.execute::<_, B, _>(
            |flow, pair| {
                let (first, second) = pair;
                flow.chain(|sub| {
                    let _ = sub.then_with_output(a_to_b, first);
                    let _ = sub.then_with_output(a_to_b, second);
                    let selected = if use_second { second } else { first };
                    let result = sub.then_with_output(a_to_b, selected);
                    sub.output_with(result);
                });
                flow.output::<B>();
            },
            (A(10), A(20)),
        ))
        .expect("接线正确的 Definition 必须执行成功");
        assert_eq!(produced.0, expected, "最后一步的输入来源决定交付值");
    }
}
