//! WE-T06 A 阶段 S04～S07：Each／Choose／Retry／Iter 的公开 API 端到端集成证据。
//!
//! 范围（任务书 §4 的 S04～S07 行与 §8 的 X08／X09／X10）：
//!
//! - **S04 Each**：普通集合与 item-backed 集合（外层 item 的 `Ref<Vec<T>>` 作为内层集合，
//!   含三层集合）；unit／empty；多输出逐槽聚合顺序；新 owned 输出合法；item 本身／ancestor／
//!   重复 owned alias 不可 Consume（运行期 typed 拒绝，拒绝前业务节点计数为 1）；原集合在
//!   Each 之后仍可读。
//! - **S05 Choose**：同一入口；`PartialEq` 最小约束；所有分支静态验证（未选分支的非法来源在
//!   Definition 期拒绝）；只运行一支；otherwise／NoMatch；失败不改走 fallback；重复配置拒绝；
//!   输出按槽一致。
//! - **S06 Retry**：重启完整 attempt；预算边界（0／N／上限）；只捕获 Retry；普通 Failure 与
//!   耗尽为 terminal；清理结束后才进入下一 attempt；原输入保持；末次动态原因保持。
//! - **S07 Iter**：current／next／final 同 Shape；首轮与后续 Break 返回 current；多状态成组
//!   Promote（imported 与 same-target 规则）；上限不交付未判断 next、不额外 Judge；current
//!   为 round-local。
//!
//! 本文件只使用 crate 根的新公开面（`Runtime／Flow／Ref／Query／Node／Data／RunError` 等），
//! 不引用 `srflow::legacy`，也不触碰 crate 私有入口；判据复用 T02～T05 已验收的语义，但每个
//! 样本都在公开执行链上重新断言具体的值／事件顺序／调用计数／错误分类与 payload，而不是
//! `is_ok()` 之类的壳断言。

use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use futures::executor::block_on;
use srflow::{BodyError, ControlSignal, Data, Flow, Node, Query, Ref, RunError, Runtime};

// ================================================================ 事件记录与复位

thread_local! {
    /// 单线程执行下的事件序列：业务调用顺序、读到的值、以及 Drop 顺序。
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn reset() {
    EVENTS.with(|events| events.borrow_mut().clear());
}

fn note(event: impl Into<String>) {
    EVENTS.with(|events| events.borrow_mut().push(event.into()));
}

fn events() -> Vec<String> {
    EVENTS.with(|events| events.borrow().clone())
}

/// 以事件前缀计数一次业务调用（计数与顺序取自同一条记录，不是从字符串反推）。
fn count(prefix: &str) -> usize {
    events()
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

// ================================================================ S04 业务类型与 Node

#[derive(Data, Debug, PartialEq, Eq)]
struct Item(u32);

impl Drop for Item {
    fn drop(&mut self) {
        note(format!("drop:item:{}", self.0));
    }
}

#[derive(Data, Debug, PartialEq, Eq)]
struct Score(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Report(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Total(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Count(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Rule(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Mark(u32);

/// 借用 Struct Node：以 `&node` 注册，输入同时含 item 与一个 ancestor（`Rule`）。
struct ScoreNode {
    delta: u32,
}

impl Node for ScoreNode {
    type Input = (Item, Rule);
    type Output = Score;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Score, BodyError> {
        let (item, rule) = query.get();
        note("score");
        Ok(Score(item.0 * rule.0 + self.delta))
    }
}

/// 同类型（`Score`）但不同逻辑位置的第二槽。
async fn bump_score(query: Query<(&Item, &Score)>) -> Result<Score, BodyError> {
    let (item, score) = query.get();
    note("bump-score");
    Ok(Score(score.0 + item.0))
}

/// 异构槽（`Report`）。
async fn make_report(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    let (item, score) = query.get();
    note("report");
    Ok(Report(item.0 * 100 + score.0))
}

/// Each 之后读取**原集合**：证明集合没有被 item 借用取走或复制。
async fn count_items(query: Query<&Vec<Item>>) -> Result<Total, BodyError> {
    note("count");
    Ok(Total(query.get().len() as u32))
}

/// item-backed 内层的每项输出。
async fn increment(query: Query<&Item>) -> Result<Total, BodyError> {
    note("increment");
    Ok(Total(query.get().0 + 1))
}

/// 读取**原外层集合**（item-backed 样本）。
async fn count_nested(query: Query<&Vec<Vec<Item>>>) -> Result<Count, BodyError> {
    Ok(Count(
        query.get().iter().map(|group| group.len() as u32).sum(),
    ))
}

/// unit body：不产生业务槽。
async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    let _ = query.get();
    note("audit");
    Ok(())
}

/// item Body 新产生的 owned 输出（各拒绝样本的业务节点）。
async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    note("touch");
    Ok(Mark(query.get().0))
}

/// 同时读取 item 与 ancestor 的业务节点（ancestor 拒绝样本）。
async fn touch_with_rule(query: Query<(&Item, &Rule)>) -> Result<Mark, BodyError> {
    let (item, rule) = query.get();
    note("touch");
    Ok(Mark(item.0 + rule.0))
}

fn nested_values(collected: &[Vec<Total>]) -> Vec<Vec<u32>> {
    collected
        .iter()
        .map(|group| group.iter().map(|total| total.0).collect())
        .collect()
}

fn cube_values(collected: &[Vec<Vec<Total>>]) -> Vec<Vec<Vec<u32>>> {
    collected
        .iter()
        .map(|plane| {
            plane
                .iter()
                .map(|row| row.iter().map(|total| total.0).collect())
                .collect()
        })
        .collect()
}

#[test]
fn s04_plain_collection_slots_aggregate_in_item_order() {
    reset();
    let runtime = Runtime::new();
    let node = ScoreNode { delta: 1 };
    let (scores, bumped, reports, counted) = block_on(runtime.execute(
        |flow: &mut Flow<'_>, (items, rules): (Ref<Vec<Item>>, Ref<Rule>)| {
            let (scores, bumped, reports) = flow.each(items, |item, each| {
                let score = each.then(&node, (item, rules));
                let bumped = each.then(&bump_score, (item, score));
                let report = each.then(&make_report, (item, score));
                (score, bumped, report)
            });
            let counted = flow.then(&count_items, items);
            (scores, bumped, reports, counted)
        },
        (vec![Item(1), Item(2), Item(3)], Rule(10)),
    ))
    .expect("a plain collection Each runs");

    // 逐槽值：score = v*10+1；bumped = score+v；report = v*100+score。
    assert_eq!(
        scores.iter().map(|v| v.0).collect::<Vec<_>>(),
        vec![11, 21, 31],
        "slot 1 values and item order"
    );
    assert_eq!(
        bumped.iter().map(|v| v.0).collect::<Vec<_>>(),
        vec![12, 23, 34],
        "slot 2 (same type, distinct position) stays independent"
    );
    assert_eq!(
        reports.iter().map(|v| v.0).collect::<Vec<_>>(),
        vec![111, 221, 331],
        "slot 3 heterogeneous values and item order"
    );
    assert_eq!(counted.0, 3, "the original collection stays readable");

    // 业务事件顺序：每个 item 内 score → bump-score → report，然后才是后续 parent step。
    let observed = events();
    assert_eq!(
        &observed[..10],
        &[
            "score",
            "bump-score",
            "report",
            "score",
            "bump-score",
            "report",
            "score",
            "bump-score",
            "report",
            "count",
        ],
        "step order inside each item is strict and the parent step runs last: {observed:?}"
    );
    assert_eq!(count("score"), 3, "one body call per element");
    assert_eq!(
        count("drop:item:"),
        3,
        "no element is moved out of the collection"
    );
}

#[test]
fn s04_item_backed_nested_and_three_level_collections() {
    reset();
    let runtime = Runtime::new();
    let (nested, counted) = block_on(runtime.execute(
        |flow: &mut Flow<'_>, groups: Ref<Vec<Vec<Item>>>| {
            let nested = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| inner.then(&increment, item))
            });
            let counted = flow.then(&count_nested, groups);
            (nested, counted)
        },
        vec![vec![Item(1), Item(2)], vec![Item(3)]],
    ))
    .expect("the outer item (Ref<Vec<T>>) is a legal inner collection");

    assert_eq!(nested_values(&nested), vec![vec![2u32, 3], vec![4]]);
    assert_eq!(
        counted.0, 3,
        "the outer collection stays readable after the nested Each"
    );
    assert_eq!(
        count("increment"),
        3,
        "one inner body call per inner element"
    );

    reset();
    let three = block_on(runtime.execute(
        |flow: &mut Flow<'_>, cube: Ref<Vec<Vec<Vec<Item>>>>| {
            flow.each(cube, |plane, level1| {
                level1.each(plane, |row, level2| {
                    level2.each(row, |cell, level3| level3.then(&increment, cell))
                })
            })
        },
        vec![
            vec![vec![Item(1), Item(2)], vec![Item(3)]],
            vec![vec![Item(4)]],
        ],
    ))
    .expect("three levels of item-backed collections run");
    assert_eq!(
        cube_values(&three),
        vec![vec![vec![2u32, 3], vec![4]], vec![vec![5]]]
    );
    assert_eq!(count("increment"), 4, "one body call per innermost element");
}

#[test]
fn s04_unit_and_empty_collections_run_no_body() {
    reset();
    let runtime = Runtime::new();

    // unit body：不产生业务槽。
    let outcome: Result<(), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                each.then(&audit, item);
            })
        },
        vec![Item(1), Item(2)],
    ));
    assert_eq!(count("audit"), 2, "the unit body runs once per element");
    outcome.expect("a unit Each over a non-empty collection succeeds");

    // 空集合：不执行 body，正常输出空 Vec。
    reset();
    let empty = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| each.then(&increment, item))
        },
        Vec::<Item>::new(),
    ))
    .expect("an empty collection is legal");
    assert_eq!(
        empty,
        Vec::<Total>::new(),
        "an empty collection yields an empty Vec"
    );
    assert_eq!(
        count("increment"),
        0,
        "the body never runs for an empty collection"
    );
}

/// 三个 Comsume 拒绝样本共用的 typed 诊断断言：`RunError::Runtime` + 冻结的框架 note +
/// 首个框架诊断中可区分的拒绝原因。
fn assert_each_consume_rejected(error: RunError, expected_source: &str) {
    match error {
        RunError::Runtime(failure) => {
            assert_eq!(
                failure.note(),
                "each item consume rejected",
                "the frozen Each consume rejection note"
            );
            let source = failure
                .source()
                .expect("the first framework diagnostic is preserved");
            let text = source.to_string();
            assert!(
                text.contains(expected_source),
                "the typed diagnostic names the real reason ({expected_source}): {text}"
            );
        }
        other => panic!("expected RunError::Runtime, got {other}"),
    }
}

#[test]
fn s04_item_output_is_not_consumable() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<Vec<Item>, RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                each.then(&touch, item);
                // 期望拒绝：formal item 自身出现在被收集的输出槽。
                item
            })
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome.expect_err("the formal item cannot be collected");
    assert_each_consume_rejected(error, "not a complete Data");
    assert_eq!(
        count("touch"),
        1,
        "the first item body runs once before the group preflight rejects"
    );
    assert_eq!(
        count("drop:item:"),
        2,
        "no element is moved out of the collection"
    );
}

#[test]
fn s04_ancestor_alias_output_is_not_consumable() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<(Vec<Mark>, Vec<Rule>), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, (items, rules): (Ref<Vec<Item>>, Ref<Rule>)| {
            flow.each(items, |item, each| {
                let mark = each.then(&touch_with_rule, (item, rules));
                // 期望拒绝：ancestor Data（Root 输入 rules）出现在被收集的输出槽。
                (mark, rules)
            })
        },
        (vec![Item(1), Item(2)], Rule(7)),
    ));
    let error = outcome.expect_err("a caller-owned alias cannot be collected");
    assert_each_consume_rejected(error, "may not dispose of");
    assert_eq!(
        count("touch"),
        1,
        "the first item body runs once before the group preflight rejects"
    );
}

#[test]
fn s04_duplicate_owned_alias_is_not_consumable() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<(Vec<Mark>, Vec<Mark>), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let mark = each.then(&touch, item);
                // 期望拒绝：同一个 owned Ref 出现在两个输出槽（Ref 是 Copy，形态合法）。
                (mark, mark)
            })
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome.expect_err("the same owned Ref must not be moved twice");
    // 同一逻辑位置在两个槽重复出现：Consume 全组预检在移动前拒绝该重复位置。
    assert_each_consume_rejected(error, "appears more than once in the same batch");
    assert_eq!(
        count("touch"),
        1,
        "the first item body runs once before the group preflight rejects"
    );
    assert_eq!(
        count("drop:item:"),
        2,
        "no element is moved out of the collection"
    );
}

// ================================================================ S05 业务类型与 Node

/// 业务路由键：只声明 `Data` 并手写 `PartialEq`，不派生 `Eq`／`Hash`／`Clone`／`Copy`／`Debug`，
/// 其中一个字段使用 `Rc`（最小约束样本）。
#[derive(Data)]
struct Route {
    source: Rc<str>,
    code: u8,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && self.source == other.source
    }
}

fn route(code: u8, source: &'static str) -> Route {
    Route {
        source: Rc::from(source),
        code,
    }
}

#[derive(Data, Debug, PartialEq, Eq)]
struct Seed(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Amount(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Sum(u32);

/// 前序 Node：由输入值产生 selector（`Ref<Route>`）。
async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    note("route");
    Ok(route(query.get().0 as u8, "selector"))
}

async fn double(query: Query<&Seed>) -> Result<Amount, BodyError> {
    note("double");
    Ok(Amount(query.get().0 * 2))
}

async fn triple(query: Query<&Seed>) -> Result<Amount, BodyError> {
    note("triple");
    Ok(Amount(query.get().0 * 3))
}

async fn fallback(query: Query<&Seed>) -> Result<Amount, BodyError> {
    note("fallback");
    Ok(Amount(query.get().0 + 100))
}

/// 第二槽：由选中的 `Amount` 派生 `Amount`（每槽类型一致的 evidence）。
async fn detail(query: Query<&Amount>) -> Result<Amount, BodyError> {
    note("detail");
    Ok(Amount(query.get().0 * 10))
}

/// 后续 parent step：同时读取 Choose 的两个输出槽。
async fn sum(query: Query<(&Amount, &Amount)>) -> Result<Sum, BodyError> {
    let (amount, detail) = query.get();
    note("sum");
    Ok(Sum(amount.0 + detail.0))
}

/// 明确的业务失败（动态对象）。
#[derive(Debug)]
struct BranchFailed(u32);

impl fmt::Display for BranchFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "branch rejected seed {}", self.0)
    }
}

impl Error for BranchFailed {}

async fn failing(query: Query<&Seed>) -> Result<Amount, BodyError> {
    note("failing");
    Err(BodyError::fail(BranchFailed(query.get().0)))
}

#[test]
fn s05_one_branch_runs_and_slots_stay_consistent() {
    for (value, expected_event, expected) in [
        (1u32, "double", 22u32),
        (2u32, "triple", 66u32),
        (7u32, "fallback", 1177u32),
    ] {
        reset();
        let runtime = Runtime::new();
        let output = block_on(runtime.execute(
            |flow: &mut Flow<'_>, seed: Ref<Seed>| {
                let selector = flow.then(&route_of, seed);
                // 同一入口：Choose 的两槽输出由后续 parent step 读取。
                let (amount, detail) = flow.choose(selector, |choice| {
                    choice.case(route(1, "selector"), |branch| {
                        let amount = branch.then(&double, seed);
                        let detail = branch.then(&detail, amount);
                        (amount, detail)
                    });
                    choice.case(route(2, "selector"), |branch| {
                        let amount = branch.then(&triple, seed);
                        let detail = branch.then(&detail, amount);
                        (amount, detail)
                    });
                    choice.otherwise(|branch| {
                        let amount = branch.then(&fallback, seed);
                        let detail = branch.then(&detail, amount);
                        (amount, detail)
                    });
                });
                flow.then(&sum, (amount, detail))
            },
            Seed(value),
        ))
        .expect("the frozen Choose usage runs");

        assert_eq!(output.0, expected, "value {value} selects one branch");
        assert_eq!(
            events(),
            vec![
                String::from("route"),
                String::from(expected_event),
                String::from("detail"),
                String::from("sum"),
            ],
            "only the selected branch runs, in definition order (value {value})"
        );
    }
}

#[test]
fn s05_key_uses_partial_eq_without_extra_bounds() {
    reset();
    let runtime = Runtime::new();
    // selector 与 case 键 code 相同、source 不同：只按 value 相等匹配 → otherwise。
    let output = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1, "different"), |branch| branch.then(&double, seed));
                choice.otherwise(|branch| branch.then(&fallback, seed))
            })
        },
        Seed(1),
    ))
    .expect("the minimal key comparison runs");

    assert_eq!(output.0, 101, "only the full value (source + code) matches");
    assert_eq!(count("double"), 0, "the unequal case does not run");
    assert_eq!(count("fallback"), 1);
}

#[test]
fn s05_unselected_branch_illegal_source_is_rejected_at_definition() {
    reset();
    let leaked: RefCell<Option<Ref<Amount>>> = RefCell::new(None);
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            let leaked = &leaked;
            flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| {
                    let produced = branch.then(&double, seed);
                    *leaked.borrow_mut() = Some(produced);
                });
                choice.case(route(2, "selector"), |branch| {
                    // 未选分支使用 sibling 分支内部的位置：Definition 静态验证必须拒绝。
                    match *leaked.borrow() {
                        Some(position) => {
                            branch.then(&detail, position);
                        }
                        None => {
                            branch.then(&double, seed);
                        }
                    };
                });
            })
        },
        Seed(1),
    ))
    .expect_err("an illegal source inside an unselected branch is rejected");
    assert!(
        matches!(error, RunError::Definition(_)),
        "expected a Definition rejection, got {error}"
    );
    assert_eq!(
        events(),
        Vec::<String>::new(),
        "no business Node runs for an invalid Definition"
    );
}

#[test]
fn s05_duplicate_configs_and_empty_config_are_rejected() {
    reset();
    let runtime = Runtime::new();

    // (a) 重复 case 值。
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| {
                    branch.then(&double, seed);
                });
                choice.case(route(1, "selector"), |branch| {
                    branch.then(&triple, seed);
                });
            })
        },
        Seed(1),
    ))
    .expect_err("a repeated case value is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert!(
        error.to_string().contains("repeats a case value"),
        "the diagnostic names the duplicated configuration: {error}"
    );

    // (b) 重复 otherwise。
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| {
                    branch.then(&double, seed);
                });
                choice.otherwise(|branch| {
                    branch.then(&fallback, seed);
                });
                choice.otherwise(|branch| {
                    branch.then(&fallback, seed);
                });
            })
        },
        Seed(1),
    ))
    .expect_err("a second otherwise is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert!(
        error.to_string().contains("second otherwise branch"),
        "{error}"
    );

    // (c) 无任何分支的配置。
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            let _: () = flow.choose(selector, |_choice| {});
        },
        Seed(1),
    ))
    .expect_err("an empty choose configuration is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert!(
        error
            .to_string()
            .contains("neither a case nor an otherwise"),
        "{error}"
    );

    assert_eq!(
        events(),
        Vec::<String>::new(),
        "no business Node runs for any rejected configuration"
    );
}

#[test]
fn s05_no_match_otherwise_and_failure_never_fall_back() {
    reset();
    let runtime = Runtime::new();

    // (a) otherwise 覆盖无匹配值。
    let covered = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| branch.then(&double, seed));
                choice.otherwise(|branch| branch.then(&fallback, seed))
            })
        },
        Seed(9),
    ))
    .expect("otherwise covers the unmatched value");
    assert_eq!(covered.0, 109);
    assert_eq!(count("fallback"), 1);
    assert_eq!(count("double"), 0);

    // (b) 无匹配且无 otherwise → NoMatchingCase，且无分支业务。
    reset();
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| {
                    branch.then(&double, seed);
                });
            })
        },
        Seed(9),
    ))
    .expect_err("no match and no otherwise terminates the execution");
    assert!(
        matches!(error, RunError::NoMatchingCase),
        "expected NoMatchingCase, got {error}"
    );
    assert_eq!(count("double"), 0, "no branch body ran");
    assert_eq!(count("route"), 1, "the selector Node ran exactly once");

    // (c) 选中分支失败不改走 otherwise，后续业务不执行。
    reset();
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| branch.then(&failing, seed));
                choice.otherwise(|branch| branch.then(&fallback, seed))
            });
            flow.then(&detail, picked)
        },
        Seed(1),
    ))
    .expect_err("a selected branch failure terminates the execution");
    match error {
        RunError::Body(failure) => {
            let source = failure
                .source()
                .expect("the dynamic failure object is preserved");
            let typed = source
                .downcast_ref::<BranchFailed>()
                .expect("the original dynamic object type is preserved");
            assert_eq!(typed.0, 1);
        }
        other => panic!("expected RunError::Body, got {other}"),
    }
    assert_eq!(
        count("fallback"),
        0,
        "a branch failure never reroutes to otherwise"
    );
    assert_eq!(count("detail"), 0, "later business does not run");

    // (d) Unit 输出与 tuple 输出同法成立（这里以 `()` 分支结果作对照）。
    reset();
    let unit: Result<(), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then(&route_of, seed);
            let _: () = flow.choose(selector, |choice| {
                choice.case(route(1, "selector"), |branch| {
                    branch.then(&double, seed);
                });
            });
        },
        Seed(1),
    ));
    assert_eq!(count("double"), 1);
    unit.expect("a unit-typed choose succeeds");
}

// ================================================================ S06 业务类型与 Node

#[derive(Data, Debug, PartialEq, Eq)]
struct Prompt(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Candidate(u32);
#[derive(Data, Debug, PartialEq, Eq)]
struct Verdict(u32);

/// attempt 自产值的析构事件：证明上一轮在下一轮业务之前已清理。
#[derive(Data)]
struct Tracked(u32);

impl Drop for Tracked {
    fn drop(&mut self) {
        note(format!("drop:tracked:{}", self.0));
    }
}

thread_local! {
    /// 每次 attempt 的序号（线程局部：每个 `#[test]` 各自线程）。
    static ATTEMPT: Cell<u32> = const { Cell::new(0) };
}

/// 业务重试理由：携带动态原因（attempt 序号与候选值）。
#[derive(Debug)]
struct NotReady {
    attempt: u32,
    candidate: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "candidate {} is not ready on attempt {}",
            self.candidate, self.attempt
        )
    }
}

impl Error for NotReady {}

/// 生成候选：只读取原始 ancestor `prompt`（每次 attempt 读到同一个值）。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    let prompt = query.get().0;
    note(format!("generate:{prompt}"));
    Ok(Candidate(prompt + 1))
}

/// 登记一个带析构事件的 attempt 自产值。
async fn track(query: Query<&Prompt>) -> Result<Tracked, BodyError> {
    note("run:tracked");
    Ok(Tracked(query.get().0))
}

/// 评审：前两次要求重试，第三次正常交付。
async fn judge_retrying(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let attempt = ATTEMPT.with(|counter| {
        let next = counter.get() + 1;
        counter.set(next);
        next
    });
    let candidate = query.get().0;
    note(format!("judge:{attempt}"));
    if attempt < 3 {
        Err(BodyError::retry(NotReady { attempt, candidate }))
    } else {
        Ok(Verdict(candidate * 10))
    }
}

/// 评审：永远要求重试（预算边界样本）。
async fn judge_always_retrying(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let attempt = ATTEMPT.with(|counter| {
        let next = counter.get() + 1;
        counter.set(next);
        next
    });
    let candidate = query.get().0;
    note(format!("judge:{attempt}"));
    Err(BodyError::retry(NotReady { attempt, candidate }))
}

/// 明确的业务失败：不得被 Retry 捕获。
#[derive(Debug)]
struct Fatal(u32);

impl fmt::Display for Fatal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fatal candidate {}", self.0)
    }
}

impl Error for Fatal {}

async fn judge_failing(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    note("judge-failing");
    Err(BodyError::fail(Fatal(query.get().0)))
}

/// 发出 IterBreak：Retry 不得捕获。
async fn judge_break(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let _ = query.get();
    note("judge-break");
    Err(BodyError::iter_break())
}

#[test]
fn s06_retry_restarts_full_attempt_and_cleans_before_the_next() {
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let runtime = Runtime::new();
    let (verdict, tracked) = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(3, |attempt| {
                let tracked = attempt.then(&track, prompt);
                let candidate = attempt.then(&generate, prompt);
                let verdict = attempt.then(&judge_retrying, candidate);
                (verdict, tracked)
            })
        },
        Prompt(41),
    ))
    .expect("the third attempt succeeds");

    assert_eq!(
        verdict.0, 420,
        "the delivered value is the succeeding attempt's"
    );
    let observed = events();
    assert_eq!(
        &observed[..11],
        &[
            "run:tracked",
            "generate:41",
            "judge:1",
            "drop:tracked:41",
            "run:tracked",
            "generate:41",
            "judge:2",
            "drop:tracked:41",
            "run:tracked",
            "generate:41",
            "judge:3",
        ],
        "each attempt restarts the whole body and the previous attempt is cleaned first: {observed:?}"
    );
    assert_eq!(
        count("generate:41"),
        3,
        "every attempt reads the same original ancestor input"
    );
    assert_eq!(
        count("drop:tracked"),
        2,
        "the two failed attempts' values were released before the next attempt"
    );
    drop(tracked);
    assert_eq!(
        count("drop:tracked"),
        3,
        "the delivered value is the last attempt's own object"
    );
}

#[test]
fn s06_budget_boundaries_zero_n_and_max() {
    let runtime = Runtime::new();

    // (a) N = 0：仍执行首次 attempt；耗尽时 attempts 为 1。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(0, |attempt| {
                let candidate = attempt.then(&generate, prompt);
                attempt.then(&judge_always_retrying, candidate)
            })
        },
        Prompt(0),
    ))
    .expect_err("a zero budget still runs the first attempt and then exhausts");
    match error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((max_retries, attempts), (0, 1));
            let reason = last_error
                .source()
                .expect("the last dynamic reason is kept");
            let typed = reason
                .downcast_ref::<NotReady>()
                .expect("the dynamic reason type is preserved");
            assert_eq!(
                (typed.attempt, typed.candidate),
                (1, 1),
                "the single attempt's own reason is the retained one"
            );
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }
    assert_eq!(count("generate:0"), 1, "exactly one attempt Node call");

    // (b) N = 1：最多两次尝试。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(1, |attempt| {
                let candidate = attempt.then(&generate, prompt);
                attempt.then(&judge_always_retrying, candidate)
            })
        },
        Prompt(0),
    ))
    .expect_err("N = 1 exhausts after two attempts");
    match error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            ..
        } => assert_eq!((max_retries, attempts), (1, 2)),
        other => panic!("expected RetryExhausted, got {other}"),
    }
    assert_eq!(count("generate:0"), 2, "two attempt Node calls");
    assert_eq!(count("judge:"), 2, "one judge per attempt");

    // (c) usize::MAX 无法表达 N+1：构建期拒绝、零 Node。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(usize::MAX, |attempt| attempt.then(&generate, prompt))
        },
        Prompt(0),
    ))
    .expect_err("usize::MAX cannot express N + 1");
    assert!(
        matches!(error, RunError::Definition(_)),
        "expected a Definition rejection, got {error}"
    );
    assert_eq!(events(), Vec::<String>::new(), "no Node runs");

    // (d) usize::MAX - 1 仍可表达；首次 attempt 成功时恰好一次调用。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let verdict = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(usize::MAX - 1, |attempt| attempt.then(&generate, prompt))
        },
        Prompt(41),
    ))
    .expect("the largest representable budget delivers a first-attempt success");
    assert_eq!(verdict.0, 42);
    assert_eq!(count("generate:41"), 1, "exactly one attempt Node call");
}

#[test]
fn s06_only_retry_is_captured_failure_and_exhaustion_are_terminal() {
    let runtime = Runtime::new();

    // (a) 普通 Failure 立即终止：不重试、不驱动任何其他业务。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(3, |attempt| {
                let candidate = attempt.then(&generate, prompt);
                attempt.then(&judge_failing, candidate)
            })
        },
        Prompt(1),
    ))
    .expect_err("a plain Failure terminates the attempt");
    match error {
        RunError::Body(failure) => {
            let source = failure.source().expect("the dynamic object is preserved");
            let typed = source
                .downcast_ref::<Fatal>()
                .expect("the original dynamic object type is preserved");
            assert_eq!(typed.0, 2);
        }
        other => panic!("expected RunError::Body, got {other}"),
    }
    assert_eq!(count("generate:1"), 1, "a Failure is not retried");
    assert_eq!(count("judge-failing"), 1);

    // (b) IterBreak 不被 Retry 捕获：到 Root 成为 UnhandledControl。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(3, |attempt| {
                let candidate = attempt.then(&generate, prompt);
                let _ = attempt.then(&judge_break, candidate);
                candidate
            })
        },
        Prompt(1),
    ))
    .expect_err("Retry must not capture IterBreak");
    match error {
        RunError::UnhandledControl(ControlSignal::IterBreak(_)) => {}
        other => panic!("expected UnhandledControl(IterBreak), got {other}"),
    }
    assert_eq!(
        count("judge-break"),
        1,
        "the signal left the Retry boundary at once"
    );

    // (c) 耗尽为 terminal，保留 max_retries／实际 attempts／最后一次动态原因，
    //     且每次 attempt 的原因互不相同（末次原因确实是最后一次的）。
    reset();
    ATTEMPT.with(|counter| counter.set(0));
    let error = block_on(runtime.execute(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(2, |attempt| {
                let candidate = attempt.then(&generate, prompt);
                attempt.then(&judge_always_retrying, candidate)
            })
        },
        Prompt(1),
    ))
    .expect_err("the budget exhaustion is terminal");
    match error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((max_retries, attempts), (2, 3));
            let reason = last_error
                .source()
                .expect("the last dynamic reason is reachable");
            let typed = reason
                .downcast_ref::<NotReady>()
                .expect("the dynamic reason type is preserved");
            assert_eq!(
                typed.attempt, 3,
                "the retained reason is the third (last) attempt's"
            );
            assert_eq!(typed.candidate, 2);
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }
    assert_eq!(
        events().iter().filter(|e| e.starts_with("judge:")).count(),
        3,
        "three attempts requested a retry"
    );
}

// ================================================================ S07 业务类型与 Node

#[derive(Data, Debug, PartialEq, Eq)]
struct Draft(String);
#[derive(Data, Debug, PartialEq, Eq)]
struct Context(u32);

/// 每轮把 draft 追加一个字符。
async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    note("revise");
    Ok(Draft(format!("{}+", query.get().0)))
}

/// 每轮把 context 加一。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    note("bump");
    Ok(Context(query.get().0 + 1))
}

/// 读取 iter 之后仍然有效的**原始** root 输入（imported state 保持原 owner）。
async fn read_context(query: Query<&Context>) -> Result<Context, BodyError> {
    note("read-original");
    Ok(Context(query.get().0))
}

/// 断言两个位置读到同一值后发出 IterBreak（共享 initial／捕获重叠样本）。
async fn compare_and_break(query: Query<(&Draft, &Draft)>) -> Result<(), BodyError> {
    let (left, right) = query.get();
    note("compare");
    assert_eq!(left.0, right.0, "both positions read the same shared value");
    Err(BodyError::iter_break())
}

/// 阈值判断节点：current 达到阈值时发出 IterBreak。
struct BreakAt {
    threshold: u32,
}

impl Node for BreakAt {
    type Input = Context;
    type Output = Verdict;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Verdict, BodyError> {
        let value = query.get().0;
        note(format!("judge:{value}"));
        if value >= self.threshold {
            Err(BodyError::iter_break())
        } else {
            Ok(Verdict(value))
        }
    }
}

/// 永远 Continue。
struct Continue;

impl Node for Continue {
    type Input = Context;
    type Output = Verdict;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Verdict, BodyError> {
        let value = query.get().0;
        note(format!("judge:{value}"));
        Ok(Verdict(value))
    }
}

#[test]
fn s07_break_delivers_the_entry_current_of_the_round() {
    reset();
    let judge = BreakAt { threshold: 1 };
    let (draft, context) = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            flow.iter(
                (draft, context),
                5,
                |(current_draft, current_context), round| {
                    round.then(&judge, current_context);
                    let next_draft = round.then(&revise, current_draft);
                    let next_context = round.then(&bump, current_context);
                    (next_draft, next_context)
                },
            )
        },
        (Draft(String::from("s0")), Context(0)),
    ))
    .expect("the second round breaks");

    assert_eq!(
        (draft.0.as_str(), context.0),
        ("s0+", 1),
        "the entry current of the breaking round is delivered, not the produced next"
    );
    assert_eq!(count("judge"), 2, "one judge per entered round");
    assert_eq!(
        count("revise"),
        1,
        "the breaking round stops before its own next steps"
    );
    assert_eq!(count("bump"), 1);
}

#[test]
fn s07_first_round_break_returns_the_original_state() {
    reset();
    let judge = BreakAt { threshold: 1 };
    let (draft, context) = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            flow.iter(
                (draft, context),
                1,
                |(current_draft, current_context), round| {
                    round.then(&judge, current_context);
                    let _next_draft = round.then(&revise, current_draft);
                    // same-target：两个槽都保留 current。
                    (current_draft, current_context)
                },
            )
        },
        (Draft(String::from("keep")), Context(9)),
    ))
    .expect("a first-round break is legal");

    assert_eq!(
        (draft.0.as_str(), context.0),
        ("keep", 9),
        "the original state is delivered unchanged"
    );
    assert_eq!(count("judge"), 1);
    assert_eq!(
        count("revise"),
        0,
        "the breaking round runs no further step"
    );
}

#[test]
fn s07_round_limit_does_not_deliver_the_next_or_judge_again() {
    reset();
    let judge = Continue;
    let error = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            flow.iter(
                (draft, context),
                2,
                |(current_draft, current_context), round| {
                    round.then(&judge, current_context);
                    let next_draft = round.then(&revise, current_draft);
                    let next_context = round.then(&bump, current_context);
                    (next_draft, next_context)
                },
            )
        },
        (Draft(String::from("r")), Context(0)),
    ))
    .expect_err("the Nth round still continuing is a terminal error");

    match error {
        RunError::IterationLimitReached {
            max_iterations,
            completed_iterations,
        } => assert_eq!((max_iterations, completed_iterations), (2, 2)),
        other => panic!("expected IterationLimitReached, got {other}"),
    }
    assert_eq!(count("judge"), 2, "no extra judge after the limit");
    assert_eq!(count("revise"), 2, "the Nth round completed its body once");
    assert_eq!(count("bump"), 2);
}

#[test]
fn s07_multi_state_promote_keeps_imported_owner_and_same_target() {
    reset();
    let judge = BreakAt { threshold: 2 };
    let (draft, context, original) = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            // 多状态成组 Promote：槽 1 same-target（原样保留），槽 2 被推进。
            let (kept_draft, advanced) = flow.iter(
                (draft, context),
                5,
                |(current_draft, current_context), round| {
                    round.then(&judge, current_context);
                    let next_context = round.then(&bump, current_context);
                    (current_draft, next_context)
                },
            );
            // imported 初始状态保持原 owner：iter 之后仍可读取原始 root 输入。
            let original = flow.then(&read_context, context);
            (kept_draft, advanced, original)
        },
        (Draft(String::from("s")), Context(0)),
    ))
    .expect("the third round breaks");

    assert_eq!(
        (draft.0.as_str(), context.0, original.0),
        ("s", 2, 0),
        "the same-target slot is unchanged, the promoted slot advanced twice, and the imported initial keeps its owner"
    );
    assert_eq!(count("judge"), 3, "three entered rounds");
    assert_eq!(count("bump"), 2, "only the two continuing rounds promote");
    assert_eq!(count("read-original"), 1);
}

#[test]
fn s07_formal_current_is_round_local() {
    // (a) round-local current 作为 Root 输出：Definition 静态拒绝、零业务。
    reset();
    let mut escaped: Option<Ref<Draft>> = None;
    let error = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, initial: Ref<Draft>| {
            let _output = flow.iter(initial, 1, |current, round| {
                escaped = Some(current);
                round.then(&revise, current);
                current
            });
            escaped.expect("captured round-local current")
        },
        Draft(String::from("x")),
    ))
    .expect_err("a round-local current must not become a Root output");
    assert!(
        matches!(error, RunError::Definition(_)),
        "expected a Definition rejection, got {error}"
    );
    assert_eq!(
        events(),
        Vec::<String>::new(),
        "an invalid Definition runs no business Node"
    );

    // (b) round-local current 被 parent Body 的 step 使用：同样在 Definition 期拒绝。
    reset();
    let mut escaped: Option<Ref<Draft>> = None;
    let error = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, initial: Ref<Draft>| {
            let _output = flow.iter(initial, 1, |current, round| {
                escaped = Some(current);
                round.then(&revise, current);
                current
            });
            flow.then(&revise, escaped.expect("captured round-local current"))
        },
        Draft(String::from("x")),
    ))
    .expect_err("a parent step must not use a round-local current");
    assert!(
        matches!(error, RunError::Definition(_)),
        "expected a Definition rejection, got {error}"
    );
    assert_eq!(events(), Vec::<String>::new());
}

#[test]
fn s07_duplicate_initial_and_capture_overlap_are_legal() {
    // (a) iter((x, x), ..)：两个 state 槽共享同一 initial 来源，各自身份保持。
    reset();
    let out = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, initial: Ref<Draft>| {
            let both = flow.iter((initial, initial), 1, |(left, right), round| {
                round.then(&compare_and_break, (left, right));
                (left, right)
            });
            both.0
        },
        Draft(String::from("dup")),
    ))
    .expect("a duplicated initial is a legal shared state alias");
    assert_eq!(out.0, "dup");
    assert_eq!(count("compare"), 1);

    // (b) round 同时捕获 initial 与 current：普通词法依赖与 initial 重叠仍合法。
    reset();
    let out = block_on(Runtime::new().execute(
        |flow: &mut Flow<'_>, initial: Ref<Draft>| {
            flow.iter(initial, 1, |current, round| {
                round.then(&compare_and_break, (current, initial));
                current
            })
        },
        Draft(String::from("overlap")),
    ))
    .expect("capturing the initial alongside the current is legal");
    assert_eq!(out.0, "overlap");
    assert_eq!(count("compare"), 1);
}
