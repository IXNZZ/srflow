//! T05 迁移自 `v22_t03_from_we_t05_p04_foreign_ref_rejected.rs`（WE-T05 P04 负例，Definition
//! 构建期拒绝）：不属本 Body 的 foreign `Ref` 不能进入 Each 定义。
//!
//! T05 契约下 Each 是单参数 Body（`|sub|`）：当前项在 Body 内用 `sub.ref_of::<Item>()` 取得，
//! Body 输出在 Body 内用 `output_with` 声明，聚合结果由 `flow.each_output::<T, O, _>` 显式取回；
//! `then_with_output`、Root 的 `output_with` 与“`RunError::Definition` 且业务 Node 计数不变”的
//! 冻结断言都不变。
//!
//! 外部唯一的合法 `Ref` 来源是真实执行（`Ref` 无公开构造入口）。把上一次执行**泄漏**的 `Ref`
//! 用于另一次 `execute` 的 Each 定义时，必须在**任何业务 Node／RootExecution 开始前**被统一
//! 验证拒绝（任务书 §3.3“无关 Definition 的引用在 Node 执行前拒绝”／P04）。
//!
//! 本夹具真实编译并运行，覆盖两个场景：
//! - 场景 A：foreign `Ref` 作为 item body 内某 Node 的**输入**；
//! - 场景 B：foreign `Ref` 作为 each body 的**声明输出**（“Body 声明不属于该 Body 的 Ref”）。
//!
//! 两者都必须返回 `RunError::Definition`，且业务 Node 计数保持不变（失败无 Node）。
//! 清单该行 `primary`／`allowed` 为空：这是 Definition 构建期拒绝，不是 rustc 诊断。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Mark(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0))
}

/// 接收“上一次执行泄漏的 Each 成果”类型（`Vec<Mark>`）的节点。
async fn review(query: Query<&Vec<Mark>>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().len() as u32))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // 第一次执行：把 Each 的公开成果 Ref 带出定义闭包，作为 foreign Ref。
    let mut leaked: Option<Ref<Vec<Mark>>> = None;
    let first = block_on(runtime.execute::<_, Vec<Mark>, _>(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let marks = flow.each_output::<Item, Ref<Vec<Mark>>, _>(items, |sub| {
                let item = sub.ref_of::<Item>();
                let mark = sub.then_with_output(&touch, item);
                sub.output_with(mark);
            });
            leaked = Some(marks);
            flow.output_with(marks);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("first execution succeeds");
    assert_eq!(first.len(), 2, "first execution ran the body once per item");
    let foreign = leaked.expect("each output ref leaked");
    let calls_after_first = CALLS.load(Ordering::SeqCst);

    // 场景 A：foreign Ref 作为 item body 内 Node 的输入 → 任何 Node 之前拒绝。
    let error_a = block_on(runtime.execute::<_, Vec<Mark>, _>(
        move |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each_output::<Item, Ref<Vec<Mark>>, _>(items, |sub| {
                let reviewed = sub.then_with_output(&review, foreign);
                sub.output_with(reviewed);
            });
            flow.output_with(out_refs);
        },
        vec![Item(3)],
    ))
    .err()
    .expect("foreign Ref as an item body input is rejected before any Node runs");
    assert!(matches!(error_a, RunError::Definition(_)), "{error_a}");

    // 场景 B：foreign Ref 作为 each body 的声明输出 → 构建期拒绝。
    let error_b = block_on(runtime.execute::<_, Vec<Vec<Mark>>, _>(
        move |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each_output::<Item, Ref<Vec<Vec<Mark>>>, _>(items, |sub| {
                sub.output_with(foreign);
            });
            flow.output_with(out_refs);
        },
        vec![Item(4)],
    ))
    .err()
    .expect("foreign Ref as an each output is rejected");
    assert!(matches!(error_b, RunError::Definition(_)), "{error_b}");

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        calls_after_first,
        "rejected definitions started no business Node"
    );
    println!("we_t05 foreign input: {error_a}");
    println!("we_t05 foreign output: {error_b}");
    println!("we_t05-reject-ok");
}
