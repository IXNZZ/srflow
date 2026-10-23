//! V22-T04 夹具（承接 WE-T04 `we_t04_e02_retry_output_shape_mismatch.rs`）：`retry_output` 的
//! 声明形状与调用处标注的 `O` 不一致。
//!
//! 阶段变化（任务书 §9.1）：旧 API 下 `retry<O, F>` 的 `O` 就是闭包返回类型，二者不一致在 rustc
//! 编译期以 E0308 拒绝；新 unit Body 后声明由 `attempt.output_with(…)` 给出，同一不一致表现为
//! **Definition 构建期拒绝**（`RunError::Definition` 内含 `DeclarationShapeMismatch`：槽位、期望
//! 与实得类型），业务 Node 零调用。旧写法（闭包返回 Ref）的编译拒绝由
//! `v22_t04_e02_retry_output_mismatch_old_closure.rs` 另建承接，两件载体互不冒充。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-retry-shape-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Other(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn produce(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();
    let error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, amount: Ref<Amount>| {
            // O 标注为 `Ref<Other>`，而 child 声明的是 `Ref<Amount>`：Definition 形状不一致。
            let declared: Ref<Other> = flow.retry_output(3, |attempt| {
                let out_refs = attempt.then_with_output(&produce, amount);
                attempt.output_with(out_refs);
            });
            let _ = declared;
            let out_refs = flow.then_with_output(&produce, amount);
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .err()
    .expect("声明形状与 O 不一致的 retry_output 应在 Definition 构建期被拒绝");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert!(
        error.to_string().contains("Other"),
        "诊断应指出期望的 O 槽类型：{error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t04 retry output shape mismatch: {error}");
    println!("we_t04-retry-shape-reject-ok");
}
