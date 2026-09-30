//! T08 端到端场景的 Fake 业务实现（示例与集成测试共享）。
//!
//! 本文件**不是** crate 的公共 API：它只被 `examples/story_workflow.rs` 与
//! `tests/story_workflow.rs` 通过 `#[path = "..."] mod support;` 包含，因此不会把任何 SES 式
//! 业务模型加进 `src/` 的正式核心接口。
//!
//! 场景形状（DAG，不是直线；`投影` 均为字段投影，实际来源见每行标注）：
//!
//! ```text
//! Brief ─(投影 topic)→ Retry(GeneratePlan → CheckPlan 的 SubFlow) → PlanAttempt
//!   PlanAttempt ─(投影 plan)→ JudgeRoute → Route ─┐
//!   PlanAttempt ─(后继消费)→ MakeInitial → T0     │
//! Brief ─(投影 topic)→ Match 的业务 Input ────────┤
//! Match((Route, 业务 Input)) → 唯一分支 → Vec<KeySeed> → Each → Vec<KeyNode>
//! Brief ─(投影 topic)→ PlanRevision → RevisionPolicy  ←── 第三个独立位置
//! Iter((Vec<KeyNode>, T0)) → StoryState
//! Finalize：bind!{ topic←Brief、plan←StoryState、prose←StoryState、needs_revision←StoryState、
//!                  revision_rounds←RevisionPolicy } → FinalResult
//! ```
//!
//! # 复制成本（框架层，供 A07 核对）
//!
//! - **整值复用读取**（裸 `Ref` 作为 Binding）：若某位置只有 N 次这种读取，前 N−1 次克隆、
//!   最后一次移动；与字段投影／消费读取混合时须按实际读取顺序计算，不能套用该简式。本流程没有
//!   整值复用读取。
//! - **字段投影读取**（`field!`）：**每次**都只克隆被投影的字段并保留根值，无论是否为最后一次读取
//!   （见 `ValueStore::read_projected`）。因此作为投影根的 `Brief`、以及 Iter 最终输出位置里的
//!   `StoryState` 在最终装配时不会被取出，调用结束后随值存储一起丢弃；`StoryState` 进入 Iter
//!   和每轮 Body 时仍按值移动。
//! - 本流程的字符串字段克隆共 **7 次**：`brief.topic` 4 次（Retry／Match／PlanRevision／Finalize 的
//!   投影）、`attempt.plan` 1 次（JudgeRoute 的投影）、`state.plan` 1 次、`state.prose` 1 次（Finalize
//!   的投影）；`needs_revision` 是 `bool` 复制。这两个最终投影根既没有在该阶段被整值复制，也没有
//!   被投影读取移走，直到调用结束随值存储一起丢弃。
//! - **整值移动（零复制）**：`PlanAttempt`（交给 `MakeInitial`）、`Route`、`Vec<KeySeed>`、
//!   `Vec<KeyNode>`、`StoryState`（Iter 的 tuple consume），以及每轮 Body 内 `StoryState` 的传递。
//! - 正文关键路径 `KeySeed → Each → KeyNode → Iter Item → DraftSection → Review → 下一轮` 全程移动；
//!   正文只在最终装配处的投影里复制 1 次。
//! - 测试替身自己克隆的记录字符串（收到的 Item、节点文本）属于观察用，不计入框架复制。
#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use srflow::{
    Each, ExecutionError, Flow, FlowBuilder, Iter, Match, Node, Retry, RetryDecision, Runtime,
    bind, consume, field,
};

// ---------------------------------------------------------------- 业务数据
//
// 全部刻意不实现 `Clone`：控制器与 Binding 的接线不需要复制整个业务值，只有字段投影会复制被投影的
// 字段（`String`／`bool`）。

/// 父 Flow 的输入。
pub struct Brief {
    pub topic: String,
}

/// Retry 的输出：某一稿计划与它是否被接受。
pub struct PlanAttempt {
    pub plan: String,
    pub accepted: bool,
    pub attempt: usize,
}

/// 上游判断得到的路由值。Match 只消费它，不做判断。
#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    Quick,
    Deep,
    Unknown,
}

/// Match 分支产生的关键节点种子。
pub struct KeySeed(pub String);

/// Each 加工后的关键节点，同时也是 Iter 的 Item。
pub struct KeyNode(pub String);

/// Iter 跨轮携带的状态：`plan` 每轮都需要，`prose` 逐轮推进。
pub struct StoryState {
    pub plan: String,
    pub prose: String,
    pub needs_revision: bool,
}

/// 修订策略：来自**第三个独立位置**的业务值（与 Brief、StoryState 都不同的 `Ref`）。
pub struct RevisionPolicy {
    pub rounds: usize,
}

/// 父 Flow 的最终业务输出。
pub struct FinalResult {
    pub topic: String,
    pub plan: String,
    pub prose: String,
    pub needs_revision: bool,
    pub revision_rounds: usize,
}

/// Fake 侧的技术错误来源（计划失败、分支失败、加工失败、推进失败）。
#[derive(Debug)]
pub enum StoryFailure {
    Plan,
    Branch,
    Refine,
    Draft,
}

impl std::fmt::Display for StoryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fake story failure")
    }
}

impl std::error::Error for StoryFailure {}

// ---------------------------------------------------------------- 观察替身

/// 事件日志：核查顺序、唯一分支与"错误 child 未启动"。
#[derive(Default)]
pub struct Events(Mutex<Vec<String>>);

impl Events {
    pub fn push(&self, event: impl Into<String>) {
        self.0.lock().expect("events poisoned").push(event.into());
    }

    /// 逗号连接的事件序列。
    pub fn text(&self) -> String {
        self.0.lock().expect("events poisoned").join(",")
    }

    /// 以给定前缀开头的次数。
    pub fn count(&self, prefix: &str) -> usize {
        self.0
            .lock()
            .expect("events poisoned")
            .iter()
            .filter(|event| event.starts_with(prefix))
            .count()
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.0.lock().expect("events poisoned").clone()
    }
}

/// 场景的可控配置：同一流程可跑正常路径与各条失败路径。
pub struct StoryConfig {
    /// 第几稿被接受；`0` 表示从不接受（用于 Retry 耗尽）。
    pub accept_on_attempt: usize,
    /// Retry 上限。
    pub retry_limit: usize,
    /// 让 GeneratePlan 在第 N 轮返回技术错误（`None` = 不失败）。
    pub fail_plan_on: Option<usize>,
    /// 强制 JudgeRoute 产出的路由值（`None` = 按计划文本判断）。
    pub force_route: Option<Route>,
    /// 选中的 Match 分支返回技术错误。
    pub fail_branch: bool,
    /// 所有分支都产出空集合。
    pub empty_keys: bool,
    /// 是否登记 Match 的 default。
    pub with_default: bool,
    /// 把分支产出的种子顺序整体反转（顺序敏感性对照用；节点集合不变）。
    pub reverse_seeds: bool,
    /// RefineKey 在第 N 个 Item 上失败。
    pub fail_refine_on: Option<usize>,
    /// DraftSection 在第 N 轮失败。
    pub fail_draft_on: Option<usize>,
}

impl Default for StoryConfig {
    fn default() -> Self {
        Self {
            accept_on_attempt: 2,
            retry_limit: 3,
            fail_plan_on: None,
            force_route: None,
            fail_branch: false,
            empty_keys: false,
            with_default: true,
            reverse_seeds: false,
            fail_refine_on: None,
            fail_draft_on: None,
        }
    }
}

/// 一条构建好的端到端流程，以及它的观察句柄。
pub struct StoryWorkflow {
    pub flow: Flow<Brief, FinalResult>,
    pub events: Arc<Events>,
    /// Retry 实际执行的轮数（生成候选的次数）。
    pub plan_attempts: Arc<AtomicUsize>,
    /// Each 的 Body **实际收到**的原始 Item 序列（判别式证据：上一项 Output 未回流）。
    pub refined_seeds: Arc<Mutex<Vec<String>>>,
    /// Iter 的 Body 实际收到的 Item 序列。
    pub drafted_nodes: Arc<Mutex<Vec<String>>>,
}

// ---------------------------------------------------------------- Retry 的 Body

/// 生成候选计划。计数器只用于模拟"每轮生成不同候选"，不参与 Iter 的状态传递。
struct GeneratePlan {
    attempt_counter: Arc<AtomicUsize>,
    events: Arc<Events>,
    fail_on: Option<usize>,
}

impl Node for GeneratePlan {
    type Input = String;
    type Output = PlanAttempt;

    async fn run(&self, topic: String) -> Result<PlanAttempt, ExecutionError> {
        let attempt = self.attempt_counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.events.push(format!("plan({attempt})"));
        if self.fail_on == Some(attempt) {
            return Err(ExecutionError::new(StoryFailure::Plan));
        }
        Ok(PlanAttempt {
            plan: format!("{topic}的三幕计划 v{attempt}"),
            accepted: false,
            attempt,
        })
    }
}

/// 检查候选是否被接受；这是业务判断，Condition 只读取它的结果字段。
struct CheckPlan {
    accept_on_attempt: usize,
    events: Arc<Events>,
}

impl Node for CheckPlan {
    type Input = PlanAttempt;
    type Output = PlanAttempt;

    async fn run(&self, mut attempt: PlanAttempt) -> Result<PlanAttempt, ExecutionError> {
        attempt.accepted = self.accept_on_attempt > 0 && attempt.attempt >= self.accept_on_attempt;
        self.events.push(format!(
            "check({})",
            if attempt.accepted { "accept" } else { "reject" }
        ));
        Ok(attempt)
    }
}

/// Retry 的 Body：一个 SubFlow，每轮使用同一个原始 Input（主题）。
fn plan_subflow(
    config: &StoryConfig,
    events: &Arc<Events>,
    attempts: &Arc<AtomicUsize>,
) -> Flow<String, PlanAttempt> {
    let mut flow = FlowBuilder::<String>::new();
    let topic = flow.input();
    // 复用读取：`String` 可复制，主题位置在 Body 内不再使用。
    let generated = flow
        .then(
            GeneratePlan {
                attempt_counter: Arc::clone(attempts),
                events: Arc::clone(events),
                fail_on: config.fail_plan_on,
            },
            topic,
        )
        .expect("连接失败");
    // 消费读取：候选整值交给检查步骤。
    let checked = flow
        .then_move(
            CheckPlan {
                accept_on_attempt: config.accept_on_attempt,
                events: Arc::clone(events),
            },
            generated,
        )
        .expect("连接失败");
    flow.output(checked).expect("连接失败")
}

/// Condition：只读正常 Output 中的接受标记。
fn accept_when_checked(attempt: &PlanAttempt) -> RetryDecision {
    if attempt.accepted {
        RetryDecision::Stop
    } else {
        RetryDecision::Retry
    }
}

// ---------------------------------------------------------------- 路由与 Match 分支

/// 由已形成的计划文本判断路由值（业务判断在 Node，Match 不参与）。
struct JudgeRoute {
    forced: Option<Route>,
    events: Arc<Events>,
}

impl Node for JudgeRoute {
    type Input = String;
    type Output = Route;

    async fn run(&self, plan: String) -> Result<Route, ExecutionError> {
        let route = match &self.forced {
            Some(route) => match route {
                Route::Quick => Route::Quick,
                Route::Deep => Route::Deep,
                Route::Unknown => Route::Unknown,
            },
            None if plan.chars().count() >= 12 => Route::Deep,
            None => Route::Quick,
        };
        self.events.push(format!("route({route:?})"));
        Ok(route)
    }
}

/// Match 分支一（Node）：快速产出两个种子。
struct QuickKeys {
    empty: bool,
    fail: bool,
    reverse: bool,
    events: Arc<Events>,
}

impl Node for QuickKeys {
    type Input = String;
    type Output = Vec<KeySeed>;

    async fn run(&self, topic: String) -> Result<Vec<KeySeed>, ExecutionError> {
        self.events.push("keys(quick)");
        if self.fail {
            return Err(ExecutionError::new(StoryFailure::Branch));
        }
        if self.empty {
            return Ok(Vec::new());
        }
        let mut seeds = vec![
            KeySeed(format!("{topic}-开场")),
            KeySeed(format!("{topic}-冲突")),
        ];
        if self.reverse {
            seeds.reverse();
        }
        Ok(seeds)
    }
}

/// Match 分支二（Flow）的第一步：把主题扩成一个更细的骨架。
struct ExpandSeeds {
    empty: bool,
    events: Arc<Events>,
}

impl Node for ExpandSeeds {
    type Input = String;
    type Output = String;

    async fn run(&self, topic: String) -> Result<String, ExecutionError> {
        self.events.push("expand(deep)");
        if self.empty {
            return Ok(String::new());
        }
        Ok(format!("{topic}-转折#{topic}-高潮#{topic}-收束"))
    }
}

/// Match 分支二（Flow）的第二步：拆成种子集合。
struct SplitSeeds {
    fail: bool,
    reverse: bool,
    events: Arc<Events>,
}

impl Node for SplitSeeds {
    type Input = String;
    type Output = Vec<KeySeed>;

    async fn run(&self, skeleton: String) -> Result<Vec<KeySeed>, ExecutionError> {
        self.events.push("keys(deep)");
        if self.fail {
            return Err(ExecutionError::new(StoryFailure::Branch));
        }
        let mut seeds: Vec<KeySeed> = skeleton
            .split('#')
            .filter(|part| !part.is_empty())
            .map(|part| KeySeed(String::from(part)))
            .collect();
        if self.reverse {
            seeds.reverse();
        }
        Ok(seeds)
    }
}

/// Match 分支二：一个 SubFlow，与分支一同为 `String → Vec<KeySeed>` 契约。
fn deep_keys_flow(config: &StoryConfig, events: &Arc<Events>) -> Flow<String, Vec<KeySeed>> {
    let mut flow = FlowBuilder::<String>::new();
    let topic = flow.input();
    let expanded = flow
        .then(
            ExpandSeeds {
                empty: config.empty_keys,
                events: Arc::clone(events),
            },
            topic,
        )
        .expect("连接失败");
    let seeds = flow
        .then_move(
            SplitSeeds {
                fail: config.fail_branch,
                reverse: config.reverse_seeds,
                events: Arc::clone(events),
            },
            expanded,
        )
        .expect("连接失败");
    flow.output(seeds).expect("连接失败")
}

/// Match 的 default：未命中路径。
struct FallbackKeys {
    empty: bool,
    fail: bool,
    reverse: bool,
    events: Arc<Events>,
}

impl Node for FallbackKeys {
    type Input = String;
    type Output = Vec<KeySeed>;

    async fn run(&self, topic: String) -> Result<Vec<KeySeed>, ExecutionError> {
        self.events.push("keys(fallback)");
        if self.fail {
            return Err(ExecutionError::new(StoryFailure::Branch));
        }
        if self.empty {
            return Ok(Vec::new());
        }
        let mut seeds = vec![KeySeed(format!("{topic}-兜底"))];
        if self.reverse {
            seeds.reverse();
        }
        Ok(seeds)
    }
}

// ---------------------------------------------------------------- Each 的 Body

/// 逐项加工关键节点。记录实际收到的原始 Item，用于证明上一项 Output 未回流。
struct RefineKey {
    seen: Arc<Mutex<Vec<String>>>,
    events: Arc<Events>,
    fail_on: Option<usize>,
}

impl Node for RefineKey {
    type Input = KeySeed;
    type Output = KeyNode;

    async fn run(&self, seed: KeySeed) -> Result<KeyNode, ExecutionError> {
        let index = {
            let mut seen = self.seen.lock().expect("seeds poisoned");
            seen.push(seed.0.clone());
            seen.len()
        };
        self.events.push(format!("refine({index})"));
        if self.fail_on == Some(index) {
            return Err(ExecutionError::new(StoryFailure::Refine));
        }
        Ok(KeyNode(format!("{}(加工)", seed.0)))
    }
}

// ---------------------------------------------------------------- Iter 的 Body

/// Iter 的第一轮步骤：把当前节点的文本按顺序追加进正文。
struct DraftSection {
    drafted: Arc<Mutex<Vec<String>>>,
    events: Arc<Events>,
    fail_on: Option<usize>,
}

impl Node for DraftSection {
    type Input = (StoryState, KeyNode);
    type Output = StoryState;

    async fn run(&self, input: (StoryState, KeyNode)) -> Result<StoryState, ExecutionError> {
        let (mut state, node) = input;
        let round = {
            let mut drafted = self.drafted.lock().expect("nodes poisoned");
            drafted.push(node.0.clone());
            drafted.len()
        };
        self.events.push(format!("draft({round})"));
        if self.fail_on == Some(round) {
            return Err(ExecutionError::new(StoryFailure::Draft));
        }
        // 顺序敏感：正文按节点顺序累积，最终状态即可反证轮次顺序。
        state.prose.push_str(&node.0);
        state.prose.push('；');
        Ok(state)
    }
}

/// Iter 的第二轮步骤：给出正常业务状态（不是技术错误，也不会让迭代提前停止）。
struct Review {
    events: Arc<Events>,
}

impl Node for Review {
    type Input = StoryState;
    type Output = StoryState;

    async fn run(&self, mut state: StoryState) -> Result<StoryState, ExecutionError> {
        state.needs_revision = state.prose.chars().count() > 12;
        self.events.push(format!(
            "review({})",
            if state.needs_revision { "revise" } else { "ok" }
        ));
        Ok(state)
    }
}

/// Iter 的 Body：一个 SubFlow。
fn story_body_flow(
    config: &StoryConfig,
    events: &Arc<Events>,
    drafted: &Arc<Mutex<Vec<String>>>,
) -> Flow<(StoryState, KeyNode), StoryState> {
    let mut flow = FlowBuilder::<(StoryState, KeyNode)>::new();
    let input = flow.input();
    let drafted_state = flow
        .then_move(
            DraftSection {
                drafted: Arc::clone(drafted),
                events: Arc::clone(events),
                fail_on: config.fail_draft_on,
            },
            input,
        )
        .expect("连接失败");
    let reviewed = flow
        .then_move(
            Review {
                events: Arc::clone(events),
            },
            drafted_state,
        )
        .expect("连接失败");
    flow.output(reviewed).expect("连接失败")
}

// ---------------------------------------------------------------- 组装父 Flow

/// 由主题决定修订策略。它的 Output 是**第三个独立位置**，供 Finalize 的结构性装配使用。
struct PlanRevision {
    events: Arc<Events>,
}

impl Node for PlanRevision {
    type Input = String;
    type Output = RevisionPolicy;

    async fn run(&self, topic: String) -> Result<RevisionPolicy, ExecutionError> {
        self.events.push("policy");
        Ok(RevisionPolicy {
            rounds: if topic.chars().count() > 3 { 2 } else { 1 },
        })
    }
}

/// 由业务数据构造 Iter 的初始状态（Node 的正常职责：构造业务结构，而不是纯投影）。
struct MakeInitial;

impl Node for MakeInitial {
    type Input = PlanAttempt;
    type Output = StoryState;

    async fn run(&self, attempt: PlanAttempt) -> Result<StoryState, ExecutionError> {
        Ok(StoryState {
            plan: attempt.plan,
            prose: String::new(),
            needs_revision: false,
        })
    }
}

/// Finalize 的结构性命名 Input：五个字段、**三个数据位置**（Brief、StoryState、RevisionPolicy）。
struct FinalizeInput {
    topic: String,
    plan: String,
    prose: String,
    needs_revision: bool,
    policy: RevisionPolicy,
}

struct Finalize;

impl Node for Finalize {
    type Input = FinalizeInput;
    type Output = FinalResult;

    async fn run(&self, input: FinalizeInput) -> Result<FinalResult, ExecutionError> {
        Ok(FinalResult {
            topic: input.topic,
            plan: input.plan,
            prose: input.prose,
            needs_revision: input.needs_revision,
            revision_rounds: input.policy.rounds,
        })
    }
}

/// 构建端到端流程。
pub fn build_story_workflow(config: StoryConfig) -> StoryWorkflow {
    let events = Arc::new(Events::default());
    let plan_attempts = Arc::new(AtomicUsize::new(0));
    let refined_seeds = Arc::new(Mutex::new(Vec::new()));
    let drafted_nodes = Arc::new(Mutex::new(Vec::new()));

    // 先构建所有借用 `config` 的子流程：`force_route` 之后会被移动给 JudgeRoute。
    let plan_body = plan_subflow(&config, &events, &plan_attempts);
    let deep_branch = deep_keys_flow(&config, &events);
    let story_body = story_body_flow(&config, &events, &drafted_nodes);

    // Match：两个具体类型不同、契约一致的分支，外加可选 default。
    let mut builder = Match::<Route, String, Vec<KeySeed>>::builder();
    builder
        .case(
            Route::Quick,
            QuickKeys {
                empty: config.empty_keys,
                fail: config.fail_branch,
                reverse: config.reverse_seeds,
                events: Arc::clone(&events),
            },
        )
        .expect("登记失败");
    builder.case(Route::Deep, deep_branch).expect("登记失败");
    if config.with_default {
        builder
            .default(FallbackKeys {
                empty: config.empty_keys,
                fail: config.fail_branch,
                reverse: config.reverse_seeds,
                events: Arc::clone(&events),
            })
            .expect("登记失败");
    }
    let matcher = builder.build();

    let mut flow = FlowBuilder::<Brief>::new();
    let brief = flow.input();

    // 1) Retry：Body 是 GeneratePlan → CheckPlan 的 SubFlow，每轮用同一个原始 Input（主题）。
    let attempt = flow
        .then(
            Retry::with_limit(
                plan_body,
                accept_when_checked,
                NonZeroUsize::new(config.retry_limit).expect("上限非零"),
            ),
            // 字段投影：只复制 topic，根结构 Brief 不复制。
            field!(brief.topic),
        )
        .expect("连接失败");

    // 2) 路由值：复用同一 PlanAttempt 的计划文本（投影），判断在 Node 里。
    let route = flow
        .then(
            JudgeRoute {
                forced: config.force_route,
                events: Arc::clone(&events),
            },
            field!(attempt.plan),
        )
        .expect("连接失败");

    // 3) Match：Route 与业务 Input 来自两个不同位置，消费读取 Route。
    let seeds = flow
        .then(matcher, (consume(route), field!(brief.topic)))
        .expect("连接失败");

    // 4) 第三个独立位置：修订策略（与 Brief、StoryState 不同的 Ref）。
    let policy = flow
        .then(
            PlanRevision {
                events: Arc::clone(&events),
            },
            field!(brief.topic),
        )
        .expect("连接失败");

    // 5) Each：逐项加工，不需要上一项 Output。
    let nodes = flow
        .then_move(
            Each::new(RefineKey {
                seen: Arc::clone(&refined_seeds),
                events: Arc::clone(&events),
                fail_on: config.fail_refine_on,
            }),
            seeds,
        )
        .expect("连接失败");

    // 6) Iter 的初始状态：由业务数据构造；这里**后继消费**同一个 PlanAttempt 位置。
    let initial = flow.then_move(MakeInitial, attempt).expect("连接失败");

    // 7) Iter：Item 来自 Each 的集合元素，T0 来自另一个位置 → 双位置消费 tuple。
    let state = flow
        .then(Iter::new(story_body), (consume(nodes), consume(initial)))
        .expect("连接失败");

    // 8) 结构性命名装配：**三个位置**（Brief、RevisionPolicy、StoryState）、五个字段。
    let report = flow
        .then(
            Finalize,
            bind!(FinalizeInput {
                topic: field!(brief.topic),
                plan: field!(state.plan),
                prose: field!(state.prose),
                needs_revision: field!(state.needs_revision),
                policy: consume(policy),
            }),
        )
        .expect("连接失败");
    let flow = flow.output(report).expect("连接失败");

    StoryWorkflow {
        flow,
        events,
        plan_attempts,
        refined_seeds,
        drafted_nodes,
    }
}

/// 示例与测试共用的驱动入口：跑一次完整流程。
pub fn run_story_workflow(
    workflow: &StoryWorkflow,
    topic: &str,
) -> Result<FinalResult, ExecutionError> {
    let runtime = Runtime::new();
    futures::executor::block_on(runtime.execute(
        &workflow.flow,
        Brief {
            topic: String::from(topic),
        },
    ))
}
