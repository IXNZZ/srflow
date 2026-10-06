//! Match Orchestrator：已有路由 Data → key 等值 lookup → 单个 branch 的真实调用。
//!
//! 语义边界（v2.1 §10、Core §13、Runtime §15）：
//! - 路由是**已有 Data**：`Match` 只做 key 等值查找，不计算业务判断；复杂判断先由 Node
//!   产出路由 Data。
//! - 一次调用只执行**一个** branch。未选 branch 只有 Definition：不建立运行 Scope、不调用
//!   Node／Flow、不产生 Data 或副作用。
//! - 只有登记了 default，未命中才走 default；没有可选项（含空登记表）是执行错误。
//! - 被选 branch 失败直接传播，不改选其他 branch 或 default。
//! - 两层出口都由真实调用边界整组提交：BranchScope → MatchScope 由 branch site 的
//!   `OrchSite` Export；MatchScope → caller 由调用本 Match 的上层边界 Export。Match body
//!   不知道上层 caller 输出位置，也不自行绑定 caller RefId。
//!
//! 内部结构：
//! - 构建态 [`MatchBuilder`] 只声明 `R`／`A` 两个输入；每个 branch 是**包装**：
//!   `FlowBuilder::<(A,)>` + `then(callable)` + `finish(choice)`，即一个完成态单输入 Flow。
//!   业务 Node 的真实调用仍是 `CallSite::Node`（不给 Node 增加子编排协议），完整 child
//!   Flow 仍是 `CallSite::Orchestrator`。
//! - 完成态 [`Match`] 持不可变 Definition 与私有登记表（擦除后的 `CallSite` 与 key 元数据）。
//!   登记表**不写入** Match Definition 的 `steps`／`produced`：否则 `run_steps` 会顺序执行
//!   全部 branch，共同输出端口也会变成合法接线来源。
//! - 共同输出端口由 [`Definition::declare_common_outputs`] **一次** checked 整组分配后批量
//!   登记，只进入 `output_ports`；每个 branch site 的 caller 输出都指向这一组同一位置。
//!   每次实际调用只运行一个 branch，因此在 MatchScope 中每个共同位置只绑定一次。
//! - 选择执行经 [`OrchScope::run_registered_site`]，只接受本登记表的索引；执行前逐项核对
//!   输入／输出归属与登记元数据，拒绝发生在任何 child 建立或业务体运行之前。
//! - 路由借用只在局部共享重借用块内存在：`Targets2::first` 返回的 `&R` 在取得选中索引后
//!   结束，随后才进入可变的选择调用。构建期 key 是用户提供的 `R` 配置值，运行期以 `&R`
//!   比较，不复制／move 路由 Data，也不要求 `R: Clone`／`Hash`。
//!
//! 运行态字段禁止项：完成态不缓存 Execution、ScopeId、DataId、借用、选择结果或任何业务
//! 可变状态；登记表只保存构建期元数据。

use std::marker::PhantomData;
use std::sync::Arc;

use super::builder::{BuildSite, CallSite, Definition, IntoCallSite, TypedCallBuilder};
use super::context::BodyError;
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder, FlowOutput};
use super::internal_error::ScopeError;
use super::orchestrator::{OrchCall, OrchScope, OrchSite, ScopeRole, Targets2};
use super::ref_id::RefId;
use super::signature::{BuildError, DeclaredPort, NodeFut, OutKind, Wiring};

/// 无匹配且没有 default 时的执行错误说明。
const UNMATCHED_ROUTE_NOTE: &str = "match has no branch for the route and no default";

/// 一个已完成包装 Definition 的对象身份（同一 `Arc<Definition>` 目标地址）。
///
/// 只用于"实际被执行的 child 是否就是登记的 branch"这一内部一致性判断；不构成全局身份表，
/// 也不跨 Execution 保存。包装在整个登记生命周期内保持存活，因此该地址不会被复用。
fn branch_identity<I, K>(wrapper: &Flow<I, K>) -> *const ()
where
    I: super::flow::FlowInputs + super::signature::InputTypes,
    K: OutKind,
    I::Pack: super::orchestrator::PackFor<I>,
{
    wrapper.raw_definition() as *const Definition as *const ()
}

/// branch 包装内部那一次真实调用的类别。
///
/// 它在登记时由接线 Marker 决定，属于擦除后的调用元数据（不参与执行决策）：业务 Node 的
/// 真实调用仍是 `CallSite::Node`，完整 child Orchestrator 仍是 `CallSite::Orchestrator`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchStepKind {
    /// 业务 Node：BranchScope 内直接是叶子调用，不额外建立 Scope。
    Node,
    /// 完整 child Orchestrator（例如完成态 Flow）：在 BranchScope 内建立自己的 Scope。
    Orchestrator,
}

/// 一个可选路径的构建期配置：key（default 为 `None`）＋其包装 Definition 的声明输出端口。
///
/// 声明端口在这里是**类型证据**：运行期用它核对 branch 的声明与 Match 共同端口一致。
struct RegisteredBranch<R, A, K> {
    key: Option<R>,
    wrapper: Flow<(A,), K>,
    /// 包装 Definition 的声明输出端口（登记时从真实包装捕获，完成时整组校验）。
    ports: Vec<DeclaredPort>,
    /// 包装 Definition 的对象身份（登记时捕获：运行期必须仍是同一个对象）。
    definition_identity: *const (),
    step_kind: BranchStepKind,
}

/// 登记条目：key、声明输出端口与包装内部调用类别（与 `Match::sites` 下标一一对应）。
struct Alternative<R> {
    key: Option<R>,
    ports: Vec<DeclaredPort>,
    /// 登记时那个包装 Definition 的对象身份（同一 `Arc<Definition>` 目标地址）。
    ///
    /// 它让运行期能证明"实际被执行的 child 就是登记的 branch"：即使另一个 Definition 的
    /// 输入／输出类型完全相同、端口类型也相同，只要来源对象不同就会被拒绝。Unit 分支没有
    /// 输出端口，因此来源身份必须独立于端口列表。
    definition_identity: *const (),
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    step_kind: BranchStepKind,
}

/// Match 构建态：声明 `R`／`A` 输入、登记 key／default，但**不实现** Orchestrator 协议。
///
/// 未完成的构建态既不能被执行，也不能作为 child 接线；只有 [`Self::finish`] 产出
/// [`Match`] 之后才成立。
#[allow(clippy::type_complexity)] // Marker 只编码 (R, A, K) 三个类型的零成本占位
pub(crate) struct MatchBuilder<R, A, K> {
    definition: Definition,
    /// `R` 输入位置（路由）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    route_position: RefId,
    /// `A` 输入位置（branch 业务输入）。
    input_position: RefId,
    registered: Vec<RegisteredBranch<R, A, K>>,
    default_index: Option<usize>,
    marker: PhantomData<fn() -> (R, A, K)>,
}

impl<R: 'static + Eq, A: 'static, K: OutKind> MatchBuilder<R, A, K> {
    /// 建立 Match：按 `(R, A)` 顺序声明两个非空输入位置。
    pub(crate) fn start() -> Result<Self, BuildError> {
        let mut definition = Definition::new();
        let route = definition.declare_input::<R>("route")?;
        let input = definition.declare_input::<A>("input")?;
        Ok(Self {
            definition,
            route_position: route.position().clone(),
            input_position: input.position().clone(),
            registered: Vec::new(),
            default_index: None,
            marker: PhantomData,
        })
    }

    /// 登记一个 key 命中 branch。
    ///
    /// key 是构建期由用户提供的 `R` 配置值；同一条 key 不能登记两次。重复 key 在本次
    /// 分支装配之前拒绝，且不改变已登记条目。
    pub(crate) fn branch<C, M>(&mut self, key: R, callable: C) -> Result<(), BuildError>
    where
        C: BuildSite<M, DataRef<A>> + IntoCallSite<M, DataRef<A>, BuildOutput = M::BuildOutput>,
        M: Wiring,
        M::BuildOutput: FlowOutput<K>,
    {
        self.register(Some(key), callable)
    }

    /// 登记 default：只在未命中任何 key 时使用；最多一个。
    pub(crate) fn default<C, M>(&mut self, callable: C) -> Result<(), BuildError>
    where
        C: BuildSite<M, DataRef<A>> + IntoCallSite<M, DataRef<A>, BuildOutput = M::BuildOutput>,
        M: Wiring,
        M::BuildOutput: FlowOutput<K>,
    {
        self.register(None, callable)
    }

    /// 登记一个 branch／default 的公共路径。
    fn register<C, M>(&mut self, key: Option<R>, callable: C) -> Result<(), BuildError>
    where
        C: BuildSite<M, DataRef<A>> + IntoCallSite<M, DataRef<A>, BuildOutput = M::BuildOutput>,
        M: Wiring,
        M::BuildOutput: FlowOutput<K>,
    {
        // 1. key／default 唯一性先于本次分支装配检查；失败不改动已登记条目。
        if let Some(candidate) = key.as_ref() {
            if self
                .registered
                .iter()
                .any(|registered| registered.key.as_ref() == Some(candidate))
            {
                return Err(BuildError::DuplicateBranchKey);
            }
        } else if self.default_index.is_some() {
            return Err(BuildError::SecondDefault);
        }
        let is_default = key.is_none();
        // 2. 临时包装：输入类型、输出分类与 unit 拒绝都由既有 typed then／finish 完成。
        let (mut wrapper, input) = FlowBuilder::<(A,)>::start()?;
        let choice = wrapper.then(callable, input)?;
        let flow = wrapper.finish(choice)?;
        // 3. 不可失败登记：只改本构建器的登记表，不动 Match Definition 的 steps／produced。
        let step_kind = if M::ORCHESTRATOR {
            BranchStepKind::Orchestrator
        } else {
            BranchStepKind::Node
        };
        let ports = flow.definition().output_ports().to_vec();
        let definition_identity = branch_identity(&flow);
        let index = self.registered.len();
        self.registered.push(RegisteredBranch {
            key,
            wrapper: flow,
            ports,
            definition_identity,
            step_kind,
        });
        if is_default {
            self.default_index = Some(index);
        }
        Ok(())
    }

    /// 完成 Match：整组校验各 branch 的共同 `K`，再一次性装配共同端口并发布不可变完成态。
    ///
    /// 顺序固定为：全部可失败校验 → 一次 [`Definition::declare_common_outputs`]（整组 checked
    /// 分配 + 批量登记）→ 不可失败地装配各 alternative 的 caller 位置与登记表。失败时不产生
    /// Match、不登记任何共同端口、不消耗共同 Ref 序号；空登记表是合法的完成态（调用时报无匹配）。
    pub(crate) fn finish(mut self) -> Result<Match<R, A, K>, BuildError> {
        let expected = <K as OutKind>::port_types();
        // 1. 整组校验 branch 的共同 Output Signature（结构上已由同一个 `K` 保证，这里是完成
        //    装配的防御性校验，全部发生在任何分配之前）。
        for (branch, registered) in self.registered.iter().enumerate() {
            let ports = &registered.ports;
            if ports.len() != expected.len() {
                return Err(BuildError::BranchOutputArity {
                    branch,
                    expected: expected.len(),
                    supplied: ports.len(),
                });
            }
            for (position, (port, (expected_name, expected_type))) in
                ports.iter().zip(expected.iter()).enumerate()
            {
                if port.expected() != *expected_type {
                    return Err(BuildError::BranchOutputType {
                        branch,
                        position,
                        expected: expected_name,
                        actual: port.expected_name(),
                    });
                }
            }
        }
        // 2. 一次整组 checked 分配 + 批量登记共同端口（Unit 为零，Data 一位，Out2 两位）。
        let common = self.definition.declare_common_outputs::<K>()?;
        // 3. 以下路径不可失败：把每个包装装配成受控调用点并登记到**本 Match Definition
        //    自己的私有登记表**；完成后不再有可恢复 BuildError 分支。
        let mut entries = Vec::with_capacity(self.registered.len());
        for registered in self.registered {
            let RegisteredBranch {
                key,
                wrapper,
                ports,
                definition_identity,
                step_kind,
            } = registered;
            let site = CallSite::Orchestrator(Box::new(OrchSite::<Flow<(A,), K>, (A,), K>::new(
                wrapper,
                vec![self.input_position.clone()],
                common.clone(),
                ScopeRole::Branch,
            )));
            self.definition.register_alternative(site);
            entries.push(Alternative {
                key,
                ports,
                definition_identity,
                step_kind,
            });
        }
        #[allow(clippy::arc_with_non_send_sync)]
        // 单线程、非 Send 执行模型：只共享不可变定义与登记表
        Ok(Match {
            definition: Arc::new(self.definition),
            entries: Arc::new(entries),
            default_index: self.default_index,
            marker: PhantomData,
        })
    }
}

#[cfg(test)]
impl<R: 'static + Eq, A: 'static, K: OutKind> MatchBuilder<R, A, K> {
    /// 本次构建已分配的位置数量（失败不消耗序号）。
    pub(crate) fn allocated_probe(&self) -> u64 {
        self.definition.allocated_probe()
    }

    /// 已登记条目数量。
    pub(crate) fn registered_probe(&self) -> usize {
        self.registered.len()
    }

    /// 测试构造：登记一个**已经完成**的包装 Flow（M10 的合法双输出 imported alias 组合）。
    ///
    /// 走与 [`MatchBuilder::branch`] 相同的登记与装配路径（同样的端口捕获、同样的 `OrchSite`
    /// 装配、同样的完成期整组校验），只是包装由调用方手工构建——例如把唯一业务输入同时接到
    /// child 的两个输入位置，使两个输出端口指向同一完整 `DataId`。
    pub(crate) fn register_wrapper_probe(
        &mut self,
        key: R,
        wrapper: Flow<(A,), K>,
    ) -> Result<(), BuildError> {
        if self
            .registered
            .iter()
            .any(|registered| registered.key.as_ref() == Some(&key))
        {
            return Err(BuildError::DuplicateBranchKey);
        }
        let step_kind = match wrapper.definition().steps().first().map(|step| step.site()) {
            Some(CallSite::Orchestrator(_)) => BranchStepKind::Orchestrator,
            _ => BranchStepKind::Node,
        };
        let ports = wrapper.definition().output_ports().to_vec();
        let definition_identity = branch_identity(&wrapper);
        self.registered.push(RegisteredBranch {
            key: Some(key),
            wrapper,
            ports,
            definition_identity,
            step_kind,
        });
        Ok(())
    }

    /// 测试观测：某个已登记 branch 的声明端口证据（M14 对照用）。
    pub(crate) fn branch_ports_probe(&self, index: usize) -> &[DeclaredPort] {
        &self.registered[index].ports
    }

    /// 测试观测：覆盖某个已登记 branch 的声明端口证据（构造"后项类型错"的完成期拒绝样本）。
    pub(crate) fn override_branch_ports_probe(&mut self, index: usize, ports: Vec<DeclaredPort>) {
        self.registered[index].ports = ports;
    }

    /// 测试构造：以指定 `RefId` 来源建立 MatchBuilder（验证共同端口整组分配的原子性）。
    pub(crate) fn with_source(source: Arc<super::ref_id::RefIdSource>) -> Self {
        let mut definition = Definition::new_with_source(source);
        let route = definition
            .declare_input::<R>("route")
            .expect("a fresh source accepts a route position");
        let input = definition
            .declare_input::<A>("input")
            .expect("a fresh source accepts an input position");
        Self {
            definition,
            route_position: route.position().clone(),
            input_position: input.position().clone(),
            registered: Vec::new(),
            default_index: None,
            marker: PhantomData,
        }
    }
}

/// 完成态 Match：不可变 Declaration + 私有登记表；不缓存任何运行态身份或业务状态。
///
/// 可作为 Root 或 child 使用；`Clone` 只共享不可变定义，不复制业务 Data。
#[allow(clippy::type_complexity)] // Marker 只编码 (R, A, K) 三个类型的零成本占位
pub struct Match<R, A, K> {
    /// 不可变定义：**同时持有自己的私有可选路径登记表**（`Definition::alternatives`）。
    ///
    /// 受控选择入口只从这张表按索引取调用点，因此不需要、也不允许把另一张表传进执行面。
    definition: Arc<Definition>,
    /// 与登记表下标一一对应的 key 元数据与构建期声明证据。
    entries: Arc<Vec<Alternative<R>>>,
    default_index: Option<usize>,
    marker: PhantomData<fn() -> (R, A, K)>,
}

impl<R, A, K> std::fmt::Debug for Match<R, A, K> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 只报告定义规模：完成态不携带业务值，也不要求 `R`／`A` 实现 `Debug`。
        formatter
            .debug_struct("Match")
            .field("branches", &self.entries.len())
            .field("outputs", &self.definition.output_ports().len())
            .field("default", &self.default_index.is_some())
            .finish()
    }
}

impl<R, A, K> Clone for Match<R, A, K> {
    fn clone(&self) -> Self {
        // 只共享不可变定义（含登记表）：不复制业务 Data，也不要求 R／A／O Clone。
        Self {
            definition: Arc::clone(&self.definition),
            entries: Arc::clone(&self.entries),
            default_index: self.default_index,
            marker: PhantomData,
        }
    }
}

#[cfg(test)]
impl<R, A, K> Match<R, A, K> {
    /// 已登记的 alternative 调用点（M15 用于构造"外来／错误元数据"注入样本）。
    pub(crate) fn sites_probe(&self) -> &[CallSite] {
        self.definition.alternatives()
    }

    /// 用另一个调用点替换指定替代（M15 归属／类型不符样本）；必须在任何 `Clone` 之前调用。
    #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型
    pub(crate) fn with_replaced_site_probe(self, index: usize, site: CallSite) -> Self {
        let Match {
            definition,
            entries,
            default_index,
            marker,
        } = self;
        let mut definition = match Arc::try_unwrap(definition) {
            Ok(definition) => definition,
            Err(_) => panic!("probe runs before the match is cloned"),
        };
        definition.replace_alternative_probe(index, site);
        Match {
            definition: Arc::new(definition),
            entries,
            default_index,
            marker,
        }
    }

    /// 用另一组声明端口替换指定替代的类型证据（M15 声明不符样本）。
    #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型
    pub(crate) fn with_ports_probe(self, index: usize, ports: Vec<DeclaredPort>) -> Self {
        let Match {
            definition,
            entries,
            default_index,
            marker,
        } = self;
        let mut entries = match Arc::try_unwrap(entries) {
            Ok(entries) => entries,
            Err(_) => panic!("probe runs before the match is cloned"),
        };
        entries[index].ports = ports;
        Match {
            definition,
            entries: Arc::new(entries),
            default_index,
            marker,
        }
    }

    /// 登记条目数量。
    pub(crate) fn registered_probe(&self) -> usize {
        self.entries.len()
    }

    /// 各替代包装内部的真实调用类别（M03 对照用）。
    pub(crate) fn branch_steps_probe(&self) -> Vec<BranchStepKind> {
        self.entries.iter().map(|entry| entry.step_kind).collect()
    }
}

impl<R, A, K> OrchCall<(R, A), K> for Match<R, A, K>
where
    R: 'static + Eq,
    A: 'static,
    K: OutKind,
{
    type Pack = Targets2<R, A>;

    /// Match 在被上层接线调用时建立的 child Scope 角色是 MatchScope。
    const ROLE: ScopeRole = ScopeRole::Match;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, K>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 1. 路由借用块：`Targets2::first` 经真实 Scope 解析返回 `&R`；取得选中索引后
            //    立即结束该共享重借用，之后才进入可变的选择调用（路由借用不跨 await）。
            let selected = {
                let shared = scope.ctx_probe();
                let child = scope.child().clone();
                match scope.pack().first(shared, &child) {
                    Ok(route) => self.lookup(route),
                    Err(error) => return Err(BodyError::from(error)),
                }
            };
            let Some(index) = selected else {
                return Err(self.report_unmatched(&scope));
            };
            // 2. 元数据核对：必须在建立任何 child 或运行任何 body 之前完成。
            self.verify_alternative(&scope, index)?;
            // cfg(test) 预占端口：只用于构造"真实 Export 提交前失败"样本（M17）。
            #[cfg(test)]
            if let Some(port) = super::test_support::take_export_conflict()
                && let Some(port) = self.definition.output_ports().get(port)
            {
                scope.prebind_probe(port.position(), 0)?;
            }
            #[cfg(test)]
            super::test_support::match_stage_record(
                "before-branch",
                scope.child().clone(),
                scope.ctx_probe(),
            );
            // 3. 受控选择执行：只运行本登记表的这一个调用点。
            let outcome = scope.run_registered_site(index).await;
            #[cfg(test)]
            match &outcome {
                Ok(()) => super::test_support::match_stage_record(
                    "body-ok",
                    scope.child().clone(),
                    scope.ctx_probe(),
                ),
                Err(_) => super::test_support::match_stage_record(
                    "after-branch",
                    scope.child().clone(),
                    scope.ctx_probe(),
                ),
            }
            outcome
        })
    }
}

impl<R: 'static + Eq, A: 'static, K> Match<R, A, K> {
    /// key 等值查找：命中唯一登记条目；未命中时回退到 default（若有）。
    ///
    /// 比较按共享借用进行：不复制 key、不要求 `R: Clone`／`Hash`，也不触碰路由 Data 的持有者。
    fn lookup(&self, route: &R) -> Option<usize> {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.key.as_ref() == Some(route))
        {
            return Some(index);
        }
        self.default_index
    }

    /// 无匹配且没有 default：报告执行错误。
    ///
    /// 返回前按 `cfg(test)` 只读记录本 MatchScope 在**真实失败时刻**的绑定与责任快照，
    /// 供"没有自行生成输出"的证据使用；记录不改变清理顺序，也不恢复调用。
    fn report_unmatched(&self, scope: &OrchScope<'_, Targets2<R, A>, K>) -> BodyError
    where
        K: OutKind,
    {
        #[cfg(test)]
        {
            if let Ok((refs, owned)) = scope.ctx_probe().snapshot_probe(scope.child()) {
                super::test_support::match_failure_scope_record(scope.child().clone(), refs, owned);
            }
        }
        let _ = scope;
        BodyError::new(UNMATCHED_ROUTE_NOTE)
    }

    /// 核对选中替代的归属与声明：索引、A 输入精确位置、共同输出端口，
    /// 以及**实际将被调用的 child 对象**的真实输入／输出 Signature。
    ///
    /// 校验对象是 `scope` 当前 inner Definition 自己的登记表条目与它指向的真实 child，
    /// 不使用任何在别处复制的类型声明；拒绝都是内部不变量错误，在 child／body 之前返回，
    /// 随后由调用边界的 `failed_with` 保存首次诊断（含本次 MatchScope）。
    fn verify_alternative(
        &self,
        scope: &OrchScope<'_, Targets2<R, A>, K>,
        index: usize,
    ) -> Result<(), BodyError>
    where
        K: OutKind,
    {
        let violated = |violated: &'static str| BodyError::from(ScopeError::Invariant { violated });
        let Some(entry) = self.entries.get(index) else {
            return Err(violated("registered alternative index is out of range"));
        };
        // 实际将被执行的调用点由当前 inner Definition 提供：调用者无法传入外来表。
        let Some(site) = scope.registered_alternative(index) else {
            return Err(violated("registered alternative index is out of range"));
        };
        let CallSite::Orchestrator(site) = site else {
            return Err(violated("registered alternative is not a branch wrapper"));
        };
        // A 输入必须**精确**是本 Match 的第二个声明输入位置（不含路由 R）。
        let Some(input_port) = self.definition.inputs().get(1) else {
            return Err(violated("match definition declares no branch input"));
        };
        if site.inputs() != std::slice::from_ref(input_port.position()) {
            return Err(violated(
                "registered alternative input is not the match branch input position",
            ));
        }
        // caller 输出必须逐位置等于本 Match 的共同输出端口（数量、顺序与完整 RefId 身份）。
        let common: Vec<RefId> = self
            .definition
            .output_ports()
            .iter()
            .map(|port| port.position().clone())
            .collect();
        if site.outputs() != common.as_slice() {
            return Err(violated(
                "registered alternative output is not the common match output ports",
            ));
        }
        // 实际被执行的 child 必须就是登记的那个包装对象：类型相同但来自另一个 Definition
        // 的替换在 body 之前拒绝（Unit 分支没有输出端口，因此来源身份独立于端口列表）。
        let child = site.inner_definition();
        if child as *const Definition as *const () != entry.definition_identity {
            return Err(violated(
                "registered alternative is not the registered branch definition",
            ));
        }
        let child_inputs = child.inputs();
        if child_inputs.len() != 1 || child_inputs[0].expected() != input_port.expected() {
            return Err(violated(
                "registered alternative child input is not the match branch input",
            ));
        }
        let child_ports = child.output_ports();
        if child_ports.len() != common.len()
            || child_ports
                .iter()
                .zip(self.definition.output_ports())
                .any(|(child_port, common_port)| child_port.expected() != common_port.expected())
        {
            return Err(violated(
                "registered alternative declaration does not match the common output ports",
            ));
        }
        // 构建期证据必须与实际被调用对象逐位置一致（**完整 RefId 身份**与类型）：不允许
        // 陈旧元数据、也不允许同类型但另一来源的端口通过前置检查。
        if entry.ports.len() != child_ports.len()
            || entry.ports.iter().zip(child_ports).any(|(recorded, live)| {
                recorded.position() != live.position() || recorded.expected() != live.expected()
            })
        {
            return Err(violated(
                "registered alternative metadata does not match the invoked branch",
            ));
        }
        Ok(())
    }
}
