//! 同步定义上下文 `Flow`（SPEC §4～§7 的定义期）。
//!
//! 公开类型只带一个 `'n`：Node 句柄与定义结构按 `'n` 借用，构建元数据由内部可变
//! 状态承载，不向调用者暴露 builder lifetime。`Flow` 不持有本次执行的业务值，也不
//! 提供公开 build／finish 入口。

use crate::core::engine::choice::Choice;
use crate::core::engine::data::Data;
use crate::core::engine::definition::{
    BodyRole, Builder, ChooseCase, ChooseContract, ChooseStep, DeclarationKind, DeclarationState,
    EachBridge,
};
use crate::core::engine::error::BuildErrorDetail;
use crate::core::engine::identity::{BodyId, PositionHandle};
use crate::core::engine::node::{AutoCallable, Callable, ProvideNode, StructCall};
use crate::core::engine::shape::{
    EachCollect, EachShape, InputOps, NodeOutput, OutputOps, PositionDesc, Ref, RefShape,
    RootInput, ShapeOps, StateShape, TypeDesc,
};
use std::cell::RefCell;
use std::rc::Rc;

/// 同步 Definition 上下文；Node 登记可以借用 `'n` 寿命的句柄。
pub struct Flow<'n> {
    builder: Rc<RefCell<Builder<'n>>>,
    body: BodyId,
}

impl<'n> Flow<'n> {
    /// 在指定 Body 上打开定义上下文（框架内部构建路径）。
    pub(crate) fn open(builder: Rc<RefCell<Builder<'n>>>, body: BodyId) -> Self {
        Self { builder, body }
    }

    /// 自动输入的普通形式：重复检查 → 逐槽最近来源 → 共同登记；返回原 Flow 以便继续链式登记。
    ///
    /// 输入依赖由类型系统连接（形状与 Node 声明一致才实现 [`AutoCallable`]），位置来源与
    /// 词法可见性由 Definition 验证覆盖。与 [`Flow::then_output`] 登记相同数量、顺序、输入、
    /// 输出与 callable 的 NodeStep，只在是否把输出句柄交给编排代码上有区别。
    #[allow(private_bounds)]
    pub fn then<N, K>(&mut self, node: N) -> &mut Self
    where
        N: AutoCallable<K> + 'n,
        K: 'static,
        <N as AutoCallable<K>>::Output: OutputOps,
        <<N as AutoCallable<K>>::Output as NodeOutput>::Refs: ShapeOps,
    {
        let _ = self.register_auto_step::<N, K>(node);
        self
    }

    /// 自动输入的取输出形式：同上，返回 Node Output 对应的 RefShape（单 `Ref<T>` 或 `()`）。
    ///
    /// 自动解析失败（重复类型或缺少来源）时不登记 Step，返回失效句柄形状；后续显式使用会被
    /// 统一 availability 拒绝，因此失败结果不会成为合法来源。
    #[allow(private_bounds)]
    pub fn then_output<N, K>(
        &mut self,
        node: N,
    ) -> <<N as AutoCallable<K>>::Output as NodeOutput>::Refs
    where
        N: AutoCallable<K> + 'n,
        K: 'static,
        <N as AutoCallable<K>>::Output: OutputOps,
        <<N as AutoCallable<K>>::Output as NodeOutput>::Refs: ShapeOps,
    {
        self.register_auto_step::<N, K>(node)
    }

    /// 显式连接的普通形式：登记同一步骤并返回原 Flow 以便继续链式登记。
    ///
    /// 输入 Shape 由 `input` 的静态类型给出，不做“最近来源”解析；连接类型/arity 不符在
    /// 编译期拒绝，位置来源与顺序仍由 Definition 统一验证。
    #[allow(private_bounds)]
    pub fn then_with<N, A, K>(&mut self, node: N, input: A) -> &mut Self
    where
        N: Callable<A, K> + 'n,
        K: 'static,
        A: RefShape + ShapeOps,
        A::Spec: InputOps,
        <N::Output as NodeOutput>::Refs: ShapeOps,
        N::Output: OutputOps,
    {
        let _ = self.then_with_output::<N, A, K>(node, input);
        self
    }

    /// 唯一 Node 登记实现：显式 RefShape 连接、真实 callable 与输出槽。
    ///
    /// 六种 Node/Provide 入口（自动/显式 × 普通/取输出、Provide × 普通/取输出）最终都经此方法，
    /// 不复制第二份登记实现。返回 Node Output 对应的 RefShape：单 Ref 或 `()`（unit 输出）。
    #[allow(private_bounds)]
    pub fn then_with_output<N, A, K>(
        &mut self,
        node: N,
        input: A,
    ) -> <N::Output as NodeOutput>::Refs
    where
        N: Callable<A, K> + 'n,
        K: 'static,
        A: RefShape + ShapeOps,
        A::Spec: InputOps,
        <N::Output as NodeOutput>::Refs: ShapeOps,
        N::Output: OutputOps,
    {
        self.builder
            .borrow_mut()
            .register_node_step::<N, A, K>(self.body, node, input)
    }

    /// 同步 Factory 的普通形式：包成零输入 Node 并登记一步；返回原 Flow。
    ///
    /// Factory 只在真实执行该 Step 时调用（`Fn() -> T` 每次返回新的 owned Data），
    /// 不要求 `'static`／Send，也不隐式 Clone。
    pub fn provide<F, T>(&mut self, factory: F) -> &mut Self
    where
        F: Fn() -> T + 'n,
        T: Data,
    {
        let _ = self.provide_output(factory);
        self
    }

    /// 同步 Factory 的取 Ref 形式：登记同一步骤并返回其 `Ref<T>`。
    pub fn provide_output<F, T>(&mut self, factory: F) -> Ref<T>
    where
        F: Fn() -> T + 'n,
        T: Data,
    {
        self.then_with_output::<ProvideNode<F, T>, (), StructCall>(ProvideNode::new(factory), ())
    }

    /// 自动登记：解析输入后交给共同登记；失败时不登记 Step 并返回失效句柄形状。
    fn register_auto_step<N, K>(
        &mut self,
        node: N,
    ) -> <<N as AutoCallable<K>>::Output as NodeOutput>::Refs
    where
        N: AutoCallable<K> + 'n,
        K: 'static,
        <N as AutoCallable<K>>::Output: OutputOps,
        <<N as AutoCallable<K>>::Output as NodeOutput>::Refs: ShapeOps,
    {
        let (input, resolved) = self.resolve_auto_input::<<N as AutoCallable<K>>::Input>();
        if resolved {
            node.register(self, input)
        } else {
            self.failed_output_shape::<<N as AutoCallable<K>>::Output>()
        }
    }

    /// 自动输入解析：先做完整 Shape 重复类型检查，再按调用位置逐槽取最近合法来源。
    ///
    /// 返回解析出的具体 RefShape 与是否全部成功。重复或缺少来源的槽位一律使用无当前
    /// Definition 身份的哨兵句柄：失败结果不会进入合法候选，也不能被后续显式路径补成
    /// 正常来源。查询本身不登记 Step、位置或 Import。
    fn resolve_auto_input<A>(&self) -> (A, bool)
    where
        A: RefShape + ShapeOps,
    {
        let types = <A as ShapeOps>::type_descs();
        let mut builder = self.builder.borrow_mut();
        let unique = builder.check_auto_query_shape(self.body, &types);
        let order = builder.current_order();
        let mut resolved = unique;
        let mut handles: Vec<PositionHandle> = Vec::with_capacity(types.len());
        for (slot, type_desc) in types.into_iter().enumerate() {
            let found = if unique {
                builder.resolve(self.body, type_desc, Some(slot as u16), order)
            } else {
                None
            };
            match found {
                Some(desc) => handles.push(desc.handle),
                None => {
                    resolved = false;
                    handles.push(builder.missing_handle());
                }
            }
        }
        drop(builder);
        let shape = <A as ShapeOps>::from_positions(&mut handles.into_iter());
        (shape, resolved)
    }

    /// 自动登记失败时的输出句柄形状：所有槽位都是哨兵，不发布为正常来源。
    fn failed_output_shape<O>(&self) -> <O as NodeOutput>::Refs
    where
        O: NodeOutput,
        <O as NodeOutput>::Refs: ShapeOps,
    {
        let missing = self.builder.borrow().missing_handle();
        let count = <<O as NodeOutput>::Refs as ShapeOps>::type_descs().len();
        let mut handles = std::iter::repeat_n(missing, count);
        <<O as NodeOutput>::Refs as ShapeOps>::from_positions(&mut handles)
    }

    /// 创建 Ordinary child 并同步执行一次 unit Body；四种控制形式共用。
    ///
    /// child Body 完整保存；普通 Rust block 不提供该生命周期边界。闭包本身同步消费，
    /// 被登记 Node 句柄的寿命由 `then` 的 `N: 'n` 约束保证。
    fn open_child<F>(&mut self, definition: F) -> BodyId
    where
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self
            .builder
            .borrow_mut()
            .new_body(Some(self.body), BodyRole::Ordinary);
        let mut sub = Flow {
            builder: Rc::clone(&self.builder),
            body: child,
        };
        definition(&mut sub);
        child
    }

    /// 失败收口按请求的 RefShape 返回与槽数一致的失效句柄形状。
    ///
    /// 句柄来自 `missing_handle`：后续显式使用会被统一 availability 拒绝，既不会成为
    /// 合法来源，也不会因少槽/多槽 panic。
    fn failed_shape<O>(&self) -> O
    where
        O: ShapeOps,
    {
        let missing = self.builder.borrow().unresolved_handle();
        let count = <O as ShapeOps>::type_descs().len();
        let mut handles = std::iter::repeat_n(missing, count);
        <O as ShapeOps>::from_positions(&mut handles)
    }

    /// 普通 Chain：建立真实 child 边界并登记其声明的输出，返回原 Flow。
    ///
    /// Body 是 `FnOnce(&mut Flow<'n>)`：在 child 内用 `output`／`output_with` 声明完整输出；
    /// 声明属于该 Body，而不是闭包的返回契约。不取 Ref 不代表不登记、不 Export 或不执行：
    /// 已声明输出照常按整组交接发布给 parent，后续步骤可依自己的 Query 找到它们。未声明
    /// Body 合法（零业务输出），且与显式 unit 是两种不同状态。
    pub fn chain<F>(&mut self, definition: F) -> &mut Self
    where
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self.open_child(definition);
        let _ = self
            .builder
            .borrow_mut()
            .register_chain_outputs(self.body, child, None);
        self
    }

    /// 取输出 Chain：与 [`Flow::chain`] 共享同一条 child 构建与登记路径，额外返回 parent 域
    /// 可见的 RefShape。
    ///
    /// `O` 不从闭包返回值推导，而由调用处给出（局部 RefShape 注解、外层返回类型或显式实参）。
    /// Definition 在收口时核对 `O` 与 child 的实际声明：不符是 Definition 错误（Rust 可以
    /// 构建，业务执行前拒绝），并按请求槽数返回失效句柄形状，不制造合法假来源；零业务槽的
    /// `O = ()` 合法，未声明状态保持未声明。
    ///
    /// ```ignore
    /// let report: Ref<Report> = flow.chain_output(|sub| {
    ///     sub.then(&generate);
    ///     sub.output::<Report>();
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn chain_output<O, F>(&mut self, definition: F) -> O
    where
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self.open_child(definition);
        let expected = <O as ShapeOps>::type_descs();
        let (outputs, ok) =
            self.builder
                .borrow_mut()
                .register_chain_outputs(self.body, child, Some(&expected));
        if ok {
            <O as ShapeOps>::from_positions(&mut outputs.into_iter())
        } else {
            self.failed_shape::<O>()
        }
    }

    /// 按 case 键类型自动解析 selector 并匹配一个分支执行（SPEC §8.2、Engine §6.7）。
    ///
    /// 自动形式在方法入口按 `K` 解析当前最近的合法 `Ref<K>` 并记录该次真实使用；分支构建
    /// 期间新出现的同类型来源不改变已固定的 selector。全部分支在构建期各登记一次真实
    /// Body；运行期只执行选中分支，未选分支没有运行 Scope，也不调用其 Node。`case` 的键是
    /// Definition 匹配配置，不登记为业务输入；无匹配且无 `otherwise` 时返回
    /// [`RunError::NoMatchingCase`](crate::RunError::NoMatchingCase)——若该失败退出同时发生
    /// 清理故障，则返回 [`RunError::Runtime`](crate::RunError::Runtime)，并以 `Error::source`
    /// 保留原终止原因、以 `cleanup_source` 保留首个清理故障诊断。
    ///
    /// 分支输出类型来自各分支 Body 内的 `output`／`output_with` 声明：全部未声明或全部显式
    /// unit 分别合法，混合拒绝；非 unit 分支必须 arity 与逐槽类型一致。普通形式照常发布
    /// 全部已声明输出，不接收 Ref 也不表示不 Export、不执行。
    pub fn choose<K, F>(&mut self, definition: F) -> &mut Self
    where
        K: Data + PartialEq,
        F: FnOnce(&mut Choice<'n, K>),
    {
        let _ = self.register_choose::<K, F>(None, definition, None);
        self
    }

    /// 自动 selector 的取输出形式：与 [`Flow::choose`] 共享同一条登记路径，额外返回父域
    /// 可见的 RefShape。
    ///
    /// `O` 由调用处给出（局部 RefShape 注解、外层返回类型或显式实参）。Definition 在收口时
    /// 先比较各分支的声明状态与共同 Shape，再核对 `O` 期望：不符是 Definition 错误（Rust 可以
    /// 构建，业务执行前拒绝），共享父输出全部保持失效，并按请求槽数返回失效句柄形状。
    ///
    /// ```ignore
    /// let report: Ref<Report> = flow.choose_output(|choice| {
    ///     choice.case(Route::A, |sub| { sub.then(&process_a); sub.output::<Report>(); });
    ///     choice.otherwise(|sub| { sub.then(&fallback); sub.output::<Report>(); });
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn choose_output<K, O, F>(&mut self, definition: F) -> O
    where
        K: Data + PartialEq,
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Choice<'n, K>),
    {
        let expected = <O as ShapeOps>::type_descs();
        let (outputs, ok) = self.register_choose::<K, F>(None, definition, Some(&expected));
        if ok {
            <O as ShapeOps>::from_positions(&mut outputs.into_iter())
        } else {
            self.failed_shape::<O>()
        }
    }

    /// 显式 selector 的普通形式：精确保留所给 `Ref<K>` 作为 selector 使用点，不按类型重选
    /// 最新来源；其余与 [`Flow::choose`] 相同。
    pub fn choose_with<K, F>(&mut self, selector: Ref<K>, definition: F) -> &mut Self
    where
        K: Data + PartialEq,
        F: FnOnce(&mut Choice<'n, K>),
    {
        let _ = self.register_choose::<K, F>(Some(selector), definition, None);
        self
    }

    /// 显式 selector 的取输出形式：与 [`Flow::choose_with`] 共享同一条登记路径，返回父域
    /// 可见的 RefShape；入口来源与自动形式只在 selector 解析上不同。
    #[allow(private_bounds)]
    pub fn choose_with_output<K, O, F>(&mut self, selector: Ref<K>, definition: F) -> O
    where
        K: Data + PartialEq,
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Choice<'n, K>),
    {
        let expected = <O as ShapeOps>::type_descs();
        let (outputs, ok) =
            self.register_choose::<K, F>(Some(selector), definition, Some(&expected));
        if ok {
            <O as ShapeOps>::from_positions(&mut outputs.into_iter())
        } else {
            self.failed_shape::<O>()
        }
    }

    /// Choose 的唯一登记实现（自动/显式与普通/取输出四种形式共用）。
    ///
    /// 顺序固定：入口固定 selector 使用点 → 构建全部分支与配置错误 → 读取各分支冻结声明并
    /// 比较共同契约 → 按契约类型延后分配共享父输出 → 逐分支登记本地出口与槽位 → 在共享输出
    /// 全部合法时按父域发布时点公开 →（可选）核对调用方 `O` 期望。任一步失败都不发布共享
    /// 输出，也不产生可运行假来源。
    #[allow(private_bounds)]
    fn register_choose<K, F>(
        &mut self,
        selector: Option<Ref<K>>,
        definition: F,
        expected: Option<&[TypeDesc]>,
    ) -> (Vec<PositionHandle>, bool)
    where
        K: Data + PartialEq,
        F: FnOnce(&mut Choice<'n, K>),
    {
        let (step, order, selector_desc, selector_ok, errors_before) = {
            let mut builder = self.builder.borrow_mut();
            let step = builder.step_count(self.body);
            let order = builder.fresh_order();
            let errors_before = builder.errors().len();
            let type_desc = TypeDesc::of::<K>();
            let desc = match selector {
                Some(selector) => PositionDesc {
                    handle: selector.handle(),
                    type_desc,
                },
                None => builder
                    .resolve(self.body, type_desc, None, order)
                    .unwrap_or(PositionDesc {
                        handle: builder.missing_handle(),
                        type_desc,
                    }),
            };
            let selector_ok = builder
                .record_use_at(self.body, Some(step), desc, order)
                .is_none();
            (step, order, desc, selector_ok, errors_before)
        };
        let mut choice = Choice::new(Rc::clone(&self.builder), self.body, step);
        definition(&mut choice);
        let (cases, otherwise) = choice.into_parts();
        let mut builder = self.builder.borrow_mut();
        if cases.is_empty() && otherwise.is_none() {
            builder.record_config_error(BuildErrorDetail::EmptyChoose {
                body: self.body,
                step: Some(step),
            });
        }
        // 配置级错误（空配置、重复 case／otherwise）同样使共享输出保持失效：
        // 发布门控必须覆盖 selector、配置、全部分支契约与调用方期望。
        let config_ok = builder.errors().len() == errors_before;
        // 分支契约：先用**统一判据**完成 状态 → arity → 逐槽类型 的全组一致性判定，
        // 再（仅当契约成立时）核对调用方 O。合法 Undeclared 零输出契约与声明失败必须
        // 区分：前者合法（零槽），后者由统一判据按状态不一致/缺声明拒绝。
        let branch_bodies: Vec<BodyId> = cases
            .iter()
            .map(|(_, body)| *body)
            .chain(otherwise.iter().copied())
            .collect();
        let mut contract_types: Vec<TypeDesc> = Vec::new();
        if let Some(first) = branch_bodies.first() {
            contract_types = builder
                .output_declaration_of(*first)
                .slots
                .iter()
                .map(|desc| desc.type_desc)
                .collect();
        }
        let mut contract_ok = true;
        if !branch_bodies.is_empty() {
            let contract = ChooseContract {
                body: self.body,
                step,
                branches: branch_bodies.clone(),
                output_types: contract_types.clone(),
            };
            // 契约不成立时：既不发布共享输出，也不做 O 期望核对（保证 finish 报出的
            // 是契约/来源错误，而不是被 O 期望遮蔽）；契约诊断由 finish 的统一判据给出，
            // 不在此重复记录，避免盖掉更早的来源/deferred 首因。
            if let Some(error) = builder.validate_choose_contract(&contract) {
                contract_ok = false;
                // 若某分支的声明自身失败（其首因更早，或由更深的来源/顺序问题派生），
                // 不在此重复记录：让 finish 按真实因果给出源/顺序错误（既有前向来源
                // 与更早真实输入的首因保持不变）。其余（状态一致但跨分支 arity/逐槽
                // 类型不一致等）是自足契约错误，必须保留为语义首因，避免被失败形状
                // 派生的占位错误（如 Root 声明失败 Ref 产生的 UnknownPosition）遮蔽。
                let derived = branch_bodies.iter().any(|branch| {
                    builder.output_declaration_of(*branch).state() == DeclarationState::Failed
                });
                if !derived {
                    builder.record_config_error(error);
                }
            }
        }
        // 共享父输出按真实契约类型延后分配：分支构建期间它是“已分配未发布”，
        // 任何分支都不可能解析到尚未成立的父输出。
        let outputs = if contract_types.is_empty() {
            Vec::new()
        } else {
            builder.allocate_choose_outputs(self.body, step, &contract_types)
        };
        let mut registered_cases: Vec<ChooseCase<K>> = Vec::with_capacity(cases.len());
        for (index, (key, body)) in cases.into_iter().enumerate() {
            let branch = builder.register_choose_branch(body, step, index as u16, &outputs);
            registered_cases.push(ChooseCase { key, branch });
        }
        let registered_otherwise = otherwise.map(|body| {
            builder.register_choose_branch(body, step, registered_cases.len() as u16, &outputs)
        });
        // O 期望只在分支契约自身成立时核对：契约已失败时保留首因，不叠加二次诊断。
        let mut expectation_ok = true;
        if let Some(expected) = expected
            && contract_ok
            && let Some(first) = registered_cases
                .first()
                .map(|case| case.branch.body)
                .or_else(|| registered_otherwise.as_ref().map(|branch| branch.body))
        {
            let declaration = builder.output_declaration_of(first);
            let found: Vec<TypeDesc> = declaration
                .slots
                .iter()
                .map(|desc| desc.type_desc)
                .collect();
            if let Some(error) = builder.validate_declaration_expectation(
                first,
                DeclarationKind::Output,
                declaration.attempted,
                declaration.declared,
                &found,
                expected,
            ) {
                builder.record_config_error(error);
                expectation_ok = false;
            }
        }
        let published = builder.push_choose_step(
            self.body,
            ChooseStep::new(
                selector_desc,
                registered_cases,
                registered_otherwise,
                outputs.iter().map(|handle| handle.position).collect(),
                order,
            ),
            selector_ok && config_ok && contract_ok && expectation_ok,
        );
        (outputs, published)
    }

    /// 普通 Retry：建立真实 child 边界并登记其声明的输出，返回原 Flow。
    ///
    /// `max_retries = N` 指首次 attempt 之外最多再执行 N 次，总尝试次数最多 N+1；`N = 0`
    /// 仍执行首次 attempt。没有默认次数，也不按 arity 分类。attempt 闭包同步执行一次并登记
    /// 完整 child Flow；运行期复用同一静态 Body，不重新构建，`usize::MAX` 无法表达 N+1，
    /// 在构建期拒绝。
    ///
    /// 支持 `()`、单 Ref 与 2～16 个 flat Ref tuple 输出；unit 不登记假业务槽。显式 unit
    /// （`attempt.output::<()>()`）与未声明（不写 `output`）是两种不同状态。预算耗尽返回
    /// [`RunError::RetryExhausted`](crate::RunError::RetryExhausted)；该失败退出同时发生清理
    /// 故障时按 SPEC §12 返回 [`RunError::Runtime`](crate::RunError::Runtime)，并以
    /// `Error::source` 保留原预算错误。
    pub fn retry<F>(&mut self, max_retries: usize, definition: F) -> &mut Self
    where
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self.open_child(definition);
        let _ = self
            .builder
            .borrow_mut()
            .register_retry(self.body, child, max_retries, None);
        self
    }

    /// 取输出 Retry：与 [`Flow::retry`] 共享同一条 child 构建与登记路径，额外返回成功尝试的
    /// 父域 RefShape。
    ///
    /// `O` 由调用处给出（局部 RefShape 注解、外层返回类型或显式实参）；Definition 在收口时
    /// 核对它与 attempt 的实际声明，不符是 Definition 错误而非编译错误，并按请求槽数返回
    /// 失效句柄形状。
    ///
    /// ```ignore
    /// let response: Ref<ApiResponse> = flow.retry_output(3, |attempt| {
    ///     attempt.provide(RequestContext::new).then(&call_api);
    ///     attempt.output::<ApiResponse>();
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn retry_output<O, F>(&mut self, max_retries: usize, definition: F) -> O
    where
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self.open_child(definition);
        let expected = <O as ShapeOps>::type_descs();
        let (outputs, ok) = self.builder.borrow_mut().register_retry(
            self.body,
            child,
            max_retries,
            Some(&expected),
        );
        if ok {
            <O as ShapeOps>::from_positions(&mut outputs.into_iter())
        } else {
            self.failed_shape::<O>()
        }
    }

    /// 控制器：用 Normal next 推进受控状态，用 IterBreak 交付进入本轮时的 current
    /// （SPEC §8.5、Engine §6.9）。
    ///
    /// Round Body 是单参数 unit 闭包，用 [`Flow::next`]／[`Flow::next_with`] 声明**恰好一次**
    /// 完整 Next（与 initial 同 StateShape；缺失/重复/类型不符/Round 使用 output 都构建拒绝）。
    /// `max_iterations = N` 要求 `N > 0`，第一轮计入 N。状态形状为单 Ref 或 2～16 个 flat
    /// Ref tuple，initial／current／next／最终输出同 Shape；**不支持 unit 状态**。每轮从正式
    /// ControlState 经 StateImport 导入 current，整组 Promote 接受 next，不重绑 parent
    /// Definition Ref。第 N 轮仍返回 Continue 时以
    /// [`RunError::IterationLimitReached`](crate::RunError::IterationLimitReached) 结束，不强制
    /// 成功、不额外执行一次判断、不通过错误返回最后状态；IterBreak 交付进入本轮时的完整
    /// current，且不在此报告次数耗尽。
    ///
    /// 普通形式返回 Flow：最终 State 照常发布为 parent 域来源（可 `ref_of` 解析，未发布即
    /// 不可用）；取最终 State 用 [`Flow::iter_output`]。
    ///
    /// ```text
    /// flow.iter(initial, 5, |round| {
    ///     let current = round.ref_of::<State>();
    ///     round.then_with(&judge, current).then(&advance);
    ///     round.next::<State>();
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn iter<S, F>(&mut self, initial: S, max_iterations: usize, definition: F) -> &mut Self
    where
        S: RefShape + StateShape,
        F: FnOnce(&mut Flow<'n>),
    {
        let _ = self.register_iter_round(initial, max_iterations, definition);
        self
    }

    /// 取输出形式：与 [`Flow::iter`] 共享同一条登记路径，返回父域最终 State RefShape。
    ///
    /// `S` 同时锚定 initial、formal Current、Next 期望与最终交付契约；登记失败（缺声明、
    /// 预算为 0、Shape 不符、来源不可用等）按请求槽数返回失效句柄形状，不 panic、不制造
    /// 合法假来源，最终仍由 Definition 收口拒绝。
    #[allow(private_bounds)]
    pub fn iter_output<S, F>(&mut self, initial: S, max_iterations: usize, definition: F) -> S
    where
        S: RefShape + StateShape,
        F: FnOnce(&mut Flow<'n>),
    {
        match self.register_iter_round(initial, max_iterations, definition) {
            Some(outputs) => <S as ShapeOps>::from_positions(&mut outputs.into_iter()),
            None => self.failed_shape::<S>(),
        }
    }

    /// Iter 的唯一登记实现（普通/取输出共用）。
    ///
    /// 入口顺序在**构建 Round 之前**取得：initial 的使用/imports 时点必须早于 Round 内新增
    /// 来源与 Next 声明，这样后增来源不能反向成为 initial 的合法来源，Round 的声明顺序也
    /// 不会掩盖 initial 的首因。Round Body 只构建一次，运行期每轮复用同一 Definition；
    /// 登记只消费 Body 已冻结的 Next 声明，不补声明、不重解析最近来源。
    ///
    /// round Body 先于 formal current 建立：current 的 owner 是该 round Body，使其词法
    /// 可见性限定在本轮（round 内合法，parent／sibling／Root 使用按 OutOfScope(ChildLocal)
    /// 在 Definition 阶段拒绝）。new_body 不消耗定义顺序，因此先建 Body 不改变位置编号。
    #[allow(private_bounds)]
    fn register_iter_round<S, F>(
        &mut self,
        initial: S,
        max_iterations: usize,
        definition: F,
    ) -> Option<Vec<PositionHandle>>
    where
        S: RefShape + StateShape,
        F: FnOnce(&mut Flow<'n>),
    {
        let (step, round, entry_order, current) = {
            let mut builder = self.builder.borrow_mut();
            let step = builder.step_count(self.body);
            let entry_order = builder.fresh_order();
            let round = builder.new_body(Some(self.body), BodyRole::Round);
            builder.set_round_initial(round, <S as ShapeOps>::type_descs());
            let current =
                builder.allocate_iter_current(round, step, &<S as ShapeOps>::type_descs());
            (step, round, entry_order, current)
        };
        let initial_slots = initial.positions();
        // Round 及其后代的构建失败必须使父结果失效：以**已知失败标记**的增量捕捉「本轮新增的
        // 任何失败」——既含即时入列的错误（current 查询错误、child Body 错误、来源/shape 失败），
        // 也含未写入错误列表的已知失败：延后到 finish 分类的未登记/前向来源，以及为保留更早
        // 首因而未重复记录的派生失败（子声明选中了已失效来源）。
        let failures_before = self.builder.borrow().failure_marker();
        {
            let mut sub = Flow {
                builder: Rc::clone(&self.builder),
                body: round,
            };
            definition(&mut sub);
        }
        let round_clean = self.builder.borrow().failure_marker() == failures_before;
        let (outputs, ok) = self.builder.borrow_mut().register_iter(
            self.body,
            round,
            step,
            entry_order,
            initial_slots,
            current,
            max_iterations,
            round_clean,
            <S as StateShape>::register_states,
            <S as StateShape>::state_import_slots,
        );
        ok.then_some(outputs)
    }

    /// 控制器：按集合元素顺序对每项执行同一 item Body，并按槽聚合输出（SPEC §8.3、Engine §6.6）。
    ///
    /// `collection` 是 `Ref<Vec<T>>`；item Body 是单参数 unit Body，用 `ref_of::<Item>()`
    /// 取得当前项的受限借用（`Ref<T>`，无独立 DataId、不被 Clone 或移走）。聚合类型来自
    /// item Body 内 `output`／`output_with` 声明时捕获的 typed 能力：`()` 为零 collector、
    /// 单 `Ref<A>` 聚合为 `Ref<Vec<A>>`、2～16 个 flat `Ref` 逐槽聚合；同类型不同逻辑位置
    /// 各有独立槽。Body 只构建一次、多次执行；空集合不执行 Body，但仍完成构建与统一验证。
    /// item Body 与所有 child Flow 一样支持 then／chain／each／choose／retry／iter。
    ///
    /// 集合来源不受限：完整 `Vec<T>` Data 与外层 Each 的 item（`Ref<Vec<T>>`）都合法。
    pub fn each<T, F>(&mut self, collection: Ref<Vec<T>>, definition: F) -> &mut Self
    where
        T: Data,
        F: FnOnce(&mut Flow<'n>),
    {
        let _ = self.register_each::<T, F>(collection, definition, None);
        self
    }

    /// 取聚合输出形式：与 [`Flow::each`] 共享同一条登记路径，返回父域可见的聚合 RefShape。
    ///
    /// `O` 由调用处给出，Definition 与 item 声明的 `<ItemRefs as EachShape>::Collected`
    /// 比较（单槽 `Ref<Vec<A>>`，多槽逐槽；`Ref<A>` 漏聚合会被拒绝）。不符是 Definition 错误，
    /// 按请求槽数返回失效句柄形状。
    ///
    /// ```ignore
    /// let reports: Ref<Vec<Report>> = flow.each_output(items, |item| {
    ///     let current = item.ref_of::<Item>();
    ///     item.then_with_output(&process, current);
    ///     item.output::<Report>();
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn each_output<T, O, F>(&mut self, collection: Ref<Vec<T>>, definition: F) -> O
    where
        T: Data,
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Flow<'n>),
    {
        let expected = <O as ShapeOps>::type_descs();
        let (outputs, ok) = self.register_each::<T, F>(collection, definition, Some(&expected));
        if ok {
            <O as ShapeOps>::from_positions(&mut outputs.into_iter())
        } else {
            self.failed_shape::<O>()
        }
    }

    /// Each 的唯一登记实现（普通/取聚合两种形式共用）。
    ///
    /// 先建 item Body 并分配形式 item（owner=item Body），再同步执行一次 unit Body；随后读取
    /// 其冻结声明与声明时捕获的 typed 能力，交给 `register_each` 完成槽位、聚合类型与调用方
    /// 期望核对。缺失/失效/槽数或聚合类型不符的 typed 能力在 Definition 阶段拒绝。
    #[allow(private_bounds)]
    fn register_each<T, F>(
        &mut self,
        collection: Ref<Vec<T>>,
        definition: F,
        expected: Option<&[TypeDesc]>,
    ) -> (Vec<PositionHandle>, bool)
    where
        T: Data,
        F: FnOnce(&mut Flow<'n>),
    {
        let collection_desc = PositionDesc {
            handle: collection.handle(),
            type_desc: TypeDesc::of::<Vec<T>>(),
        };
        // item Body 必须先于形式 item 位置建立：item 的 owner 是该 Body，使其词法可见性
        // 限定在 item 内（parent／sibling／Root 使用按 OutOfScope(ChildLocal) 在 Definition
        // 阶段拒绝）。new_body 不消耗定义顺序，因此先建 Body 不改变任何位置编号或顺序。
        let (step, item, item_position) = {
            let mut builder = self.builder.borrow_mut();
            let step = builder.step_count(self.body);
            let item = builder.new_body(Some(self.body), BodyRole::Ordinary);
            let item_position = builder.allocate_each_item(item, step, TypeDesc::of::<T>());
            (step, item, item_position)
        };
        {
            let mut sub = Flow::open(Rc::clone(&self.builder), item);
            definition(&mut sub);
        }
        self.builder.borrow_mut().register_each(
            self.body,
            item,
            step,
            collection_desc,
            item_position,
            expected,
            crate::core::engine::shape::each_bind_item::<T>,
            crate::core::engine::shape::each_collection_len::<T>,
        )
    }

    /// 当前位置最近的合法业务 Ref（只查询元数据，不登记 Step、位置或 Import）。
    ///
    /// 查询成功返回可继续显式使用的 `Ref<T>`；缺少来源时记录 `MissingSource` 并返回没有
    /// 当前 Definition 身份的哨兵，后续显式使用仍会被统一 availability 拒绝。
    pub fn ref_of<T: Data>(&self) -> Ref<T> {
        let mut builder = self.builder.borrow_mut();
        let order = builder.current_order();
        match builder.resolve(self.body, TypeDesc::of::<T>(), None, order) {
            Some(desc) => Ref::from_handle(desc.handle),
            None => Ref::from_handle(builder.missing_handle()),
        }
    }

    /// 自动 Output 声明：按**调用时点**逐槽解析类型 Shape，返回原 Flow。
    ///
    /// 选定来源后即冻结：后续同类型 Node/Provide 不会改变本次声明的来源。声明本身不登记
    /// Step／Data，也不终止执行；缺少来源、重复类型与逐槽类型/arity 不符在 Definition 验证中拒绝。
    #[allow(private_bounds)]
    pub fn output<T: RootInput>(&mut self) -> &mut Self
    where
        T::Refs: ShapeOps + EachCollect,
        <T::Refs as EachShape>::Collected: ShapeOps,
    {
        let types = <T::Refs as ShapeOps>::type_descs();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let mut slots = Vec::with_capacity(types.len());
        let mut missing = false;
        for (slot, type_desc) in types.into_iter().enumerate() {
            match builder.resolve(self.body, type_desc, Some(slot as u16), order) {
                Some(desc) => slots.push(desc),
                None => {
                    missing = true;
                    slots.push(PositionDesc {
                        handle: builder.missing_handle(),
                        type_desc,
                    });
                }
            }
        }
        let ok = builder.declare(self.body, DeclarationKind::Output, slots, order, missing);
        if ok {
            // 声明时捕获该 Body 的 typed 能力：作为同一份输出声明的私有元数据保存，
            // 供 Each 等控制结构消费；重复/失败声明不会覆盖第一份能力。
            builder.attach_each_bridge(self.body, EachBridge::of::<T::Refs>());
        }
        drop(builder);
        self
    }

    /// 显式 Output 声明：直接声明给定 RefShape 的槽位；复用同一声明与验证路径。
    ///
    /// 与 [`Flow::output`] 的区别是不做“最近来源”解析，因此可以直接声明多个不同来源的槽位；
    /// 同一逻辑位置重复声明仍按直接重复 Root Ref 拒绝。
    #[allow(private_bounds)]
    pub fn output_with<R>(&mut self, refs: R) -> &mut Self
    where
        R: ShapeOps + EachCollect,
        <R as EachShape>::Collected: ShapeOps,
    {
        let slots = refs.positions();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let ok = builder.declare(self.body, DeclarationKind::Output, slots, order, false);
        if ok {
            builder.attach_each_bridge(self.body, EachBridge::of::<R>());
        }
        drop(builder);
        self
    }

    /// 自动 Next 声明：在声明位置按类型解析最近合法来源并**冻结**（SPEC §8.5）。
    ///
    /// 仅直接 Iter Round Body 合法；Round 必须恰好有一次完整 Next 声明，与 initial 同 Shape
    /// （arity/逐槽类型），unit 状态因缺 `StateShape` impl 在编译期拒绝。声明不终止 Body、
    /// 不提前 Promote；其后的步骤照常登记/执行。同类型多槽应改用 [`Flow::next_with`]。
    ///
    /// 名字来自 Iter 的 Next 状态（SPEC 术语），与 `Iterator::next` 无关。
    #[allow(private_bounds, clippy::should_implement_trait)]
    pub fn next<T: RootInput>(&mut self) -> &mut Self
    where
        T::Refs: StateShape,
    {
        let types = <T::Refs as ShapeOps>::type_descs();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let mut slots = Vec::with_capacity(types.len());
        let mut missing = false;
        for (slot, type_desc) in types.into_iter().enumerate() {
            match builder.resolve(self.body, type_desc, Some(slot as u16), order) {
                Some(desc) => slots.push(desc),
                None => {
                    missing = true;
                    slots.push(PositionDesc {
                        handle: builder.missing_handle(),
                        type_desc,
                    });
                }
            }
        }
        let _ = builder.declare(self.body, DeclarationKind::Next, slots, order, missing);
        drop(builder);
        self
    }

    /// 显式 Next 声明：逐槽给出 Ref，保留同类型多槽的次序与来源（SPEC §8.5）。
    ///
    /// 与 [`Flow::next`] 同等受唯一声明与用途校验约束：仅直接 Iter Round Body 合法，
    /// 完整 Shape 必须与 initial 一致（arity/逐槽类型），重复声明仍构建拒绝。
    #[allow(private_bounds)]
    pub fn next_with<S: StateShape>(&mut self, refs: S) -> &mut Self {
        let slots = refs.positions();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let _ = builder.declare(self.body, DeclarationKind::Next, slots, order, false);
        drop(builder);
        self
    }

    /// 取得本 Round 的完整 formal Current 槽（G06 审定能力，SPEC §8.5）。
    ///
    /// 仅直接 Iter Round Body 合法：返回该 Round 全部 formal Current 槽（声明序 = initial
    /// 槽序），arity／逐槽类型必须与 initial 完全一致。不做“最近来源”选择（那是
    /// [`Flow::ref_of`] 的语义），不接受子 Shape 部分查询，重复调用幂等（同一批槽身份）；
    /// 与 captured initial（原实例、不随轮推进）也不同。`&self` 查询不登记 Step/Data、
    /// 不声明 Next/Output、不推进、不做运行期查询；非直接 Round 或 Shape 不符记 Definition
    /// 期首因并按 `T::Refs` 返回受控失败形状，不 panic、不产生可运行来源。
    ///
    /// ```text
    /// flow.iter((x0, y0, z0), 5, |round| {
    ///     let (x, y, z) = round.current::<(Parcel, Parcel, Parcel)>();
    ///     round.then_with(&judge, x);
    ///     round.next_with((y, x, z)); // 逐槽交换/保留
    /// });
    /// ```
    #[allow(private_bounds)]
    pub fn current<T: RootInput>(&self) -> T::Refs
    where
        T::Refs: StateShape,
    {
        let wanted = <T::Refs as ShapeOps>::type_descs();
        let handles = self
            .builder
            .borrow_mut()
            .resolve_iter_current(self.body, &wanted);
        match handles {
            Some(handles) => {
                let mut handles = handles.into_iter();
                <T::Refs as ShapeOps>::from_positions(&mut handles)
            }
            None => self.failed_shape::<T::Refs>(),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/internal/engine/flow_observe.rs"]
pub(crate) mod observe;
