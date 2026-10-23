//! 同步定义上下文 `Flow`（SPEC §4～§7 的定义期）。
//!
//! 公开类型只带一个 `'n`：Node 句柄与定义结构按 `'n` 借用，构建元数据由内部可变
//! 状态承载，不向调用者暴露 builder lifetime。`Flow` 不持有本次执行的业务值，也不
//! 提供公开 build／finish 入口。

use crate::core::engine::choice::Choice;
use crate::core::engine::data::Data;
use crate::core::engine::definition::{BodyRole, Builder, ChooseStep, DeclarationKind};
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
        let missing = self.builder.borrow().missing_handle();
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

    /// 按 selector 的值匹配一个分支执行（SPEC §9、Engine §6.7）。
    ///
    /// 全部 case／otherwise 的定义闭包在构建期同步执行一次并登记各自分支 Body；运行期
    /// 只执行选中分支，未选分支没有运行 Scope，也不调用其 Node。`case` 的键是 Definition
    /// 匹配配置，不登记为业务输入；无匹配且无 `otherwise` 时返回
    /// [`RunError::NoMatchingCase`](crate::RunError::NoMatchingCase)——若该失败退出同时发生
    /// 清理故障，则返回 [`RunError::Runtime`](crate::RunError::Runtime)，并以 `Error::source`
    /// 保留原终止原因、以 `cleanup_source` 保留首个清理故障诊断。
    ///
    /// 所有分支返回同一个输出 Shape `O`：类型、arity 与 unit 由 Rust 类型检查保证，
    /// 返回的是 parent 可见的新 RefShape。
    #[allow(private_bounds)]
    pub fn choose<K, O, F>(&mut self, selector: Ref<K>, definition: F) -> O
    where
        K: Data + PartialEq,
        O: RefShape + ShapeOps,
        F: FnOnce(&mut Choice<'n, K, O>),
    {
        let selector_desc = PositionDesc {
            handle: selector.handle(),
            type_desc: TypeDesc::of::<K>(),
        };
        let (step, order, outputs) = {
            let mut builder = self.builder.borrow_mut();
            let step = builder.step_count(self.body);
            let order = builder.fresh_order();
            let selector_ok = builder
                .record_use_at(self.body, Some(step), selector_desc, order)
                .is_none();
            let outputs =
                builder.allocate_choose_outputs(self.body, step, &<O as ShapeOps>::type_descs());
            if !selector_ok {
                for handle in &outputs {
                    builder.mark_tainted(*handle);
                }
            }
            (step, order, outputs)
        };
        let mut choice = Choice::new(Rc::clone(&self.builder), self.body, step, outputs.clone());
        definition(&mut choice);
        let (cases, otherwise) = choice.into_parts();
        let mut builder = self.builder.borrow_mut();
        if cases.is_empty() && otherwise.is_none() {
            builder.record_config_error(BuildErrorDetail::EmptyChoose {
                body: self.body,
                step: Some(step),
            });
        }
        builder.push_choose_step(
            self.body,
            ChooseStep::new(
                selector_desc,
                cases,
                otherwise,
                outputs.iter().map(|handle| handle.position).collect(),
                order,
            ),
        );
        drop(builder);
        <O as ShapeOps>::from_positions(&mut outputs.into_iter())
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
    /// （SPEC §11、Engine §6.9）。
    ///
    /// `max_iterations = N` 要求 `N > 0`，第一轮计入 N。状态形状为单 Ref 或 2～16 个 flat
    /// Ref tuple，initial／current／next／最终输出同 Shape；**不支持 unit 状态**。每轮从正式
    /// ControlState 经 StateImport 导入 current，整组 Promote 接受 next，不重绑 parent
    /// Definition Ref。第 N 轮仍返回 Continue 时以
    /// [`RunError::IterationLimitReached`](crate::RunError::IterationLimitReached) 结束，不强制
    /// 成功、不额外执行一次判断、不通过错误返回最后状态；IterBreak 交付进入本轮时的完整
    /// current，且不在此报告次数耗尽。
    #[allow(private_bounds)]
    pub fn iter<S, F>(&mut self, initial: S, max_iterations: usize, definition: F) -> S
    where
        S: RefShape + StateShape,
        F: FnOnce(S, &mut Flow<'n>) -> S,
    {
        // round Body 必须先于 formal current 建立：current 的 owner 是该 round Body，
        // 使其词法可见性限定在本轮（round 内合法，parent／sibling／Root 使用按
        // OutOfScope(ChildLocal) 在 Definition 阶段拒绝）。new_body 不消耗定义顺序，
        // 因此先建 Body 不改变任何位置编号或顺序。
        let (step, round, current) = {
            let mut builder = self.builder.borrow_mut();
            let step = builder.step_count(self.body);
            let round = builder.new_body(Some(self.body), BodyRole::Round);
            builder.set_round_initial(round, <S as ShapeOps>::type_descs());
            let current =
                builder.allocate_iter_current(round, step, &<S as ShapeOps>::type_descs());
            (step, round, current)
        };
        let current_refs = {
            let mut handles = current.iter().copied();
            <S as ShapeOps>::from_positions(&mut handles)
        };
        let initial_slots = initial.positions();
        let next = {
            let mut sub = Flow {
                builder: Rc::clone(&self.builder),
                body: round,
            };
            definition(current_refs, &mut sub)
        };
        let outputs = self.builder.borrow_mut().register_iter(
            self.body,
            round,
            step,
            initial_slots,
            next.positions(),
            current,
            max_iterations,
            <S as StateShape>::register_states,
            <S as StateShape>::state_import_slots,
        );
        <S as ShapeOps>::from_positions(&mut outputs.into_iter())
    }

    /// 控制器：按集合元素顺序对每项执行同一 item Body，并按槽聚合输出（SPEC §8、Engine §6.6）。
    ///
    /// `collection` 是 `Ref<Vec<T>>`；每次 Item 闭包接收 `Ref<T>`——原集合元素的受限借用，
    /// 无独立 DataId、不被 Clone 或移走。闭包返回的 item Body 输出 `()`／单 `Ref<O>`／
    /// 2～16 个 flat `Ref` tuple，分别映射为 `()`／`Ref<Vec<O>>`／逐槽 `Ref<Vec<..>>`；
    /// 同类型不同逻辑位置各有独立聚合槽。Body 只构建一次、多次执行；空集合不执行 Body，
    /// 但仍完成构建与统一验证。item Body 与所有 child Flow 一样支持 then／chain／each／
    /// choose／retry／iter。
    ///
    /// 集合来源不受限：完整 `Vec<T>` Data 与外层 Each 的 item（`Ref<Vec<T>>`，其元素自身
    /// 是集合）都合法。两种来源都只建立受限元素借用，不移动／复制集合，也不给来源任何
    /// 独立消费权。
    #[allow(private_bounds)]
    pub fn each<T, O, F>(
        &mut self,
        collection: Ref<Vec<T>>,
        definition: F,
    ) -> <O as EachShape>::Collected
    where
        T: Data,
        O: RefShape + ShapeOps + EachCollect,
        <O as EachShape>::Collected: RefShape + ShapeOps,
        F: FnOnce(Ref<T>, &mut Flow<'n>) -> O,
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
        let declared = {
            let mut sub = Flow {
                builder: Rc::clone(&self.builder),
                body: item,
            };
            definition(Ref::<T>::from_handle(item_position), &mut sub)
        };
        let outputs = self.builder.borrow_mut().register_each(
            self.body,
            item,
            step,
            collection_desc,
            item_position,
            declared.positions(),
            <<O as EachShape>::Collected as ShapeOps>::type_descs(),
            <O as EachCollect>::begin_collectors,
            crate::core::engine::shape::each_bind_item::<T>,
            crate::core::engine::shape::each_collection_len::<T>,
            <O as EachCollect>::finish_collectors,
        );
        <<O as EachShape>::Collected as ShapeOps>::from_positions(&mut outputs.into_iter())
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
        T::Refs: ShapeOps,
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
        let _ = builder.declare(self.body, DeclarationKind::Output, slots, order, missing);
        drop(builder);
        self
    }

    /// 显式 Output 声明：直接声明给定 RefShape 的槽位；复用同一声明与验证路径。
    ///
    /// 与 [`Flow::output`] 的区别是不做“最近来源”解析，因此可以直接声明多个不同来源的槽位；
    /// 同一逻辑位置重复声明仍按直接重复 Root Ref 拒绝。
    #[allow(private_bounds)]
    pub fn output_with<R: ShapeOps>(&mut self, refs: R) -> &mut Self {
        let slots = refs.positions();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let _ = builder.declare(self.body, DeclarationKind::Output, slots, order, false);
        drop(builder);
        self
    }

    /// 内部自动 Next 声明：仅直接 Iter Round 合法。
    #[allow(private_bounds, dead_code)] // T06 公开前由内部测试/后续任务使用
    pub(crate) fn next<T: RootInput>(&mut self) -> &mut Self
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

    /// 内部显式 Next 声明；仅直接 Iter Round 合法。
    #[allow(dead_code)] // T06 公开前由内部测试/后续任务使用
    pub(crate) fn next_with<R: StateShape>(&mut self, refs: R) -> &mut Self {
        let slots = refs.positions();
        let mut builder = self.builder.borrow_mut();
        let order = builder.fresh_order();
        let _ = builder.declare(self.body, DeclarationKind::Next, slots, order, false);
        drop(builder);
        self
    }

    /// 测试可控入口：当前 Definition 身份，用于构造“无来源位置”反例。
    #[cfg(test)]
    pub(crate) fn definition_id(&self) -> crate::core::engine::identity::DefinitionId {
        self.builder.borrow().definition_id()
    }

    /// 测试可控入口：下一个将分配的 Definition 内位置编号，用于构造“前向来源”反例。
    #[cfg(test)]
    pub(crate) fn next_position_id(&self) -> crate::core::engine::identity::PositionId {
        self.builder.borrow().next_position_id()
    }
}
