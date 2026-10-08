# SRFlow Core Runtime 实现设计 v0.1

> 状态：**已审定的 Core Runtime 内部实现基线**。规范切换日期：2026-10-03。上位总规范为 [SRFlow Design v2.1](SRFlow_Design_v2.1.md)，Core 语义依据为 [Core Design v0.1](SRFlow_Core_Design_v0.1.md)。本文件规定 Runtime 的内部结构、操作顺序和验收条件；未冻结的精确 Rust 接口与未完成的整合验证仍按 §24、§26 处理。当前从已清理的最小 crate 重写；旧实现的历史验收不自动成为本基线的实现证据。
>
> 证据：[Core Compile Probe 计划](SRFlow_Core_Compile_Probe_v0.1.md)、[P01～P07 结论](SRFlow_Core_Compile_Probe_Results_v0.1.md)及其[最小实现与逐项记录](../../v3/SRF_Core_Compile_Probe/)。七项局部 Probe 均 PASS；P01 的调用模型和 P02～P07 的 Data Core 尚未端到端整合。因此，下文标为“采用”的内容是本版实现选择，标为“待验证”的接口或算法不能借局部 PASS 冒充已验证实现。

## 1. 文档目的与范围

[Core Design](SRFlow_Core_Design_v0.1.md) 规定**执行语义必须是什么**；本文回答**在不改变这些语义的前提下，Runtime 内部怎样组织**。本文覆盖 Root 执行入口、ExecutionContext、异构 Data 存储、逻辑与物理身份、Scope、Invocation、Node／Orchestrator 调用、Flow／Match／Each／Loop 的内部流程、错误清理和 Root 提取。

本文可讨论内部 Rust 类型、trait、枚举和类型擦除，但代码片段只是实现候选，不是最终 public Builder API。derive macro、DSL、Bundle、parallel Each、通用 Collection、持久化、分布式、完整 tracing、极限性能优化和发布包装不在本版范围内。

**最小化原则：** Probe 已证明简单机制足以承载语义时，不因为转入正式实现就主动增加抽象层；也不照搬 Probe 为测试而暴露的接口或临时存储结构。

## 2. 实现目标与硬约束

第一版应在 stable、safe Rust 下保持可追踪的所有权和顺序执行。运行错误不得使仍存活的 Scope 留下半绑定输出、双 owner 或指向已移除 DataId 的有效引用。默认调用路径不隐式 `Clone` 业务 Data。

以下边界不可合并：`Node ≠ Orchestrator`、`RefId ≠ DataId`、`DataScope ≠ DataContainer`、`Invocation ≠ Scope`、`Definition ≠ Execution`。Root 边界接收和返回 owned 值；内部边界传递 `RefTarget`，只有 Node 新输出进入 DataContainer，Each 收集等内部重组仍归 DataContainer 管辖。

实现不得为了借用检查方便，让 Node 按值消费已有 Data、给 child 创建第二个 DataContainer、让 Scope 任意读取容器全局数据、把 imported Data 变成 child-owned、为 Each item 隐式复制或分配独立 DataId、在 Orchestrator 之间取出业务值再传递，或强迫 Node 和 Orchestrator 实现同一个运行时业务 trait。旧 `Binding` 不重新成为 Core 接线对象；业务投影与转换仍交给 Node。

## 3. 总体结构和一条完整调用路径

```text
Application --owned input--> Runtime::execute(root)
                              └─ 新建 ExecutionContext
                                 ├─ DataContainer：本次 Execution 的唯一业务值存储
                                 ├─ Scope registry：refs、owned、父子关系
                                 └─ 当前 Invocation 状态
                                      └─ Root Orchestrator
                                           └─ CallSite
                                                ├─ Node adapter
                                                └─ Orchestrator adapter
                                                     └─ 同一 ExecutionContext

Root 正常输出目标 --全部校验--> DataContainer.take --owned output--> Application
```

例：Application 把 `A` 移入 Root；Root Flow 的 Step 从本地 `RefId` 解析到 `Data(D1)`，Node adapter 短暂借用 `&A`，Node 返回 owned `B`，ExecutionContext 插入 `D2` 并绑定 Step 输出。后续 SubFlow 只导入 `D2` 的目标；它完成内部执行后，由当前 ExecutionContext 的 CallSite 调用边界统一提交 child 输出到 caller：绑定 caller 的输出 RefId，并转移 child-owned Data 的责任，然后关闭 child Scope。Root 所有 Step 成功后，先校验全部声明输出，再把符合条件的值移交 Application。

## 4. Runtime 与 ExecutionContext

### 4.1 Root 入口

`Runtime::execute` 是 Application 进入**新 Root Execution** 的唯一边界：创建 ExecutionContext 与 Root Scope、注入 owned Root Input、启动 Root Orchestrator、校验并提取正常输出，最后结束 Context。即使实现上 `Runtime` 是无状态对象或一组函数，它也不承担内部递归时“再次执行 Root”的含义。

### 4.2 当前 Execution 的内部入口

`ExecutionContext` 管理本次执行的数据、Scope 与当前 Invocation，并提供受控操作：创建／退出 Scope、解析本地引用、短期借用、Node 输出登记、import／export、内部 consume／promote，以及分别调用 Node 和 Orchestrator。`ctx.invoke_node(...)`、`ctx.invoke_orchestrator(...)` 是说明职责的候选名称，签名待整合验证。

Context 提供共同机制与不变量校验；Flow 的 Step 循序、Match 的路由、Each 的逐项遍历及 Loop 的策略决策由各 Orchestrator 实现，不能集中到 Context 的控制类型分派中。

内部 child Invocation 继续使用当前 Context 和 DataContainer。Root 调用与 nested 调用不得共用会重新分配 DataId 空间的入口。Context 的具体字段可拆成私有组件，但这些组件不能成为业务 Node 可访问的隐式数据通道。

## 5. 异构 Data 与类型擦除

DataContainer 必须同时持有不同业务类型。第一版候选是按 DataId 索引的私有 entry，其中存放类型信息和 owned erased value；Probe 使用 `HashMap<DataId, Box<dyn Any>>`，足以说明 safe Rust 中可行，但是否使用这一精确布局仍待实现阶段决定。

```text
DataId -> DataEntry { type identity, owned value }
```

类型安全有两道边界：Definition 的 `DataRef<T>`／Signature 使调用方错误连接尽量在构建时失败；Runtime 的 type metadata 或受控 downcast 用来发现内部绑定错误和不可信的 stale target。业务接线不能退化为“先用 `Any` 接上，执行时再猜类型”。P01 的四个错误连接在 `then` 处编译失败，P02～P07 用运行时类型检查保护容器操作。

`Data` 可以先以 `Any`／`'static` 约束的 marker 形态讨论，不预设运行时方法。是否要求 `Send + Sync`、能否使用 `Box<dyn Any + Send>`，取决于后续异步与线程模型；P01～P07 的同步局部结果不足以冻结这些 bounds。

## 6. DataId 与 DataContainer

### 6.1 身份策略

**本版实现决定：DataId 在单次 Execution 内单调分配且不复用。** 销毁或取走后不得把同一身份赋给新值。这样旧 target 不会因槽位重用指向另一个实例；若未来改成可复用 slot，必须引入 generation 并重新验证 stale target。DataId 不跨 Execution 使用，具体整数宽度及溢出处理待实现时确定。

### 6.2 最小操作面

容器只需受控的 `insert_owned`、按类型借用、目标有效性验证、内部 `drop`／`consume_into_collector`、Root `take`。`contains` 等观测可留给内部断言；不设计能绕过 Scope 的万能业务存储 API。每次移除 DataId，都要同步撤销或阻止所有可能成为有效入口的旧引用；关闭 Scope 后残留的纯目标值也不能重新 resolve。

### 6.3 借用阶段

调用 Node 前，Context 从**调用方当前 Scope 的本地引用**取得 target，验证身份、类型和生命周期，再生成短期 `&T`。Node 调用或对应 Future 完成后，借用结束，才允许执行会移动／清理该 Data 的 Scope mutation。异步场景中借用可跨 `await` 的适配形式和 Future 的 `Send` 限制待验证；不能把 Rust `&T` 长期保存在可变的 Scope registry 中。

## 7. RefId、DataRef 与 Definition 身份

不同逻辑 `DataRef<T>` 必须有不同 RefId。同一 Definition 被多次调用时，其 RefId 在不同 DataScope 中可分别绑定到不同 target。本版建议由**完整编译 Definition 树**的 builder 共享一个单调 RefId 分配器，覆盖 Flow、Match、Each 与 Loop 的子定义；若支持独立构建后组合，组合阶段必须证明无碰撞或进行受控重映射。

`DataRef<T>` 可内部表示为 `RefId + PhantomData<T>` 一类轻量句柄；其 `Copy`／`Clone` 只复制逻辑引用，不复制 Data。跨 Definition、跨 Flow 的本地引用不能直接在别的 Scope 查找。构建时应核对所属 Definition、签名与可达性；具体采用 DefinitionId 还是全局唯一 RefId 加归属表，**未冻结**。P01 证明强类型接线可行，没有证明完整归属检查。执行时也必须只从当前 Scope 的 `refs` 查找，不能因 RefId 数值全局唯一就越过 Scope。

**Scope-local RefId 单赋值。** 同一个 RefId 在同一 DataScope 生命周期中只允许从 Unbound 变成 Bound 一次；已有绑定不得覆盖，即使新 target 类型相同。Definition 阶段能发现的重复接线应作为 Definition Error 拒绝；运行时再次绑定属于 Internal Invariant Error。Loop 的 current state 和 collector 等可变控制状态放在控制器自己的 runtime state 中，不通过重绑 Definition Ref 表达。同一 RefId 在同一 Definition 的另一次 Invocation 中可以绑定到另一个新建 Scope，这不违反单赋值。

## 8. RefTarget 与 resolve

最小目标形态可按 Probe 表示为：

```rust
enum RefTarget {
    Data(DataId),
    CollectionItem { collection: DataId, index: usize, lifetime_cap: ScopeId },
}
```

字段和名字是内部候选。`RefTarget` 只表示定位与生命周期能力，**不拥有业务 Data**。业务调用通过当前 Scope 的本地绑定取得输入；控制器也可从经合法 Promote 保留的 runtime state 取得目标，再受控导入新 child 的本地输入 RefId。这两条来源均须验证 DataId 存活、类型及责任链，不能凭任意 DataId 全局读取。CollectionItem 还须先验证 cap Scope 仍有效、请求方位于其有效后代范围、集合 DataId 存活且索引有效，再短暂借用 `Vec<T>` 中的 `&T`。保存 target 不等于保存 Rust borrow；关闭 ItemScope 后 target 即使被复制出来，也不得再解析。跨 Scope 的 import／Export／Promote 均不能绕过 cap 检查。

## 9. DataScope：可见性与生命周期责任

Scope 的最小内部状态是 `parent`、单赋值的 `refs: RefId -> RefTarget`、`owned: Set<DataId>` 和生命周期状态；Each／Loop 可附加只服务其语义的私有元数据。`ScopeId` 用于父子关系与 CollectionItem cap。`owned` 是**处置责任登记**，不是另一份 Data 存储。同一 DataId 在存活 Scope 中恰有一个责任方；导入只增加目标绑定，不能增加 owner。

**ScopeId 在单次 Execution 内单调分配且不复用。** Closed Scope 的身份永不代表另一个新 Scope，因而保留的旧 CollectionItem cap 不能因 ID 重用而重新有效。具体整数宽度与溢出处理待实现时确定；若未来复用 Scope slot，必须引入 generation 并重新验证 stale cap。

状态至少需要区分可创建引用和 child 的 Active、禁止新业务调用的 Finalizing、不可再解析的 Closed。实际 enum 可更简单，但必须实现这些可观察边界。父 Scope 不能在 descendant 存活时关闭；Invocation 与 Scope 不要求一一对应。

### 9.1 Import

调用方从自身 `refs` 或合法保留的控制器 runtime state 取得输入并校验目标，创建 child 的本地 RefId 绑定。后者用于将 Loop 当前状态导入新的 RoundScope，不修改 LoopScope 的 Definition Ref。若目标是 CollectionItem，还需确认 child 在 cap 内。此过程不移动或复制业务值、不改变 `owned`。失败时 child 不留下部分输入绑定；多输入导入应整体预检后提交，或在不可见的 child 建立阶段失败并完整撤销。

### 9.2 Export

**Export 是 Definition output 跨 Scope 的唯一 Ref-binding commit。** 它用于 SubFlow、Match、Each 最终集合和 Loop 最终输出等向 caller 暴露声明输出；不涵盖所有跨 Scope 的数据处置。Child Orchestrator 在自身 Scope 中完成执行，只确定并准备其声明的 child-local 输出目标；它不知道 caller 的 RefId，也不自行向 caller 绑定。CallSite 保存 caller 输出 RefId，由当前 ExecutionContext 的调用边界取得两侧 Scope 和 Signature 后统一执行 Export。Node 没有这种 child Scope Export：Node 新产出的 owned Data 由同一调用边界登记到当前 Scope，并把其输出 RefId 绑定一次。

调用边界先冻结 child 的输出集合，验证 child／caller 关系、child 输出引用、类型和 target 存活、CollectionItem cap、caller 目标 RefId 尚未绑定，以及每个 DataId 的 owner 情况。多个输出可合法别名到同一完整 DataId：caller 获得各自的只读逻辑绑定，生命周期责任对该 DataId **只转移一次**；重复别名不能产生第二个 owner，也不能让 CollectionItem 越过 cap。这与 Root 最终重复提取同一 DataId 必须拒绝是不同边界。验证完成后，在同一不可观察的提交边界内绑定 caller 输出 RefId；若目标确由 child 负责，同时把责任从 child 转给 caller。只是 imported alias 时仅绑定 caller 引用，不能制造第二位 owner。**只有 commit 完成后才能关闭 child Scope；不能先关闭 child、返回裸 target，再尝试绑定 caller。**

P02 的顺序执行 Probe 采用“所有可失败校验在前、其后无可恢复错误分支”的提交方式，拒绝预提交验证时没有半成品。正式实现必须把**整组声明输出**纳入此规则；多输出原子提交、内存分配失败／panic 和并发执行均未被 P02 证明，不能写成已验证结果。对本版顺序 Runtime，提交期间不得让其他调用观察中间状态。

### 9.3 Promote：保留到控制器状态

Promote 把 RoundScope 等内部 Scope 的选定目标保留到父控制器的 runtime state，**不绑定父 Scope 的 Definition RefId**。提交前验证两侧 Scope 关系、目标存活、类型、生命周期及 owner；若完整 Data 由来源 Scope 负责，则原子地转移到控制器 Scope 的 `owned` 并更新控制状态。若是 imported target，只更新控制状态，原 owner 不变。CollectionItem 不能借 Promote 突破 cap。

Iter 通过 Promote 更新 Loop 的 `current_state`；Retry 完成时也可用同一保留机制把本轮结果交给 Loop 的最终结果状态，随后 Loop 的对外声明输出才走 Export。新旧同一 DataId 不销毁或新增责任。不同 DataId 的旧状态可能仍被本轮输入引用，因此在 RoundScope 的引用失效并关闭后，再检查它是否由控制器负责且已不被任何有效引用或控制状态使用，满足条件才清理。

### 9.4 Consume：消费进内部建构状态

Consume 用于 ItemScope 正常结果进入 DataContainer 的 collector，**不绑定 EachScope 的 Definition RefId，也不需要先把 DataId 转入 EachScope.owned**。在 ItemScope 仍有效、其 descendant 和业务借用已结束时，解析其声明输出，验证目标为完整 `Data(D)`、`ItemScope.owned` 包含 D、类型匹配，且 collector 确由该 ItemScope 的 parent EachScope 管理。Imported Data 和 CollectionItem 均在移动前拒绝。

预检通过后，在不可观察且无可恢复失败分支的提交段中，从 ItemScope.owned 移除 D，将 owned 值在 DataContainer 内部移入 collector，使旧 D 及其本地引用失效，清理剩余 owned 后关闭 ItemScope。收集后的业务值仍物理位于 DataContainer，随 collector 的 EachScope 生命周期清理。ItemScope 的 `owned` 提供本 item 调用链的责任证据，不再额外维护 `collectible` 集合。

P04 证明的是“先 Export 到 EachScope，再按 collectible 消费”的局部路径；本版选择的直接 Consume 不应被表述为已经由 P04 原样验证。整合时须在新的边界上重跑 imported 完整 Data／CollectionItem 拒绝、合法新输出移动、旧身份失效及失败清理样本。

### 9.5 正常 finalization

语义次序为：child 的全部业务执行与 descendant 调用完成 → 停止新 child 与新业务借用 → 当前 ExecutionContext 的边界解析并预检待保留或消费的输出 → 按用途完成 Export、Promote 或 Consume → 使 child 本地引用失效 → 清理剩余 child-owned Data 与 collector → 关闭 child Scope。已有借用必须先结束。三种操作各有用途，不能为 Each item 或 Loop round 动态分配 RefId，也不能重复绑定同一父 RefId 来模拟 runtime slot。正常 commit 是该来源 Scope 成功路径最后一个输出处置动作；其后不再在该 Scope 调用 Node／Orchestrator，也不再执行可能返回普通 Execution Error 的业务逻辑。父控制器可在来源 Scope 关闭后继续下一项或下一轮。任何移除 DataId 的操作都要阻止仍有效的 RefTarget 继续解析它。Probe 的 `finalize` 方法只覆盖局部关闭，不能直接当作完整 finalization 算法。

## 10. Invocation 状态

Invocation 表示一次实际调用，Scope 表示数据可见性与生命周期边界。第一版可用当前调用栈或等价的结构记录父子关系和错误上下文；若不需要执行结束后保留全树，就不为 tracing 预建持久 Invocation tree。是否分配 InvocationId 取决于错误定位或内部不变量是否实际需要，不能把它与 ScopeId 合并。

每次 CallSite 执行都经过当前 ExecutionContext 的内部调用边界。轻量 Node 调用可以沿用已有 Scope；SubFlow、Branch、Item、Round 等按 Orchestrator 语义创建 Scope。调用结束前须完成自己创建的 descendant Scope 的退出。

## 11. Node Runtime Adapter

普通 `fn(&A, &B) -> Result<O>`、异步借用函数及持有固定配置的结构体 Node 是既有借用式 Probe 支持的候选表达；P01 进一步证明同步普通函数、`Arc<具体结构体 Node>`、子 Flow 可从一个 typed `then` 进入异构 Step。最终 trait、宏及 async `Send` bounds **未定**。

Node adapter 保存 Definition 输入 RefId／输出 RefId 和调用对象。执行时由 Context 从当前 Scope 解析输入，取得短期只读借用，调用 Node；Node 成功返回 owned `O` 后，由 Context 插入 DataContainer、分配 DataId、登记当前 Scope 的 `owned` 并绑定输出 RefId。Node 只见 `&A` 等业务借用和自己的配置，不见 DataRef、RefTarget、DataContainer 或 ExecutionContext。

`Result<()>` 成功只表示完成动作，**不分配 `DataId<()>`，不制造 `RefTarget`**；错误直接进入执行错误通道。零输入 Node 的运行路径不要求 `()` Flow Input 已被审定。Node adapter 是 CallSite 内部机制，不把 Node 变成 Orchestrator，也不能把已有 Data 按值传入 Node。

## 12. Orchestrator 内部调用协议

Orchestrator 接收按其 Input Signature 排列的运行时目标，通过当前 Context 使用其调用 Scope、递归执行 child，再准备按 Output Signature 排列的 **child-local 输出目标**；它不拥有输入业务值，也不以 Rust owned `I -> O` 模拟内部调用。概念协议是：

```text
Input Targets + current ExecutionContext
    -> Orchestrator invocation
    -> child-local Prepared Output Targets 或 Execution Error
```

这些目标只供仍存活的 child Scope 内的 finalization 使用，不能作为已经完成生命周期交接的普通返回值。对有 caller 声明输出的调用，当前 ExecutionContext 的 CallSite 调用边界负责按 §9.2 Export、单次绑定 caller RefId 并关闭 child。Each 的 ItemScope 和 Loop 的 RoundScope 包装边界则分别按 §9.4 Consume 或 §9.3 Promote 处理内部结果，不向父 Scope 绑定重复的 Definition Ref；其内部 body 若为另一个 Orchestrator，仍可正常 Export 到当前 ItemScope／RoundScope 的一次性本地引用。Root Orchestrator 进入 §18 的 Root 输出边界。所有操作由当前 ExecutionContext 管理，Orchestrator 不接触 caller 的 Definition RefId。

目标包可为 typed tuple、带签名的内部 target list，或受控 erased representation；P01 的单输入 `usize` 和 P02 的手工目标结构都不足以冻结 `Targets<I>`。选择必须同时处理多输入／多输出、`()` 输出、强类型 Builder 到运行时的桥接及异构存储。Orchestrator 的精确 trait 签名保留为整合设计问题，不在本文虚构已编译的正式签名。

## 13. CallSite 与 Step

CallSite 是 Definition 中的通用调用描述，保存被调用对象、逻辑输入／输出位置和必要签名信息；它本身不保存某次 Execution 的 DataId。Flow Step 只是有顺序位置的 CallSite，Match branch、Each body、Loop body 也通过 CallSite 指向 child。需要 Export 时，CallSite 提供 caller 输出 RefId，实际 commit 由当前 ExecutionContext 的调用边界执行。Each／Loop 的内部结果处置不要求在父 Scope 中另造 caller RefId，而是由其包装边界 Consume／Promote。CallSite 本身和 child Orchestrator 都不再各自做一次 caller 绑定。

P01 已验证一个可行内部形状：`CallSite::Node(Box<dyn NodeCall>)` 与 `CallSite::Orchestrator(Box<dyn OrchestratorCall>)`，在 typed `then` 接线后擦除具体类型，`Vec<CallSite>` 可异构保存并分别 dispatch。二者不共享业务运行时 trait。正式实现可以换成等价封装，但应保留**构建时类型检查、执行时双路径分派**。Probe 的 Marker tuple 用于避免 trait impl 重叠，是实现提示；具体 Marker 类型不应进入公共 API 承诺。

## 14. Flow Executor

Flow Definition 必须先完成明确的 Output Signature，才允许执行或作为 child 组合；显式 `()` Output 是已完成定义，不是缺失输出声明。具体由 Builder typestate 或定义验证承载，接口形式仍待任务落实。

进入 Flow 时绑定其 Input Signature；SubFlow 的 Scope 由调用边界建立，Root Flow 使用本次 Root Scope。按 Step 定义顺序逐项执行：从当前 Scope 解析输入 RefId → 经 CallSite 调用边界启动 Node 或 Orchestrator → 由该边界把正常输出绑定到 Step 的 caller RefId；若被调用者是 Orchestrator，同时按 §9.2 完成 child Scope 的责任交接和关闭。Flow Executor 不另设第二套输出 commit。即使后一 Step 不读取前一输出，也不能自动并行。

某 Step 出错，停止后续 Step，按 §20 清理当前 Flow 调用 Scope 并传播错误。所有 Step 成功后，Flow 只准备自己的声明输出；SubFlow 的输出由调用它的边界统一预检、导出和 finalization，Root Flow 由 §18 的 owned 提取边界结束。Root Flow 与 SubFlow 可共享顺序执行主体，但输入注入和输出移交边界不同。

## 15. Match Runtime

Match 先从明确输入解析已有的路由 Data，由内部 route lookup 选择一个 branch；复杂业务判断须先由 Node 产生 Data。未被选中的 branch 不创建 Scope、不调用 child。选中后，当前调用边界为 branch 建立 BranchScope、导入显式输入，经该 branch 的 CallSite 调用 child，并由同一边界把 branch 声明输出提交到 Match 的调用 Scope 后关闭 BranchScope。Match 再准备自己的共同 Output Signature，由调用 Match 的上层边界提交；Match 不另行绑定 caller RefId。

仅在配置了 default 时，未命中可进入 default；无匹配且无 default 是执行错误。被选 branch 调用失败不能改选另一 branch，也不能把错误当成正常业务拒绝。路由键的表示、分支登记结构和精确错误类型属于本版待定实现细节。

## 16. Each Runtime

EachScope **只持有 CollectorId 等句柄与控制元数据**，并导入 `Vec<T>` 集合及显式 shared inputs；它不在自身字段中物理持有 `Vec<O>` 或已收集的业务 `O`。按索引顺序为每个 item 创建 ItemScope，把 `CollectionItem(collection DataId, index, ItemScope cap)` 绑定为本地目标。body 由 CallSite 调用 Node 或 Orchestrator；descendant 仅在 cap 内导入 item 目标。空集合不调用 body，最终形成空的 `Vec<O>`。

collector 的物理 `Vec<O>` 与其中的业务值存于 DataContainer 的内部建构区，可概念化为 `CollectorId -> ErasedCollector`；它由 EachScope 的句柄定位，但从未成为 EachScope 物理持有的值。完成前没有普通 DataId，也不能被 Node 借用。Body 成功后，在仍存活的 ItemScope 中解析声明输出，并按 §9.4 直接 Consume：目标须为完整 Data，且当前由该 ItemScope 承担责任；collector 须属于其 parent EachScope 并具有正确类型。Body 为 Orchestrator 时，先把其新 owned 输出正常 Export 到 ItemScope；body 为 Node 时，新输出直接登记在 ItemScope。两者都通过 ItemScope.owned 证明可消费来源，**不再使用 EachScope.collectible，也不把逐项结果绑定到 EachScope RefId**。

imported 完整 Data 即使被 body 原样输出，也不属于 ItemScope.owned；CollectionItem 不是完整 owned Data；EachScope 自己拥有的其他 Data 也不属于当前 ItemScope。三者均须在任何内部取值前拒绝。合法结果被 Consume 后，从 ItemScope.owned 移除，旧 DataId 失效，旧本地引用不能再 resolve；随后关闭 ItemScope。完成全部 item 后，collector 在 DataContainer 内部形成新的普通 `Vec<O>` DataId，登记为 EachScope owned，最终集合的声明输出才向 parent Export。若某项失败，后续项不执行，部分 collector 随 EachScope 清理，不返回部分正常集合；外部副作用不回滚。

## 17. Loop Runtime

LoopScope 保存原始输入 target、当前状态 target、必要的最终结果 target 及其 Scope 责任表；每轮创建新的 RoundScope，按策略把控制状态受控导入本轮的输入 RefId，并经 body CallSite 调用。**可变控制 target 与 `owned`、Definition RefId 绑定分开表示**。P05／P06 的最小代码通过覆写 `refs[current_ref]` 和 candidate 引用模拟状态推进；正式实现不复制这些 runtime Ref slot。Round body 完成后，在 RoundScope 仍有效时读取正常 Output 所表达的业务状态并作出继续／完成决定：需保留的目标经 §9.3 Promote 提交到 Loop 的控制状态，不绑定 LoopScope RefId，再关闭 RoundScope；要丢弃的结果不提交，正常清理 RoundScope。Round 关闭后才进入下一轮，不在已提交的 Round 上继续执行业务调用。Round 执行错误立即传播，不自动成为 Retry。

Retry 继续时丢弃本轮正常结果，下一轮重新使用原始目标；完成时先 Promote 所选正常结果到 Loop 的最终结果状态，Loop 结束后才按对外 Signature Export。次数、零次配置、耗尽结果等未在 Core v0.1 冻结，本文不补写旧规则。

Iter 继续时以 Promote 把本轮选定 target、必要的 Round-owned 责任及 current-state 更新一起提交；imported target 的原 owner 保持不变。旧 DataId 只有在由 LoopScope 负责、与新 DataId 不同且不再被有效引用或其他控制状态使用时，才由 DataContainer 清理。若新旧 target 指向**同一 DataId**，不销毁、不新增 owner。P05／P06 已验证这些局部责任规则；改用无 Ref-binding 的 Promote 路径后仍须在整合中复验。Loop 最终声明输出由正常 Export 规则交给 parent。item stream、停止策略与 Retry 耗尽规则仍是开放项。

## 18. Root Input 与 Root Output

Application 的 owned Root Input 经 Runtime 插入唯一 DataContainer，取得 DataId，并绑定 Root 输入 RefId。除 Root Input 和 Node Output，内部重组不能成为任意新业务 Data 注入点。

Root 正常结束时，应一次性解析全部声明 Output RefId，验证每个 target 是可提取的完整 Data、类型正确、由 Root 承担责任、DataId 存活且互不重复。**比较 DataId，不只比较 RefId。** 任何检查失败时取值次数为零。预检通过后，在其他执行路径不可观察、且不再有可恢复失败分支的提交段中，把每份输出从 DataContainer `take` 并在同一提交边界从 `RootScope.owned` 移除，完成 RootScope → Application 的所有权移交；然后关闭 Root 引用并清理未选中的 Root-owned Data。不能让 Root cleanup 再处理已移交 Application 的 DataId；`()` 输出不分配或提取 Data。

P07 只实测了两个输出的 tuple。任意 tuple arity、异构输出包和失败退出时 Root 的清理算法仍需正式实现与整合测试。内存分配失败或 panic 的处理不由 P07 的“零次提取”结论推导。

## 19. 错误边界

内部至少区分三类：Definition 构建／连接错误、业务 Node 或 child 调用产生的 Execution Error、表示实现不变量被破坏的 Internal Invariant Error。名称和 Rust enum 布局未冻结。缺失路由、无效 target、CollectionItem 生命周期越界、重复 Root DataId、Export 校验失败等应有明确诊断位置；不能用普通业务“不接受”代替 Execution Error，也不能把正常拒绝偷换成技术失败。

错误传播保留原始 child 错误及必要调用位置；不自动换 branch、跳过 item、Retry 技术错误或宣称外部副作用已回滚。内部 invariant 失败与可预期的业务调用失败应区别处理，避免已经部分提交后再返回一个看似可恢复的普通错误。

## 20. 失败时的 Scope 清理

Node、Orchestrator、Match branch、Each item 或 Loop round 在 Export／Promote／Consume commit **之前**失败时，停止该路径后续调用，不提交该边界的正常结果。先结束活跃 descendant，再撤销本地有效引用和对应控制状态；由 DataContainer 清理该 Scope 剩余 owned Data、未完成 collector 和尚未保留的中间值；ancestor-owned imported Data 保持其原 owner。最后传播错误。commit 后不再执行该来源 Scope 的业务动作或返回普通 Execution Error；随后只进行不可观察的本地引用失效、剩余 owned 清理与关闭。父控制器后续失败仍须清理自己接收的 owned 状态或 collector，但不能误删 imported ancestor Data。

这是一条需要整合验证的实现流程：P02 仅验证了顺序执行中的预提交 Export 失败，P04 验证了 collector 的局部拒绝；取消、Future drop、panic、多个输出部分准备失败及外部副作用不在七项 Probe 的完整覆盖内。正式实现需为每种失败点定义清理动作，确保没有 detached child Scope，也不把“失败后保留状态供 Probe 观察”的测试手段写进 Runtime 契约。

## 21. Runtime 内部不变量

实现中的断言与测试至少覆盖下列清单：

| 编号 | 不变量 |
| --- | --- |
| I01 | DataId 仅属于当前 Execution；失效身份不能再次解析。 |
| I02 | 业务输入来自本地已绑定 RefId；控制器只能使用合法保留的 runtime target，并受控导入新 child；不按任意 DataId 全局读取。 |
| I03 | 每个存活 owned DataId 恰有一个生命周期责任 Scope；import 不增加 owner。 |
| I04 | 父 Scope 不在 descendant 存活时 finalization；Closed Scope 不再产生或解析业务引用。 |
| I05 | Export 的整组输出绑定与责任交接无可观察的半提交状态。 |
| I06 | CollectionItem 仅在 cap 存活且请求方位于 cap 内时 resolve；item 不独立持有 DataId。 |
| I07 | Each collector 只 Consume 当前 ItemScope 声明的完整 owned output；不绑定 EachScope RefId，消费后旧 DataId 失效。 |
| I08 | Iter 替换 imported state 不改变原 owner；same-DataId 提升不销毁或重复拥有。 |
| I09 | Root 在所有输出完成类型、owner 和重复 DataId 校验前不 take。 |
| I10 | Node 只获得已有 Data 的只读借用；`()` 不成为业务 Data 实例。 |
| I11 | 一次 Root Execution 只有一个 DataContainer；内部 Orchestrator 只传 target。 |
| I12 | 错误退出没有半导出正常输出、悬空有效 target 或脱离父级的 child Scope。 |
| I13 | 同一 DataScope 内 RefId 只能绑定一次；Loop state 和 collector 不通过重绑 Definition Ref 更新。 |
| I14 | Definition 输出由调用边界 Export；控制结果由 Promote／Consume 处置而不绑定父 RefId；commit 后来源 Scope 不再有普通执行失败。 |
| I15 | Collector 业务值物理上始终位于 DataContainer 的内部建构区，EachScope 只持有句柄；Root take 同时移除 RootScope 的 owned 责任。 |
| I16 | ScopeId 在单次 Execution 内单调且不复用；Closed 身份永不重新代表新 Scope，旧 lifetime cap 不重新有效。 |

## 22. 内部性能原则

连接 DataRef、import／export target、Each 遍历和 Loop 推进都不隐式复制业务 Data。RefTarget 应是轻量描述，Scope 查找只复制描述；Root 输出只移动 owned 值。类型擦除和 map 查找在当前阶段服从语义正确性与可审计性，不因假设的性能问题引入 `unsafe`、slot reuse 或第二套存储。真实瓶颈需由后续测量决定。

## 23. Probe 结果到实现责任的映射

| Probe | 已证明的局部能力 | 正式实现要接入的责任 |
| --- | --- | --- |
| [P01](../../v3/SRF_Core_Compile_Probe/P01_RESULTS.md) | typed `then`、异构 Step、Node／Orchestrator 双分派 | CallSite 接入当前 Context 与真实 Scope；保留构建时类型检查 |
| [P02](../../v3/SRF_Core_Compile_Probe/P02_RESULTS.md) | 单 Context、import／export 和预提交拒绝 | 整组输出的原子提交与完整 finalization |
| [P03](../../v3/SRF_Core_Compile_Probe/P03_RESULTS.md) | `CollectionItem` 借用和 cap 拒绝 | 在 Each 的真实 item 调用路径上维持 cap |
| [P04](../../v3/SRF_Core_Compile_Probe/P04_RESULTS.md) | Export＋collectible 路径拒绝 imported output、消费合法新 Data | 用 ItemScope.owned 直接 Consume 复验同样正反例及错误清理 |
| [P05](../../v3/SRF_Core_Compile_Probe/P05_RESULTS.md) | imported initial state 保留原 owner | 在 Iter 的完整 Round lifecycle 上推进 |
| [P06](../../v3/SRF_Core_Compile_Probe/P06_RESULTS.md) | same-DataId 无重复销毁 | 与多轮出口和最终 Export 一起复验 |
| [P07](../../v3/SRF_Core_Compile_Probe/P07_RESULTS.md) | 两个 Root 输出先检验再提取 | 扩展到声明的完整 Output Signature |

Probe 代码是实验依据，不是正式代码模板。尤其 P01 的全局引用编号与极小 Context、P02～P07 的手动驱动接口、测试故障注入和观测计数，都不应不经审查进入 public API。

## 24. 尚未冻结的实现问题

以下选择保留开放：`Data` trait 与 erased entry 的精确形态；`Send`／`Sync` 与异步 Future bounds；Node adapter 的宏或 trait 组合、`Arc<dyn Node>`；Orchestrator trait 与多输入／多输出 target pack；DefinitionId 或归属表；RefId／DataId／ScopeId 整数宽度及溢出处理；多输出 Export 的具体提交结构；错误 enum、InvocationId 与调试信息；`()` Flow Input；Retry 耗尽、Iter item stream 与停止策略。开放项不得反向改变本文依赖的 Core 语义。DataId／ScopeId 单调且不复用、RefId 的 Scope 内单赋值、collector 的物理归属，以及 Export／Promote／Consume 的职责已是本版实现决定；新 Consume／Promote 路径仍须在整合中验证。

其中**最高优先级**是 P01 调用层和 P02～P07 Data Core 的端到端整合，以及失败／取消时的清理。若整合证明候选结构不适用，先记录最小反例，判断是实现表达问题还是 Core 语义问题，再修订对应层；不能在代码里静默放宽 ownership、借用或 Scope 边界。

## 25. 实施顺序

| 阶段 | 交付边界 | 主要回归依据 |
| --- | --- | --- |
| 1 | Execution 内 ID、异构 DataContainer 与短期借用 | P02、P03 的存储与 borrow 路径 |
| 2 | DataScope、import／Export、Promote／Consume 边界、正常和失败 finalization | P02 及多输出原子性补充样本；三种 commit 不重绑父 Ref |
| 3 | ExecutionContext 的内部调用和 Scope 生命周期 | P02、P03；禁止 nested Root |
| 4 | Node adapter、Orchestrator 路径、异构 CallSite | P01 接到真实 Context 的整合样本 |
| 5 | Root／SubFlow 的顺序执行 | P01＋P02 端到端 |
| 6 | Match BranchScope 与错误传播 | 选中／未选中／default／child 错误 |
| 7 | Each 的 item cap、ItemScope 直接 Consume 与失败清理 | P03＋P04 正反例接入真实 body；多 item 不产生父 Ref slot |
| 8 | Loop 的 Retry／Iter 已冻结推进语义和 Promote | P05＋P06 接入真实 Round；多 round 不重绑父 Ref |
| 9 | Root 全输出预检与 owned 提取 | P07 扩展到完整 Signature |
| 10 | 整合与故障路径验证 | I01～I16、取消／失败清理 |

阶段顺序允许为编译依赖微调，但每阶段只增加当前必需的抽象，并复用已通过 Probe 的正反例。规范已按 [v2.1 §25](SRFlow_Design_v2.1.md#25-审定与规范切换记录) 切换；详细任务由[当前任务入口](tasks/README.md)另行拆分、审定和授权，旧 v2.0 任务记录只作迁移参考。

## 26. 本版验收标准

只有当正式实现把 P01～P07 的局部机制串入**同一 ExecutionContext 的真实调用路径**，并重新验证对应正反例，才可称 Core Runtime 的关键实现语义通过。验收还需确认：无 `unsafe` 或隐式业务 `Clone`；Node 不依赖 Runtime 内部；Node 与 Orchestrator 双路径分明；Root 一次执行只有一个 DataContainer；Scope 隔离与唯一责任成立；Each 直接 Consume、Loop 无 Ref-binding 的 Promote、多项／多轮 Ref 单赋值和 Root 零部分提取成立；I01～I16 有对应测试；错误退出可清理自身数据而不误删 ancestor 数据。

本文件是已审定的内部实现基线；Runtime 代码、公开 API 与整合验收仍按相应任务完成情况判断，不能仅凭规范切换宣布实现已完成。
