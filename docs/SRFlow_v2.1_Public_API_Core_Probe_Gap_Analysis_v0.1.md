# SRFlow v2.1 Public API：Core 与 Probe 接口差距分析 v0.1

> 状态：**设计决策前的审查记录，待后续设计评审**。
>
> 日期：2026-10-08（Asia/Shanghai）。本报告经用户要求记录到文档；记录行为不表示其中的实现建议已获审定。
>
> 审查基线：本地 `main`，提交 `9bc1f9ac03324c28e4ff183f06d024f71bec317e`（`feat: add public api probe`）。审查时与本地缓存的 `origin/main` 一致，工作区干净；没有联网确认远端是否存在更新。
>
> Public 契约依据：[SRFlow v2.1 Public API SPEC v0.1](SRFlow_v2.1_Public_API_SPEC_v0.1.md)，状态为 USER CONFIRMED — FROZEN CONTRACT，含 Data 派生及零输入 Root。数据责任和生命周期依据：[Core Design](SRFlow_Core_Design_v0.1.md) 与 [Runtime Implementation Design](SRFlow_Core_Runtime_Implementation_Design_v0.1.md)。文档之间的具体差异在本报告中显式列出，不据此静默修改任何规范。
>
> 本报告只记录代码与文档证据支持的对照、静态发现、适用建议和未决项；不新增 Public API 契约，不批准正式 Definition／IR 布局，不修改源码，不更新任务或 Gate 状态。

## 1. 核心结论与证据边界

**Probe 已证明新 Public API 可以建立在现有 DataContainer／ScopeCoordinator 上，但尚未证明正式 ExecutionContext／Invocation／Orchestrator 能原样承接冻结 SPEC。**

可以保留数据责任底座；Definition 与 Node 接入需要适配；控制退出协议、成组 Consume／Promote 以及相关调用许可需要正式变更。现有证据不足以要求重写 DataContainer，也不足以决定必须新增一套独立、复杂的 IR。

本次审查按用户指定使用本地 `engineering/srflow`，没有联网。通过逐文件比较确认：

- Probe 的 16 个普通 Core 模块与当前正式 Core 完全相同。
- `scope.rs` 仅增加 `consume_item_group_probe` 与 `promote_group_probe` 两项扩展。
- `mod.rs` 的入口清单不同。
- 正式 `src/` 与 Probe 来源提交 `f9b6e2054b89e6295bc21766313aa8197d2e2ba2` 之间没有源码差异。

这些观察与 [CORE_PROVENANCE.md](../examples/SRFlow_Public_API_v21_Probe/CORE_PROVENANCE.md#L3) 的来源说明一致。完整扩展差异见 [CORE_SCOPE_DELTA.patch](../examples/SRFlow_Public_API_v21_Probe/CORE_SCOPE_DELTA.patch)。

本报告区分三种证据：

| 证据类型 | 本次如何使用 | 不授予的结论 |
| --- | --- | --- |
| 源码证据 | 确认数据结构、调用路径、检查与提交顺序；标明静态推导 | 不自动等同于编译或执行成功 |
| 测试源码 | 确认实际场景和断言范围 | 不把测试名称当成比断言更强的证明 |
| 历史执行记录 | 引用 Probe RESULTS 的已记载结果 | 不当作本次复跑或正式 Core 新接口验收 |

[RESULTS.md §2](../examples/SRFlow_Public_API_v21_Probe/RESULTS.md#L21) 记载增补后 71 项测试通过，包含一个验证 18 个编译拒绝样本的 harness。**本次没有复跑 Cargo 检查**，以遵守当时“只读、不修改任何文件”的审查范围；历史 PASS 不转换为本次 PASS。

Probe 虽然复制了完整 Core 模块，实际门面由自己的 `Run` 执行，不能据此认定完整 Core 调用链已被验证。[README §与原 Core 的关系](../examples/SRFlow_Public_API_v21_Probe/README.md#L44) 和 [RESULTS §5](../examples/SRFlow_Public_API_v21_Probe/RESULTS.md#L99) 也明确保留这项限制。

## 2. 总体分类

| 分类 | 内容 | 适用判断 |
| --- | --- | --- |
| 可直接复用 | DataContainer 的异构存储、类型检查和 collector 建构区；ScopeCoordinator 的 Import／Export、item cap、ownership、状态保留及 Root 整组提取 | 已有源码基础；正式接入仍须通过 Context 的受控边界 |
| 需要适配 | Schema 的逻辑位置及捕获关系；Flow／Body 的完成定义；Node／Query；Copy Ref 到内部 RefId 的映射 | Probe 提供候选模型，当前正式类型不能直接改名后使用 |
| 需要正式 Core 变更 | 可捕获控制退出；成组 Consume／Promote；多位置收口许可；现有调用路径中的 Shape、寿命和错误承载限制 | 单靠公开门面包装不能满足 SPEC |
| Probe 专属实现 | `Run`、直接访问 Coordinator 的 Step、`Rc<RefCell<Schema>>`、隐藏但公开的 helper、硬编码包名的 derive | 不应视为已冻结的正式布局或公开边界 |
| 不确定项 | 完整 Invocation 接入后的控制清理、成组故障诊断、公开 helper 的构建旁路、部分别名边界 | 有具体缺口或证据不足，不能授予整合合规结论 |

这里的“复用”主要指能力和现有实现主体，不意味着不经新的调用边界验证就授予正式合规结论。Shape／寿命一类差距可以通过扩展或替换现有内部 adapter 处理；它们不必同时改变 DataContainer 的存储语义。

## 3. Schema／Flow／Body／Step

### 3.1 Probe 已有的 Definition 模型

| 对象 | 实际职责 | 与正式 Core 的差距 |
| --- | --- | --- |
| `Schema` | 保存 Definition 身份、共享 RefId 分配器、类型端口、逻辑 Scope 树、Import、alias 和构建错误 | 正式 `Definition` 使用自己的输入／已生产位置／输出端口表，没有这套词法捕获树 |
| `Flow<'n>` | 同步注册步骤、建立 child Definition、允许借用 Node 句柄 | 正式 `FlowBuilder` 与完成态 `Flow<I,K>` 的类型和寿命模型不同 |
| `Body<'n>` | 保存逻辑 Scope、顺序步骤和明确输出位置 | 可作为完成定义候选；输入与输出签名还需衔接正式调用边界 |
| `Step` | 擦除具体步骤，直接执行 `&mut Run` | 正式 Step 通过 `CallSite::Node`／`CallSite::Orchestrator` 进入 Invocation |

主要证据：

- [Probe `src/flow.rs`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L24)：`Port`、`ScopeDef`、`Schema`、`Run`、`Step`、`Body`。
- [Probe `Flow`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L180)：`child`、`body`、`then` 与六类编排操作。
- [正式 `src/core/builder.rs`](../src/core/builder.rs#L31)：`CallSite`、`Step`、`Definition`、`run_definition`、`run_site`。

### 3.2 值得保留的机制

1. **Definition 与 Execution 分离。** `Raw` 保存 Definition 身份、逻辑 Scope 和端口位置，不保存运行时 DataId／ScopeId。`Body` 也不保存某次执行的业务值。
2. **整棵逻辑树共享位置分配来源。** 多次调用同一 body 时，在新建运行 Scope 中绑定相同 Definition RefId，保持 Scope-local 单赋值。
3. **明确输出与严格顺序。** closure 返回的 RefShape 成为 body 输出；`Body::execute` 按步骤逐项 `await`，没有按依赖重排或并发。

不能把 `Body` 直接等同于正式 `OrchCall`。正式协议还要求可验证的输入 pack、声明端口和受控的调用、收口。`OrchScope` 不向编排体公开任意可变 Context。见 [正式 `OrchCall`](../src/core/orchestrator.rs#L59) 和 [`OrchScope`](../src/core/orchestrator.rs#L257)。

统一内部 `Step` 擦除本身不意味着 Node 与 Orchestrator 的业务协议被合并；问题在于 Probe 的实际执行没有落实正式 Invocation 权限和退出协议。

**适用建议：**保留“类型端口＋逻辑 Scope 树＋捕获转 Import＋顺序步骤”的候选模型，先验证它如何进入正式 CallSite。是否另称 IR、是否复用现有 `Definition` 布局，仍是设计选择。

## 4. Node／Query、Ref 与 Shape

### 4.1 可复用的借用与存储机制

Probe 的 `NodeStep::execute` 从 Coordinator 取得不可变输入借用，跨 `await` 调用业务 Node，借用块结束后才登记 owned 输出。正式 `Leaf1／Leaf2` 同样按“共享重借用 → 调用 → 可变登记”组织执行。

证据：[Probe `NodeStep::execute`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L418)、[正式 `Leaf1::invoke`](../src/core/node.rs#L344)。

这支持借用底座兼容的判断，但 Probe 没有进入 Leaf frame，也没有保留正式 leaf 在业务执行前的输出位置预检，不能据此认定 Node Invocation 已接入。

### 4.2 类型与调用适配差距

| 契约 | Probe／冻结 SPEC | 当前正式实现 | 判断 |
| --- | --- | --- | --- |
| Struct Node | `Node::Input／Output`，`Query<&Self::Input>` | `NodeCall0／1／2` | 需要新的业务协议适配 |
| Function Node | 自然 `Query<&A>`／引用 tuple；异步调用 | 0／1／2 个直接借用参数，同步与异步分开适配 | 不能直接复用原函数 adapter |
| Node 输入位置 | 零、单、2～16 | 当前 leaf adapter 为 0／1／2 | 调用适配层须扩展或替换 |
| `()` Node 输出 | 自然返回，零数据槽 | 普通函数的 unit 输出在 builder 中拒绝；结构体通过 `Unit` 分类 | 现有 builder 规则须调整 |
| Node 句柄寿命 | owned、`&Node`、`Arc<Node>`；允许非 `'static` 定义依赖 | CallSite trait object 未带定义寿命，多项 `BuildSite` 要求对象 `'static` | 不能直接承载普通栈上 Node 借用 |
| Public Ref | `Ref<T>: Copy` | `DataRef<T>` 仅实现 Clone，内部 RefId 持 Arc | 需要外层轻量句柄映射，不能简单类型别名 |
| Data | 空 marker trait，与 derive 同名导出 | `Data<O>` 是输出分类标记 | 名称相同，职责不同 |
| Root Input | `()`、单值、2～16 tuple | 1～16 tuple，单值需要 `(A,)`；没有零输入映射 | Root 类型映射须调整 |
| 编排／Root Output | `()`、单 Ref、2～16 Ref tuple | `Unit／Data<O>／Out2` | 输出分类和组装须扩展或替换 |

证据入口：

- [SPEC §3～§5](SRFlow_v2.1_Public_API_SPEC_v0.1.md#L33)：Data、Shape、Runtime、Node 与 Query 的冻结范围。
- [Probe `QueryType／Query／InputSpec`](../examples/SRFlow_Public_API_v21_Probe/src/shape.rs#L58) 与 [`Node／Callable`](../examples/SRFlow_Public_API_v21_Probe/src/shape.rs#L334)。
- [正式 `NodeCall0／1／2`](../src/core/node.rs#L32) 与 [`BuildSite` 适配及寿命约束](../src/core/builder.rs#L510)。
- [正式 `DataRef`](../src/core/data_ref.rs#L23)、[`OutputKind／OutKind`](../src/core/signature.rs#L81)、[`RootInputs／RootOutputs`](../src/core/root_signature.rs#L26)。
- [正式 builder 的 unit 输出拒绝](../src/core/builder.rs#L351)。

这些差距不要求改变 DataContainer 对 `Any／'static` 值的存储方式。**业务 Data 的 `'static` 要求与 Node 句柄的定义期寿命必须分开处理**，不能用更强的 Node `'static` 限制缩窄已冻结契约。

Root 入口也不只是改名：Probe [`Runtime::execute`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L636) 在 Future 被驱动时同步构建、验证 closure 的 Definition，再执行；正式 [`Runtime::execute`](../src/core/runtime.rs#L323) 接收已完成 Orchestrator。新门面可以在内部形成完成定义，但不应要求用户恢复旧的 build／finish 使用方式。

## 5. Scope Import／Export 与调用权限

### 5.1 词法捕获确实转换为显式 Import

`Schema::ensure` 的算法是：

1. 拒绝外来 Definition 和不匹配的端口身份。
2. 同 Scope 的位置直接使用。
3. 跨 Scope 时只接受祖先位置。
4. 递归在每一层建立本地 alias 和 Import 关系。

随后 `Run::import` 把关系转换为 `ImportSlot`，调用原 `import_batch`。多层祖先捕获经过逐边界 Import，没有变成运行时全局读取。见 [Probe `Schema::ensure／Run::import`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L76)。

### 5.2 Export 已复用原有数据责任机制

`Run::finish` 调用原 `ScopeCoordinator::finalize`：

- child-owned 输出转移责任。
- imported 输出保留原 owner。
- 未保留的 child-owned 数据清理。
- CollectionItem 继续受 cap 限制。
- 不同逻辑位置指向同一完整 DataId 时，Export 可以只转移一次责任。

证据：[Probe `Run::finish`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L142)、[正式 `finalize`](../src/core/scope.rs#L2171)、[正式 Export 预检与责任去重](../src/core/scope.rs#L2450)。

消费者测试包含 [child／sibling／foreign Ref 拒绝及 Root alias 拒绝](../examples/SRFlow_Public_API_v21_Probe/tests/composition.rs#L131) 和 [item cap 内的 descendant 传递](../examples/SRFlow_Public_API_v21_Probe/tests/composition.rs#L274)。

### 5.3 尚未复用 Invocation 的操作权限

正式 Context 的 `require_call_scope` 限制当前 Invocation 可以操作哪些 Scope；Import 通常在 caller frame 中装配，进入 child 后不能任意访问 ancestor。Probe 的 `Run` 没有 Invocation 栈，只执行 Coordinator 的身份、关系和生命周期校验。见 [正式调用可见范围](../src/core/context.rs#L272)。

正式接入应保留以下职责顺序：

```text
caller 中建立、装配 child
    → 进入对应 Invocation
    → 执行 body
    → 由调用边界统一收口
    → 退出 frame
```

Probe Step 已持有 child 与 caller 输出映射，正式 `OrchSite` 也承担 Export。接入时必须确定唯一提交方，避免重复 `finalize`。见 [正式 `OrchSite::invoke`](../src/core/orchestrator.rs#L532) 和 [`export_orchestrator_outputs`](../src/core/orchestrator.rs#L758)。

**适用建议：**可以复用捕获分析与 Import／Export 的数据算法；调用装配和输出提交应进入正式受控边界，不把可变 Coordinator 直接交给新的普通编排执行面。

## 6. 成组 Consume／Promote

### 6.1 扩展的必要性与已保留的规则

正式单项 Consume／Promote 会关闭来源 Scope，不能对同一个 Item／Round 多次调用来实现多位置输出。

| 操作 | Probe 的实际扩展 | 保留的规则 |
| --- | --- | --- |
| `consume_item_group_probe` | 全部位置先调用 `prepare_consume`，拒绝重复 DataId；之后移动全部值，关闭 Item 一次 | 只消费 Item-owned 完整 Data；拒绝 ancestor／item 借用；collector 属于直接 parent |
| `promote_group_probe` | 全部位置先调用 `prepare_promote`，之后更新全部 state 和责任，关闭 Round 一次 | 不重绑父 RefId；imported owner 不变；允许状态 alias；旧状态进入 pending 回收 |

源码见 [Probe 两项成组扩展](../examples/SRFlow_Public_API_v21_Probe/src/core/scope.rs#L568)。预检复用了正式 [`prepare_promote`](../src/core/scope.rs#L3031) 和 [`prepare_consume`](../src/core/scope.rs#L3195)；状态回收仍由原 [`recycle_pending`](../src/core/scope.rs#L1492) 检查有效引用和控制状态保留。

三类别名边界必须分开：

- Consume 重复 DataId 必须拒绝，不能把一份值移动两次。
- Promote 多状态共享同一 target 可以合法，不因此产生重复 owner。
- Root 最终重复取走同一 DataId 仍须在任何 take 前拒绝。

### 6.2 正式变更不能止于复制 patch

当前正式路径的限制包括：

- `ItemConsumePermit` 只绑定一个 collector。
- `RoundCollectPermit` 只绑定一个 state 和登记包装的唯一输出。
- Context 的受控收口检查当前 frame、父 frame、owner 与选定位置。
- `EachSession／LoopSession` 的登记、执行和最终输出围绕单位置组织。

证据：[许可结构](../src/core/context.rs#L111)、[受控 Item／Round 收口](../src/core/context.rs#L693)、[Each／Loop 专用会话转交](../src/core/orchestrator.rs#L333)。

成组能力需要进入 **Coordinator、Context、许可和控制器会话**，同时维持“只提交本次登记输出到固定 parent 建构状态”的权限边界。不能通过放宽普通 ancestor 操作权限代替成组许可。

Probe patch 仍是验证实现：长度不一致使用断言，成组操作只返回一个 `ScopeError`，没有对应正式单项 `ConsumeOutcome／PromoteOutcome` 的原始拒绝与清理故障分离报告。后者见 [正式收口报告](../src/core/scope.rs#L302)。这部分不能直接视为完整正式故障协议。

Each 的 `()` body 输出也是正式控制器适配项：Probe 无 collector 时关闭 Item、最终无输出；当前正式 Each 围绕单 owned 输出与一个 `Vec<O>` 组织。见 [Probe `EachStep`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L552) 和 [正式 Each 的登记模型](../src/core/each.rs#L1)。

## 7. 错误与控制信号

### 7.1 现有全局终止协议不能直接承载可捕获 Control

Probe 区分 `BodyError::Control`、`BodyError::Failure`，以及内部 `Signal::Control`、`Signal::Terminal`。Retry／Iter 只捕获对应 Control；耗尽变成 Terminal 向外传播。见 [Probe 错误模型](../examples/SRFlow_Public_API_v21_Probe/src/error.rs#L69)。

当前正式 Core 则是：

- `BodyError` 保存静态说明和可选 ScopeError，没有 Control 分类和动态业务 source。
- leaf 与 Orchestrator 对执行体返回的 `Err` 调用 `guard.failed_with`。
- `failed_with` 记录不可恢复的首次终止。
- `require_running` 在终止后拒绝新的普通执行操作。

证据：[正式 `BodyError`](../src/core/context.rs#L1501)、[`Leaf1::invoke`](../src/core/node.rs#L344)、[`OrchSite::invoke`](../src/core/orchestrator.rs#L580)、[`InvocationGuard::failed_with`](../src/core/context.rs#L1293)、[`require_running／terminate`](../src/core/context.rs#L304)。

**把新 Retry 信号转换成现有 BodyError 后再由外层捕获，不能成立。** 到外层捕获时 Context 已终止，新的 attempt 会被门禁拒绝。清除 termination 也会破坏现有“首次失败不可恢复”的约束。

正式协议需要支持：控制信号退出当前路径，相关调用与 Scope 正确收拢，同时不把可捕获控制退出误记为整个 Execution 的失败或取消。具体类型布局尚未决定，但这项语义变更不可省略。

### 7.2 控制器行为差距

| 结构 | 冻结 SPEC／Probe | 当前正式控制器 | 适用建议 |
| --- | --- | --- | --- |
| Retry | 正常完成即成功；显式 Retry 重启完整 body；最多 N+1 次 | 读取正常 Output 的 `LoopControl` 决定继续／完成 | 保留原输入重用和丢弃机制，重接控制协议与预算 |
| Iter | 正常输出即 next；IterBreak 返回本轮输入 current；到限报错 | 读取输出字段决定 Continue／Finish，保留选定输出 | 保留 state／Promote 机制，改变停止与最终输出选择 |
| Choose | `PartialEq`；空配置构建失败；未匹配为独立错误 | Match 要求 `Eq`；允许空完成态，调用时失败 | 路由机制可复用，类型 bound、验证时机和错误分类须适配 |
| Chain／Each／Choose 的控制传播 | 原样向外传播 Control，停止当前路径后续执行 | 现有 child `Err` 按全局失败处理 | 调用边界必须参与新退出协议 |

Probe 源码：[RetryStep](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L457)、[IterStep](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L498)、[ChooseStep](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L600)。

正式源码：[LoopControl／LoopDecision](../src/core/loop_orchestrator.rs#L41)、[当前无预算参数的 run_loop](../src/core/loop_orchestrator.rs#L609)、[Match 空登记表完成规则](../src/core/match_orchestrator.rs#L203)、[Eq lookup 与未匹配错误](../src/core/match_orchestrator.rs#L513)。

现有测试源码覆盖最近同类捕获、异类穿越和内层耗尽不重试，见 [control.rs 的传播矩阵](../examples/SRFlow_Public_API_v21_Probe/tests/control.rs#L329)。还覆盖 [Break 返回 current 并丢弃已产生 next](../examples/SRFlow_Public_API_v21_Probe/tests/lifecycle.rs#L377) 和 [第二轮 Retry 清理已提升状态、从初始状态重启](../examples/SRFlow_Public_API_v21_Probe/tests/lifecycle.rs#L405)。这些是 Probe 路径的证据，尚未转换为真实 Invocation 整合证据。

最近对应控制器捕获应继续由控制器沿实际调用返回链实现，Context 提供共同退出机制。没有证据要求把 Retry／Iter 策略集中到 Context 的类型分派中。

### 7.3 与既有内部设计文档的关系

Public SPEC 已明确零输入 Root、Retry 预算与耗尽、Iter 上限与 Break。既有 Core／Runtime 文档中相应开放项和读取正常 Output 决定 Loop 推进的描述，不能代替新契约。

具体证据：[SPEC §10～§12](SRFlow_v2.1_Public_API_SPEC_v0.1.md#L252)、[Runtime Implementation Design §17](SRFlow_Core_Runtime_Implementation_Design_v0.1.md#L197)、[Core Design 的历史开放项](SRFlow_Core_Design_v0.1.md#L310)。后续设计应显式处理差异；本报告不自行修订这些文档，也不据此宣布整个既有 Core 语义失效。

## 8. 动态错误、清理与取消

Probe 的 `BodyFailure` 和 `RetryError` 保留动态错误对象，`RunError::source` 继续暴露 source chain。当前正式 `BodyError` 和公开 `RunError` 无法原样承载这些信息。正式适配不能只取 `note()` 或转成字符串，还需决定原始动态错误的所有权及 Root 移交。

证据：[Probe 动态错误及 source](../examples/SRFlow_Public_API_v21_Probe/src/error.rs#L21)、[正式 BodyError](../src/core/context.rs#L1501)、[正式公开 RunError](../src/api/mod.rs#L320)。

正式 Context 已有 InvocationGuard、首次终止定位和独立 cleanup diagnostic。Probe 的 `Run` 没有这些设施：

- `abort(...)?` 失败可能直接替代正在传播的原始 Signal。
- `ScopeError` 的本地 `Error` bridge 是空实现，没有补齐内部 source 链。
- Root Future 取消时，没有执行正式 guard 的取消报告路径。

证据：[Probe ScopeError bridge](../examples/SRFlow_Public_API_v21_Probe/src/error.rs#L1)、[ChainStep 的错误清理](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L436)、[正式 InvocationGuard::drop](../src/core/context.rs#L1317)。

Probe 有真实挂起与 drop 计数测试，覆盖跨 await 借用、取消 Root 和清理中间值。见 [lifecycle.rs](../examples/SRFlow_Public_API_v21_Probe/tests/lifecycle.rs#L292)。这些支持“所测场景下业务值寿命正确”，不能证明正式 frame 退出顺序、取消分类和清理故障诊断已经接入。

正式实现自身也并非所有路径都完整保留双诊断：`ScopeCoordinator::complete_exit` 在 Export 预检失败后使用 `cleanup_subtree(scope)?`，清理再失败时会替代原 Export 错误。见 [正式 Export 失败清理](../src/core/scope.rs#L2200)。因此不能笼统承诺“接回正式 Core 就自然获得完整诊断”。

**适用建议：**把可捕获控制退出、不可恢复失败、取消和清理故障一起审查，保留它们的区别；检查新路径和既有 Export 路径，不用单个清理错误覆盖正在传播的业务原因或控制原因。

## 9. Probe 专属选择、静态发现与不确定项

### 9.1 不应直接冻结的实现选择

| 项目 | 本次证据支持的结论 | 正式设计前的处理建议 |
| --- | --- | --- |
| `Rc<RefCell<Schema>>`、`Vec<Box<dyn Step>>`、Box Future | 是可运行的候选布局，SPEC 未冻结这些选择 | 可保留为候选，不据此增加或禁止抽象层 |
| `Run` 直接操作 Coordinator | 验证了数据底座兼容，没有验证 Invocation 权限 | 替换或改接正式受控调用路径 |
| derive 硬编码 Probe 包名 | 宏生成 `::srflow_public_api_v21_probe::Data` | 正式包布局和路径处理另行落实 |
| 公开 helper | `doc(hidden)` 隐藏文档，不限制调用 | 正式公开面须收窄 |
| 重复 child-local 输出位置 | Probe 定义阶段未统一拒绝；原 Export 会拒绝重复声明 RefId | 明确它与“不同 Ref 指向同一 DataId”的合法 alias 的区别 |
| 成组故障覆盖 | 正常路径和部分拒绝有证据；完整许可、错误及取消整合未验证 | 成组扩展不能单独授予 Core 合规结论 |

证据：[SPEC 的内部布局未冻结声明](SRFlow_v2.1_Public_API_SPEC_v0.1.md#L393)、[derive 生成路径](../examples/SRFlow_Public_API_v21_Probe/data-derive/src/lib.rs#L8)、[Export 对重复声明位置的拒绝](../src/core/scope.rs#L2424)。

### 9.2 公开 `RefShape::fresh` 的静态可达性发现

Probe 导出了 `RefShape`，其 `fresh` 是公开默认方法。使用者可以尝试调用 `<Ref<u32> as RefShape>::fresh(flow)`，分配一个没有生产步骤的位置。

`Schema::ensure` 只检查已分配端口和可见性，不检查该位置是否来自 Root Input 或已注册生产步骤；Root 的构建拒绝检查也没有这项检查。从源码路径看，此位置可以进入接线，随后在运行时解析未绑定位置或提取输出时失败，而非在 Definition 阶段统一拒绝。

证据：[helper 导出](../examples/SRFlow_Public_API_v21_Probe/src/lib.rs#L11)、[`RefShape::fresh`](../examples/SRFlow_Public_API_v21_Probe/src/shape.rs#L153)、[`Schema::ensure`](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L76)、[Root 构建检查](../examples/SRFlow_Public_API_v21_Probe/src/flow.rs#L636)。

**这是源码支持、尚未编译复跑的可达性发现。** 它足以说明当前 Probe 公开面不能直接作为 SPEC 所承诺的“无预造 Ref／前向接线入口”的正式边界；不在本报告中伪装成新增编译或执行反例已经通过。

### 9.3 测试断言范围的限制

| 样本 | 实际断言 | 不能外推的结论 |
| --- | --- | --- |
| `each_group_validates_all_outputs_before_consuming` | Runtime 错误及最终 Drop 数 | 测试没有观察移动次数；“移动前整组预检”的直接依据主要是实现顺序 |
| `ap07_sixteen_state_positions_share_one_control_boundary` | 已有 current 作为 next，第二次 Judge Break，结果与调用次数 | 不能外推为所有 16 位置 owned 替换及故障组合均已覆盖 |
| `tuple_states_sharing_one_next_target_never_duplicate_ownership` | 最终 Runtime 错误及 Drop 次数 | 没有单独断言错误发生在 Root 提取阶段 |

源码见 [成组消费测试](../examples/SRFlow_Public_API_v21_Probe/tests/lifecycle.rs#L242)、[16 状态测试](../examples/SRFlow_Public_API_v21_Probe/tests/large_shapes.rs#L108)、[共享状态目标测试](../examples/SRFlow_Public_API_v21_Probe/tests/lifecycle.rs#L456)。

### 9.4 尚未决定或尚未验证的事项

- Schema／Body 如何映射或替换正式 Definition、CallSite、Signature，尚未作实现决策。
- 借用 Node 的定义期寿命如何穿过真实 CallSite 和控制结构，尚未完成整合验证。
- Control 清理和正常 frame 退出如何衔接、如何避免被当作全局失败或 Pending 取消，尚未验证。
- 成组许可、组内位置关系、预检拒绝与清理故障并存的诊断，尚未形成正式路径。
- 重复同一 child-local Ref 的拒绝时机应与合法只读 alias 分开明确；SPEC 对 Root 直接重复 Ref 的拒绝最明确，不能把其他边界的细节偷偷当成已审定规则。
- 业务 panic、性能、并发、缓存、预编译及长期身份耗尽不由本次静态审查或 Probe 的历史 PASS 获得保证。其中多项明确不属于本版 Public 契约。

## 10. 设计决策前的适用建议

现有证据支持以下评审顺序，不表示本报告已经批准其中的具体实现：

1. **先审定调用退出语义。** 明确正常输出、传播／捕获中的 Control、全局 Failure、取消与清理故障如何经过真实 Invocation。这会影响 Node、全部控制结构和 Root。
2. **再审定成组收口边界。** 将输出组、collector／state、登记来源和调用许可一起处理，分别保持 Consume、Promote、Export、Root 提取的别名规则。
3. **然后决定 Definition 的接入方式。** 以 Probe 的逻辑 Scope／Import 模型对接正式 CallSite，同时处理 Copy Ref、Node 定义寿命、自然 Shape 和公开 helper 边界。
4. **用真实 Context 重新验证完整场景。** 现有 Probe 测试可作为行为验收素材；当前正式 Core 测试可作为既有不变量素材，二者都不能替代新调用链的整合证据。

这些建议保留现有数据责任机制，并把必须改变的执行协议与尚未决定的内部布局分开；不据此预先批准复制 Probe 或重写整套 Core。

## 11. 记录与后续维护边界

- 本报告的代码行号链接对应上述审查提交；代码变更后应同时核对路径、关键符号与实际行为，不能只依赖行号。
- SPEC、README、RESULTS 中部分相对链接仍指向 Probe 搬入仓库之前的目录。本报告使用当前仓库中的实际文件路径，不修改那些历史文档或把旧链接当作另一份实现证据。
- 将来若形成设计决策，应另行记录审定内容、与本报告的关系及验证证据；不要把“建议”“静态发现”“未验证”直接改写成正式合规结论。
- 本次文档记录仅新增本文件，没有修改源码、SPEC、上位 Core 设计、任务状态或 Gate，也没有提交或发布。
