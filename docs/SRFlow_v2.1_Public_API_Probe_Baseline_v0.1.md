# SRFlow v2.1 Public API 设计与 Probe 基线 v0.1

> 状态：**HISTORICAL DESIGN BASELINE — PROBE COMPLETED WITH REVISIONS**。
>
> 后续验证记录（2026-10-08）：已按用户授权在独立工程完成 AP01～AP08，结果为“修订后的 API 在明确支持范围内成立”。当前契约见 [Public API SPEC v0.1](SRFlow_v2.1_Public_API_SPEC_v0.1.md)，反例、修订和限制见 [Probe 结果](../../v3/SRFlow_Public_API_v21_Probe/RESULTS.md)。下文保留初次整理时的方案与待验证状态，**不是当前接口契约**；尤其 Struct Node 的原 Query 签名不能作为已通过写法。
>
> 整理日期：2026-10-08。讨论来源：[继续设计 SRFlow v2.1](chatgpt-conversation://6ac4bfd3-76c8-83ec-8a29-76028e92d752)。本文记录该讨论的最终有效结论、理想使用方式和待验证边界；不是正式 Public API SPEC，不构成 Rust 可编译性或实现合规结论。
>
> 本次仅新增本文，不修改 Rust 源码、现有 Core 规范、任务状态或 Gate。后续先开展 API Probe，再依据证据修订基线、冻结 SPEC、设计 Definition / IR，最后改造实现。

## 1. 文档说明

### 1.1 文档目标

记录已完成的 Public API 讨论，供后续 Rust API Probe 使用，避免 Probe 从当前 Rust 类型和实现 Shape 反向推导产品接口。本文应回答：使用者希望怎样组织 workflow；各边界必须产生什么可观察行为；哪些设计已选定；哪些准确 Rust 表达仍需证明。

本文是**本轮新 Public API Probe 的设计输入**。数据所有权、Scope 和生命周期仍以上位 Core 规范为依据；本文不是对三层设计的整体替代。正式 SPEC 只能收录经过相应验证、审定的契约。

### 1.2 状态用语

| 标记 | 含义 |
| --- | --- |
| 已确定设计 | 讨论已选定的语义或 API 方向，仍可能等待 Rust 可行性验证 |
| 暂定设计 | 已有首选表达，但具体签名、类型或边界尚未冻结 |
| 待 Probe 验证 | 尚无本轮 API 的对应证据，不能称为已可编译或已实现 |
| 开放问题 | 讨论没有定案；文档整理者和 Probe 执行者不得代为决定 |

“已确定设计”与“待 Probe 验证”可以同时成立：例如 Root 入口已经选定为 `Runtime::execute(closure, input)`，但其完整类型推导尚未验证。

本文全部 Rust 片段均为**目标 API 示例**。业务类型和 Node 名称是场景占位；省略的 trait 关联项、adapter 与泛型签名不是已完成实现。示例中的 `todo!()` 仅标明业务实现未展开，不应作为 Probe 的运行结果。

### 1.3 适用范围

本文覆盖 Public API Architecture、Workflow Definition、Node / Query 协议、Ref 与输入输出 Shape、Chain / Fragment、Each / Choose / Retry / Iter、错误传播、S01～S10 以及 AP01～AP08 验证目标。

不在本文重新设计 DataContainer、DataScope、ExecutionContext、Invocation、内部调度、Collector、Promote、Consume、存储布局或清理算法。本文也不引入日志、Trace、持久化、恢复、动态 Graph DSL、自动并发或隐式技术重试。

### 1.4 与现有 v2.1 Core 的关系

基础依据：

- [SRFlow Design v2.1](SRFlow_Design_v2.1.md)：整体架构与可观察语义。
- [Core Design v0.1](SRFlow_Core_Design_v0.1.md)：数据身份、只读借用、所有权和 Scope。
- [Core Runtime Implementation Design v0.1](SRFlow_Core_Runtime_Implementation_Design_v0.1.md)：内部责任交接与生命周期实现基线。
- [当前任务入口](tasks/README.md)：已有实现与验收范围；历史 PASS 不授予本轮 API 的通过结论。

本轮保留 Execution Core，重新打开 Definition / Orchestration 的 Public 使用模型。新的 capture 写法必须 lower 为明确的内部 Import；新的 Child Output 必须沿既有 Export / Consume / Promote 边界处理。

对照现有文档，需要明确以下迁移关系，不能静默择一：

| 事项 | 现有文档与本轮讨论的关系 | 本基线处理 |
| --- | --- | --- |
| `Flow` | Core 中是顺序 Orchestrator；本轮 Public `Flow` 是定义上下文 | 区分所在层，不把定义上下文当作运行时业务对象 |
| `DataRef<T>` / `Ref<T>` | 本轮采用短名 `Ref<T>` | 保留逻辑身份语义，不要求 Core 内部改名 |
| SubFlow 显式输入 / ancestor capture | Core 要求跨 Scope 明确导入；Public 允许直接引用合法 ancestor Ref | 由 Definition 生成显式 Import；不开放全局读权限 |
| `Match` / `choose` | Core 路由语义保留；Public 改用 `choose` | 不使用 `match_` 或 macro |
| `Loop` / `retry` / `iter` | Core 保留共同 Loop 生命周期机制 | Public 分成 Retry / Iter 两种协议 |
| Node 候选形式 | Core 曾列同步、异步候选；本轮只选 async Node | 本基线不测试同步 Node 路线 |
| Loop 错误表述 | 旧 Core 表述为 body 执行错误立即传播；本轮另设 `ControlSignal` 通道 | `Failure` 仍立即传播；只有显式控制信号由对应边界处理，后续 SPEC 须对齐表述 |
| Flow 执行顺序 | 总规范 §9 已规定定义顺序；S10 仍留有确认问题 | 保留已有顺序约束，并在 O01 记录讨论状态差异 |
| Each 多输出、Retry 次数、Iter 停止规则 | 较早总规范留为开放项，本轮已给出新目标 | 按本文验证；不把旧实现或旧 Gate 当作新目标的证据 |

本次不批量修订旧规范和使用示例。后续若 Probe 反例涉及 Core 不变量，应带具体证据回到评审，不能为实现便利修改其 ownership 或生命周期原则。

## 2. 设计背景与问题

### 2.1 SRFlow 的核心目标

SRFlow 降低 workflow 执行逻辑的组织、修改和验证成本。对于 SES 这种持续实验执行策略的系统，新增依赖、调整阶段边界、切换分支或迭代方式，应尽量停留在 Definition 层，而不要求重写业务 Node、包装业务数据或修改框架。

SES 是验证背景，不是 SRFlow 的构建依赖。能表达 SES workflow 也不等于该正文生成策略已通过故事质量实验。

### 2.2 当前实现暴露的问题

以下是本轮讨论对既有 Public 使用模型的诊断，不重新展开源码审计：

| 问题 | 表现 | 影响 |
| --- | --- | --- |
| Runtime Shape 泄漏 | `EachShared`、`Retry1/2`、`Iter1/2` | 修改依赖数量会改变能力类型 |
| Node Protocol 复杂 | `NodeCallN`、`NodeFut` | 使用者需要理解调用适配机制 |
| 控制结构构建复杂 | 独立 Builder、预先构造 body | 执行结构与使用代码不直接对应 |
| Workflow 组合复杂 | 先构造 SubFlow，再绑定输入 | 增加一套框架参数与绑定概念 |
| 业务数据适配框架 | 为控制器包装输入或实现控制 trait | 编排变更侵入业务结构 |
| 上层使用模型缺少独立验证 | Core 已完成，Public API 随实现 Shape 固化 | 底层通过不足以证明使用成本降低 |

### 2.3 本轮重构结论

> Execution Core 成功了，但 Orchestration Model 过早冻结成了具体 Rust 类型 Shape。

> 保留 Execution Core，重新打开 Definition / Orchestration 这一层设计。

这两项是讨论的设计判断，不表示全部旧接口已删除，也不表示新接口已经实现。现有代码是后续适配对象和可行性参考，不应决定目标 Public API 的形态。

### 2.4 设计方法与文档位置

```text
Public API 设计 → Scenario → 本文（待验证基线）
                            ↓
                       Rust API Probe
                            ↓
                     反例 → 修订设计
                            ↓
                 API 冻结 → 正式 Public API SPEC
                            ↓
                   Definition / IR → 实现改造
```

当前只新增 `docs/SRFlow_v2.1_Public_API_Probe_Baseline_v0.1.md`，保留原有文档目录。Probe 工程位置与任务书另行落实，不提前创建空目录或实现文件。

本地 `srflow/.gitignore` 当前包含 `docs/`，因此本文默认不被 Git 跟踪。本次不改变忽略规则，不进行提交、推送或发布。

## 3. Public API Architecture

### 3.1 三层关系

```text
Application：owned business input
    ↓
Runtime::execute(closure, input)
    ↓
Flow Definition Context
    ├─ then(Node, dependencies)
    ├─ chain(...)
    ├─ each(...)
    ├─ choose(...)
    ├─ retry(...)
    └─ iter(...)
    ↓ 同步构建、验证、lowering
Definition / IR
    ↓ 内部调用与显式 Scope 连接
Execution Core
    ├─ ExecutionContext / Invocation
    ├─ DataContainer / DataScope
    └─ Import / Export / Consume / Promote
    ↓ Root 成功输出边界
Application：owned business output
```

这张图表达职责，不预定 IR 数据结构、lowering 算法、具体 adapter 或 Core 改造方案。

### 3.2 Public 概念

| 概念 | 当前 Public 语义 |
| --- | --- |
| `Runtime` | 封装 Root Definition 构建、验证与一次 Root Execution |
| `Flow` | 同步 Definition 阶段的编排上下文 |
| `Ref<T>` | 某份逻辑业务数据的强类型 Definition 身份 |
| `Query<T>` | Node 本次 invocation 已绑定输入的只读借用视图 |
| `Node` | 异步业务执行叶子；结构体形式需要稳定执行入口 trait |
| `BodyError` | Node 向 workflow 报告控制信号或真正执行失败的通道 |
| `RunError` | Application 接收的构建失败或 Root 终止错误 |

InputShape、OutputShape、StateShape 是本文描述类型关系与边界的用语，不要求使用者显式构造内部 Shape 类型。

### 3.3 Definition 与 Execution 分离

Definition closure 同步执行，用 Ref 描述步骤、依赖和输出，不在其中直接 `await` 或读取 Ref 对应业务值。Each item、Choose branch、Retry attempt 和 Iter round 的 closure 定义相应 body；不因其运行时重复执行就要求用户重建 body。

Node 在 Execution 阶段异步执行，接收 Query 中的借用，返回新的 owned Data 或 `()`。每次 `Runtime::execute` 都构建本次 Definition；公开的缓存或预编译能力未设计，见 O05。

### 3.4 历史候选退出当前模型

`Flow::build()`、公开 `Workflow<I, O>`、`Workflow::call()` / `flow.call()` 和“先 Build 一个可复用 Workflow 对象”的方案已被后续讨论替代，不能作为本轮主 API 重新引入。

Root 使用 `Runtime::execute(closure, input)`；Child 使用 `chain` 或相应控制结构；复用普通 Rust Definition Fragment。内部仍需完整 Definition，但不因此公开 Workflow 类型。

## 4. Public API 设计原则

本章按最终结论整理 P01～P16。早期 P06 / P07 中的 reusable Workflow 与 `call` 已退出；早期 IterDecision 和 Public 统一 Loop 也不再有效。每条原则的“禁止方向”约束 Public 使用负担，不禁止内部存在必要适配机制。

### 4.1 P01 — 描述 workflow 语义

- **定义：** Public API 表达步骤、依赖、组合关系和可观察执行语义。
- **理由：** Execution Core 可替换的实现机制不应决定业务写法。
- **约束：** 类型安全、顺序、作用域和错误行为仍需明确，不能以简洁为由隐藏可观察语义。
- **禁止方向：** 要求业务操作内部 ID、容器、Scope bookkeeping、类型擦除或 Invocation。

### 4.2 P02 — 能力不按 arity 或 Runtime Shape 分类

- **定义：** 输入数量变化不改变 workflow 能力名称。
- **理由：** 新增一个依赖应是接线变化，不应变成框架类型迁移。
- **约束：** 使用同一 `then / each / choose / retry / iter`；内部 tuple 展开和 adapter 留在内部。
- **禁止方向：** Public `EachOnly / EachSharedN / RetryN / IterN / NodeCallN`。

### 4.3 P03 — Lexical Capture 与内部显式 Import

- **定义：** Child 可引用当前位置合法可见的 ancestor Ref；Definition 生成显式 Import。
- **理由：** 自然依赖写法与严格 Scope-local 访问可以同时成立。
- **约束：** 检查归属、定义先后和可见性；child-local 输出通过边界产生 parent Ref。
- **禁止方向：** 全局解析任意 Ref、sibling 直接互读、未来 Ref、child-local Ref 直接逃逸。

### 4.4 P04 — 统一 Async Node / Query 协议

- **定义：** 全部 Node 为 async，以单个 Query 获取声明输入的不可变借用。
- **理由：** 函数 Node 和结构体 Node 共享输入心智模型，协议不随输入数量膨胀。
- **约束：** 结构体实现 Node 执行入口，返回 `Result<OwnedOutput, BodyError>`；准确 trait 待 Probe。
- **禁止方向：** 同步 Node 路线、按值消费已有 Data、要求用户实现 arity adapter 或返回 `NodeFut`。

### 4.5 P05 — Ref 是 Definition Identity

- **定义：** `Ref<T>` 表示逻辑数据位置，不拥有或读取业务值；已选定 `Ref<T>: Copy`。
- **理由：** 同类型多实例必须按身份连接，Definition 与真实执行数据必须分离。
- **约束：** Copy 不复制 T、不增加 Data ownership，也不扩大可见性。
- **禁止方向：** `ref.get()`、暴露 DataId、按 TypeId 猜输入，或为 Copy 要求业务 `T: Copy`。

### 4.6 P06 — Flow 是 Definition Context，Runtime 建立 Root

- **定义：** Root 入口是 `Runtime::execute(closure, input)`；closure 中 Flow 负责定义编排。
- **理由：** 使用者不必管理“已 Build 但是否还能追加步骤”的公开对象。
- **约束：** 同步定义，异步执行；Root owned 数据由 Runtime 接收和返回。
- **禁止方向：** 把 `Flow::build()` 或 `Workflow<I, O>` 恢复为必需 Public 概念。

### 4.7 P07 — Lexical Closure 定义组合边界

- **定义：** Child workflow 用 closure 定义，实际边界由 Chain 或控制结构的语义确定。
- **理由：** 使用代码的嵌套结构能够直接表达 workflow 组合。
- **约束：** Node 与 Orchestrator 继续分工，closure 返回值明确声明输出。
- **禁止方向：** 为统一接口让 Flow 实现业务 Node，或重新引入 built Workflow 的 `call`。

### 4.8 P08 — Child Flow 具有统一编排能力

- **定义：** Root、Chain、Each item、Choose branch、Retry attempt 和 Iter round 共享编排能力。
- **理由：** 嵌套不应产生另一套 API。
- **约束：** 普通 child Flow 都可使用六类操作；控制结构只改变特殊输入、输出和控制语义。
- **禁止方向：** 为每类 body 暴露不同 Builder、限制其只能执行一个 Node。

### 4.9 P09 — Each 的 Item Scope 与按 Shape 聚合

- **定义：** `Ref<Vec<T>>` 提供局部 `Ref<T>`，逐项执行 body，分别聚合其输出位置。
- **理由：** Each 保持 mapping 与多个独立输出的边界语义。
- **约束：** 单输出变 `Ref<Vec<O>>`，tuple 输出分别变 Vec Ref，`()` 保持 `()`；合法 owned 输出由 Container 收集。
- **禁止方向：** Public shared-input 分类、隐式 Clone、把 item 借用移出原集合、制造 `Vec<()>`。

### 4.10 P10 — Choose 只执行一个 branch

- **定义：** 基于 selector value 匹配 case，支持可选 otherwise。
- **理由：** 路由判断由业务 Node 产生，控制器只选择执行路径。
- **约束：** 所有 branch 的 OutputShape 一致；无匹配且无 otherwise 时失败。
- **禁止方向：** macro、predicate case、隐式跳过或改走首分支、执行失败后改走 otherwise。

### 4.11 P11 — Public Retry / Iter 分离，Core Loop 共享

- **定义：** Public 提供 `retry` 与 `iter`，内部可使用共同 Loop 生命周期机制。
- **理由：** 两种推进语义不同，Core 共享不要求 Public 统一。
- **约束：** 分别记录 discard 与 state promotion 规则。
- **禁止方向：** 万能 Public Loop、让业务 Data 实现 `LoopControl`、用一套 Decision 表达全部策略。

### 4.12 P12 — Retry 由成功 / Retry 信号驱动

- **定义：** 正常完成即成功；最近 Retry 捕获显式 Retry 信号并重新执行整个 attempt。
- **理由：** Judge 可以只返回 `()` / 控制信号，候选数据无需为停止判断 Clone。
- **约束：** `max_retries=N` 最多 N+1 次；继续使用原始依赖；耗尽保留最后 RetryError。
- **禁止方向：** 所有 Err 自动重试、只重跑出错 Node、提升失败 attempt 状态或自动接受最后结果。

### 4.13 P13 — Iter 推进状态，以 IterBreak 结束

- **定义：** 正常 body Output 是 next state，表示继续；IterBreak 结束并返回 current state。
- **理由：** Judge 与状态生产可分开，终止无需携带 Container 中的业务状态。
- **约束：** `max_iterations=N>0` 最多 N 轮；未 Break 而耗尽形成终止错误。
- **禁止方向：** `IterDecision`、默认历史、自动接受未判断的最后 next state、通过错误搬出业务状态。

### 4.14 P14 — BodyError 区分 Control 与 Failure

- **定义：** `BodyError::Control(ControlSignal)` 与 `BodyError::Failure(BodyFailure)` 两层结构。
- **理由：** 控制请求不等于整个 Execution 失败，动态业务错误需要保留诊断链。
- **约束：** Control 只由最近对应边界捕获；Failure 立即传播；source chain 不退化为固定字符串。
- **禁止方向：** 吞掉未知信号、平铺混同全部错误、把 RuntimeError 交给 Node 返回。

### 4.15 P15 — RunError 是 Root 终止错误报告

- **定义：** Application 通过 RunError 区分 Definition、Body、控制耗尽、未处理 Control 和 Runtime 失败。
- **理由：** 单个 execute 入口仍需保留构建与执行阶段的诊断区别。
- **约束：** 被正常捕获的信号完成职责；耗尽错误不再转换为可捕获控制信号。
- **禁止方向：** 用 `Ref<RunError>` 驱动业务流程、通过错误返回未正式输出的业务 Data。

### 4.16 P16 — Chain 是 Inline Child Flow，复用 Fragment

- **定义：** `chain` 建立独立数据生命周期边界，普通 Rust 函数复用 Definition 逻辑。
- **理由：** 阶段结束后只保留声明输出，复用不需要公开 Workflow 对象。
- **约束：** 首选 `flow.chain(fragment(refs...))`；签名与备选表达由 AP05 比较。
- **禁止方向：** 只把 chain 当代码分组、恢复 `Workflow::call()`、提前冻结未经验证的 closure factory 签名。

## 5. Runtime / Flow / Ref

### 5.1 Runtime::execute

当前 Root baseline：

```rust
let output = runtime.execute(
    |flow, (a, b)| {
        let c = flow.then(&node1, (a, b));
        flow.then(&node2, c)
    },
    (value_a, value_b),
).await?;
```

第二个参数提供 owned business input 的类型信息；Definition closure 接收相应 RefShape；closure 返回值声明 Root Output；执行成功后 Runtime 返回对应 owned business values。

| Application Input | Definition 参数目标类型 | closure Output | Application 成功输出 |
| --- | --- | --- | --- |
| `A` | `Ref<A>` | `Ref<B>` | `B` |
| `(A, B)` | `(Ref<A>, Ref<B>)` | `Ref<C>` | `C` |
| `(A, B)` | `(Ref<A>, Ref<B>)` | `(Ref<C>, Ref<D>)` | `(C, D)` |
| `A` | `Ref<A>` | `()` | `()` |

单输入直接用 Ref，不强迫 `(Ref<A>,)`。`()` 作为 **Node Input** 已选定；`()` 作为 **Root Flow Input** 仍未审定，不能由零输入 Node 反向推出。本文不以零 Root Input 作为已确定正例。

Root 构建和验证失败通过 `RunError::Definition(BuildError)` 报告；能在 Definition 阶段判定的非法连接不得拖到 Node 已开始执行之后。实际执行只创建一个 ExecutionContext 和 DataContainer，child 调用继续使用这一执行域。

### 5.2 Flow 的职责与执行顺序

Flow Definition Context 提供：

```text
then(node, dependencies)
chain(child_definition)
each(collection, item_definition)
choose(selector, branch_definitions)
retry(max_retries, attempt_definition)
iter(initial_state, max_iterations, round_definition)
```

准确接收者形式、Flow 的内部泛型和 lifetime、closure trait bounds 均待 Probe。场景用 `flow / sub / branch / attempt / round` 表达同一套编排能力，不据变量名创造新 Public 类型。

**已有 Core 顺序约束继续适用：** 同一个 Flow 中，Step 按定义顺序执行，前一步完成后才开始后一步。没有数据依赖不意味着允许重排或并发。无输出 Node 仍是步骤；不能因其输出未被引用而消除其执行。发生 Failure 或需退出当前路径的 Control 后，不执行被退出路径的后续步骤。

S10 对这个约束的确认记录尚未闭环，见 O01；本文不由此授权自动 DAG 调度。

### 5.3 Ref：Copy、身份与可见性

已确定 `Ref<T>: Copy`，复制 Ref 只复制逻辑身份，不复制 T，也不要求 `T: Copy / Clone`。同类型不同 Ref 可表示不同实例；同一 Ref 可反复用于只读输入。

```rust
let diagnosis = flow.then(&diagnose, (draft, basis));
flow.then(&revise, (draft, diagnosis, boundary))
```

这里 draft 被两次引用，仍是同一份业务数据。Ref 不提供 `get / borrow / resolve` 或 DataId / ScopeId 操作，不能在 Definition closure 内按其运行时值做 Rust `if / match`。

合法使用位置必须位于该 Ref 的定义 Scope 或合法 descendant，并满足定义先于使用。以下连接必须拒绝：

- 从一个 Definition 捕获另一个无关 Definition 的 Ref。
- 直接读取 sibling Scope 的内部 Ref。
- 使用尚未产生的未来 Ref。
- 绕过 child Output 把 child-local Ref 留给 parent。
- 让 Each item 目标越过原集合和 ItemScope 的 lifetime cap。

Public `Ref<T>` 不要求编码 `ParentRef / ChildRef / ScopedRef`。不能由 Rust 类型静态拒绝的归属错误由 Definition validation 拒绝，并在执行任何 Node 前给出 BuildError。

### 5.4 InputShape / OutputShape

Node 接线的目标输入 Shape：

```text
0 输入：()
1 输入：Ref<A>
N 输入：(Ref<A>, Ref<B>, ...)
```

编排边界的目标 OutputShape：

```text
()
Ref<A>
(Ref<A>, Ref<B>)
(Ref<A>, Ref<B>, Ref<C>)
...
```

closure 返回值是显式输出声明；不是自动取最后一步。尾部有分号且无返回值表示 `()`，但此前注册的步骤照常执行。无输出不创建业务数据槽。

多个 Ref 的 tuple 表示多个独立数据位置。它不等于一个业务 tuple Data 的 `Ref<(A, B)>`，也不自动证明 Node 已支持一次产生多个独立输出。本基线的多输出场景通过多个 Node 产生不同 Ref，再在编排边界返回 tuple，不新增 Bundle 或 Node tuple 拆分机制。

### 5.5 Root Output 的所有权边界

Root 只能在成功结束、全部输出目标完成预检后，把 owned Data 移交 Application。多个输出若最终解析到同一 DataId，必须在任何 take 前拒绝，不能通过 Copy Ref 或多次 child Export 得到两份 owned 输出。

这项约束继承 Core Design §16。具体错误名称与能否提前发现 alias 待 Probe；失败不能产生部分正常 Root Output。

## 6. Node / Query

### 6.1 统一 Async Node

所有 Node，包括函数形式和结构体形式，都使用异步执行协议。正常输出为新的 owned Data 或 `()`，错误通道统一为 `BodyError`。Node 不是 Orchestrator，不接收 Definition Ref，不访问 Runtime，也不启动 SRFlow child。

结构体 Node 必须有稳定的执行入口 trait。本文选定 `Node` / `run` 使用心智，但不冻结关联类型、GAT、HRTB、adapter、RPITIT 或 boxed future 方案。

### 6.2 Function Node

```rust
async fn make_plan(
    query: Query<(&Prose, &Rules)>,
) -> Result<Plan, BodyError> {
    let (prose, rules) = query.get();
    // prose: &Prose；rules: &Rules。
    // 业务逻辑在这里产生 owned Plan。
    todo!("业务实现未展开")
}
```

目标调用写法：

```rust
let plan = flow.then(make_plan, (prose, rules));
```

函数直接传入与函数引用适配的准确支持形式由 AP02 验证；使用者不手工构造 `NodeCallN`。

### 6.3 Struct Node

```rust
struct GenerateNode {
    client: LlmClient,
}

impl Node for GenerateNode {
    // Node trait 必要关联项和准确签名留给 AP02；本段不是已可编译实现。
    async fn run(
        &self,
        query: Query<(&Plan, &Rules)>,
    ) -> Result<Candidate, BodyError> {
        let (plan, rules) = query.get();
        // 可以在本次 invocation 的合法借用期内跨 await 使用输入。
        todo!("业务实现未展开")
    }
}
```

使用者希望直接写 `flow.then(&generate_node, (plan, rules))`。固定配置或服务客户端可以是 Node 自身依赖；参与本次 workflow 数据流的业务输入仍应走 Root Input 或 Node Output。

### 6.4 Query 的输入对应关系

| Definition Input | Node 参数 | `query.get()` 的业务视图 |
| --- | --- | --- |
| `()` | `Query<()>` | `()` |
| `Ref<A>` | `Query<&A>` | `&A` |
| `(Ref<A>, Ref<B>)` | `Query<(&A, &B)>` | `(&A, &B)` |
| `(Ref<A>, Ref<B>, Ref<C>)` | `Query<(&A, &B, &C)>` | `(&A, &B, &C)` |

单输入不强迫 singleton tuple；零输入仍保留统一 Query 参数，不另建无参数协议。

首选 `Query<T>`，借用 lifetime 在 T 的引用内，由函数参数位置省略；使用者不被迫写 `Query<'_, (A, B)>`。这是明确的使用目标，准确 Rust 可行性待 AP02 证明，不能据此宣称 trait 已完成。

### 6.5 Query 的权限边界

Query 只提供本次 invocation 已绑定的输入借用。解包后 Node 面对普通 `&A / &B`，不能任意查询容器或发现其他数据。

不开放 `query.context()`、`query.container()`、`query.resolve(ref)`、`query.execute(...)`。业务借用不能逃逸 invocation，不能把借用输入伪装为新的 owned Output，也不能在 Future 仍借用时清理或移动来源 Data。

### 6.6 Output 与 Error

```rust
async fn inspect(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let candidate = query.get();
    todo!("产生新的 owned Report")
}

async fn send(query: Query<&Item>) -> Result<(), BodyError> {
    let item = query.get();
    todo!("完成动作或报告失败")
}

async fn load_config(query: Query<()>) -> Result<Config, BodyError> {
    let () = query.get();
    todo!("外部数据通过 Node Output 进入 Execution")
}
```

BodyFailure 保留动态错误对象和 source chain。`BodyError::fail(err)`、`.map_err(BodyError::fail)?` 是讨论中的便利目标；准确构造函数和转换 trait 暂定，不冻结宽泛 `From<E>`。

### 6.7 Node 生命周期与 Probe 边界

目标包括借用 `&Node` 和复用 `Arc<具体 Node>`。Arc 克隆只共享 Node 句柄，不 Clone 业务输入。`&self` 与 Query 借用需覆盖 Node Future 的实际使用期；Definition 闭包结束后，已注册 Node 必须仍合法可调用。

`Arc<dyn Node>`、公开 trait 的 object safety、Future / Data 的 Send + Sync bounds、Node / Flow 的准确 lifetime 均未冻结。AP02 应验证借用跨 await、函数 adapter、结构体 trait、零至多输入和 Arc 使用，不把旧 borrowed-node Probe 的结果直接迁移为新 Query 协议的 PASS。

## 7. Workflow Composition

### 7.1 Chain 的基本形式

```rust
let result = flow.chain(|sub| {
    let candidate = sub.then(&generate_node, (prepared, rules));
    sub.then(&check_node, candidate)
});
```

Chain 是真实 child Flow 的数据生命周期边界。执行时建立 child Scope，导入合法依赖，按顺序执行，正常输出经 Export 对接 parent；未保留中间数据在 child 退出时处置。

普通 Rust `{ ... }` 只分组代码，不自动建立这项边界。选择 chain 的理由是独立阶段与中间数据生命周期，而不是 Node 数量。

### 7.2 Ancestor Ref Capture

prepared 和 rules 在 ancestor Scope 已定义，child 使用它们时无需手工 import。Definition 识别实际 Ref 依赖并形成内部明确导入；此过程不意味着分析任意 Rust closure 的捕获内存，也不要求所有被 Rust closure 捕获的对象都成为 workflow Data。

多级 capture 必须穿过合法 Scope 连接，不能借唯一 DataContainer 绕过中间边界。导入目标仍由来源 Scope 负责，不能变成 child-owned Data。

### 7.3 Child Scope Output

```text
child-local Ref<Checked>
           ↓ Chain Output / Export
parent-visible Ref<Checked>
```

parent 得到的是边界输出的逻辑 Ref，不直接获得 child-local Ref 的使用权限。Export 可能传递同一业务目标及其责任，不意味着复制 Checked。

Chain 支持 `()`、单 Ref 和多个 Ref 的 tuple。导出 child-owned Data 时，输出绑定与生命周期责任转移遵循 Core 的原子边界；再输出 imported Data 不创造第二份 ownership。

### 7.4 Fragment 复用

首选使用目标：

```rust
let result = flow.chain(generate_fragment(prepared, rules));
```

讨论中的 factory 轮廓如下，**返回签名刻意未定，是 AP05 的验证对象**：

```rust
// 目标签名示意：不是已可编译的 Rust 函数定义。
fn generate_fragment(
    prepared: Ref<Prepared>,
    rules: Ref<Rules>,
) -> impl /* chain closure，准确 FnOnce / lifetime 签名待 Probe */ {
    move |sub| {
        let candidate = sub.then(&generate_node, (prepared, rules));
        sub.then(&check_node, candidate)
    }
}
```

复用的是 Definition 逻辑。Ref 参数按值复制即可，Node 依赖如何传入或借用、捕获 lifetime 如何表达，也须纳入 AP05，不让示意中的 Node 名称变成隐式业务数据通道。

备选是普通函数接收 Flow context：

```rust
let result = flow.chain(|sub| {
    generate_fragment(sub, prepared, rules)
});
```

其目标签名轮廓为 `generate_fragment(flow: &mut Flow, prepared: Ref<Prepared>, rules: Ref<Rules>) -> Ref<Checked>`；准确 Flow lifetime 与泛型同样未定。AP05 比较两个形式，不能因 factory 难写就自动改成公开 Workflow 对象。

### 7.5 Nested Composition

Chain 内可嵌套 Chain、Each、Choose、Retry 和 Iter。所有 child 使用相同编排能力，局部特殊 Ref 只改变该结构输入含义；父级通过结构输出使用结果，不直接读取 child 内部步骤。

S08 验证多级捕获和输出；S09 验证穿越这些结构的动态控制信号。词法数据可见性与动态信号捕获必须分别成立。

### 7.6 生命周期边界

child 和 descendant 的调用与借用结束后，按 Core 规则校验输出、交接责任、清理剩余 owned Data 并退出。错误或 Control 导致退出时，同样完成适用清理；parent 不能先于存活 child finalization。

Chain 清理不销毁 ancestor-owned 输入，也不撤销 Node 已发生的外部副作用。Probe 不得用隐式 Clone、第二个 Container 或在 Orchestrator 之间 take 业务值来模拟 child 组合。

## 8. Control Structures

### 8.1 Each

baseline：

```rust
let scores = flow.each(items, |item, each| {
    each.then(&score_node, (item, rules))
});
```

items 为 `Ref<Vec<Item>>`；item 是局部 `Ref<Item>`；rules 是普通 ancestor Ref；scores 为 parent `Ref<Vec<Score>>`。

| Item body Output | Each Output |
| --- | --- |
| `Ref<O>` | `Ref<Vec<O>>` |
| `(Ref<A>, Ref<B>)` | `(Ref<Vec<A>>, Ref<Vec<B>>)` |
| `()` | `()` |

多输出按位置分别收集，不自动包装为 `Ref<Vec<(A, B)>>`。若业务本来需要一个 tuple / struct 结果，那应由业务 Node 明确产生该 Data，不混淆两种 Shape。

本轮沿用 Core 的 `Vec<T>`、逐项顺序及空集合行为：空集合不调用 body，有输出时产生对应空 Vec，无输出时返回 `()`。失败或向外传播的 Control 停止后续 item，不返回部分正常集合。

item 是原集合元素的受限借用，不分配独立 DataId，不隐式 Clone 或移走元素。collector 仅消费 item 调用链中新产生且责任可合法转移的 owned 输出；返回 item 本身或 ancestor Ref 不能借收集器移动 imported Data。

S08 曾质疑 Vec 聚合与 Container ownership 的兼容性，随后用户明确：**Container 已具有重新组装 Vec 的能力**。本基线不重新打开 Container 设计；AP06 / AP08 仍需证明新 API 调用了合法 Consume 路径，尤其是多输出的逐位置聚合及别名拒绝。

并发策略与其取消行为保持开放 O03；不得改变当前默认顺序。

### 8.2 Choose

```rust
let result = flow.choose(route, |choice| {
    choice.case(Route::A, |branch| {
        branch.then(&node_a, (data, rules))
    });
    choice.case(Route::B, |branch| {
        branch.then(&node_b, (data, rules))
    });
    choice.otherwise(|branch| {
        branch.then(&fallback_node, (data, rules))
    });
});
```

route 是 `Ref<K>`，case 使用值匹配。只有选中 branch 在运行时执行；未选 branch 不创建执行 Scope，不产生 Data 或副作用。case 和 otherwise 的 OutputShape 必须相同，包括 `()` 与多 Ref 输出。

otherwise 可选，不强制静态穷举。未命中且无 otherwise 时形成明确 `NoMatchingCase` 执行错误，不能静默返回 `()` 或改用首分支。选中 branch 的失败直接传播，不改走 otherwise。

selector 比较所需的精确 Rust bounds、重复 case / 重复 otherwise 的诊断、`NoMatchingCase` 在 RunError 中的准确归类尚未定案。第一轮不设计 predicate case、branch macro 或独立 Public MatchBuilder。

### 8.3 Retry

```rust
let candidate = flow.retry(3, |attempt| {
    let candidate = attempt.then(&generate_node, (prompt, rules));
    attempt.then(&judge_node, (candidate, rules));
    candidate
});
```

`max_retries=N` 表示首次 attempt 外最多再执行 N 次，总数最多 N+1。N=0 仍执行首次 attempt；成功正常输出，否则 Retry 信号导致耗尽。

| attempt 的执行结果 | Retry 行为 |
| --- | --- |
| 正常完成 | 导出声明 OutputShape，结束 Retry |
| `ControlSignal::Retry` | 中止整个当前 attempt，清理后按预算重试 |
| `ControlSignal::IterBreak` | 不捕获，向外传播 |
| `BodyFailure` / Runtime 失败 / child 控制器耗尽 | 不转成 Retry 信号，终止并传播 |

Retry 没有特殊输入 Ref。每轮使用合法捕获的原始 ancestor dependencies，失败 attempt 不做 state promotion，也不默认保留历史。信号可能来自 body 任意深度，重执行的单位仍是完整 attempt。

预算耗尽形成 `RetryExhausted`，诊断保留 max_retries、实际 attempts 和最后 RetryError；字段布局待 Probe。失败的最后 Candidate 不作为正常结果，也不通过错误搬出 Container。

只捕获显式 Retry 控制信号。普通 HTTP / 文件 / 模型失败不自动成为技术重试；S09 中的服务检查若发出 Retry，属于场景明确选择的控制请求。

### 8.4 Iter

```rust
let final_draft = flow.iter(draft, 5, |current, round| {
    round.then(&judge_node, (current, rules));
    round.then(&revise_node, (current, rules))
});
```

第一轮 current 对应 initial state；正常 body Output 是同一 StateShape 的 next state。next 经 Promote 成为下一轮 current，其他 round-local 数据结束生命周期。Imported 初始状态仍由原 owner 负责，不因状态替换而被销毁；Loop 自己负责的旧状态按既有责任规则处置。同一目标作为 next 时不得重复拥有或先销毁再使用。

| round 的执行结果 | Iter 行为 |
| --- | --- |
| 正常完成并产生 next state | Continue；有剩余轮次则进入下一轮 |
| `ControlSignal::IterBreak` | 最近 Iter 捕获，返回本轮输入 current |
| `ControlSignal::Retry` | 不捕获，向外传播 |
| `BodyFailure` / Runtime 失败 / child 控制器耗尽 | 停止并传播 |

`max_iterations=N` 计入第一轮，要求 N>0。零值配置不得作为有效 Iter；准确配置诊断待 AP07。第 N 轮正常完成却未 Break 时，产生 `IterationLimitReached`，不强制成功、不输出最后 next state。

若工作流先 Judge current 再 Revise，最后产生的 next 尚未被下一轮 Judge 判断；上限限制的是 round 次数，不保证最后一次修改已获判断。该取舍已在 S08 后确认，不应被实现改成“接受最后状态”。

IterBreak 可来自 round 任意深度，但不携带某个 round-local next 作为最终输出。推荐先判断 current 再产生 next。单 `Ref<State>` 为当前场景基线；tuple StateShape 是已提出的扩展方向，准确支持待 AP07，不产生 `Iter1/2` 类型。无状态 `()` Iter 未作为已定能力。

## 9. Error / Control Propagation

### 9.1 BodyError 两层模型

概念结构已确定，准确数据布局、bounds 与构造函数未冻结：

```rust
pub enum BodyError {
    Control(ControlSignal),
    Failure(BodyFailure),
}

pub enum ControlSignal {
    Retry(RetryError),
    IterBreak(IterBreak),
}
```

`Err(BodyError)` 是 Node 没有按正常 Output 路径完成的通道；其中 Control 不一定意味着整个 Execution 失败。普通业务判断可以返回正常 Data；只有明确的控制 adapter 发出信号时才有控制含义。

BodyFailure 必须保留动态错误及 source chain。RetryError 允许动态原因，耗尽时保留最后一次原因。IterBreak 只表达停止对应 Iter，不承担业务输出；轻量 payload 及便利构造方法暂定。

### 9.2 Root RunError

```text
RunError
├─ Definition(BuildError)
├─ Body(BodyFailure)
├─ RetryExhausted
├─ IterationLimitReached
├─ UnhandledControl(ControlSignal)
└─ Runtime(RuntimeError)
```

这是一组已讨论的终止类别，不是宣称最终 Rust enum 已穷尽。`NoMatchingCase` 必须有明确执行诊断，但它归入哪一最终 variant 尚未指定；不得自行新增或混入 BodyFailure 来填补文档形式。

Node 只报告 BodyError，不主动产生 RuntimeError，也不以 RunError 作为 workflow Data 或控制输入。错误诊断不能借携带业务 state 绕过 Root 成功输出边界。

### 9.3 最近对应边界捕获

控制信号沿动态调用链向上传播，寻找最近**对应类型**边界：

```text
Retry     → 最近 enclosing Retry
IterBreak → 最近 enclosing Iter
```

Chain、Choose、Each 不解释或吞掉这些信号；Retry 不捕获 IterBreak，Iter 不捕获 Retry。相同类型嵌套时，内层先处理，不能越过它指定外层。

已捕获信号完成职责，正常控制结果向父级继续。内层 RetryExhausted / IterationLimitReached 是终止错误，不再转回信号，因此外层 Retry 不会自动重新处理内层耗尽。

### 9.4 跨越 Each 与不同控制器

Each item 中的 IterBreak 若对应最近外层 Iter，会停止当前 Each 的后续 item并结束那个 Iter；它不是“只跳过当前 item”。Each 当前没有独立 break / skip 控制协议。

Iter 内的 Retry 信号可继续到外层 Retry。外层 Retry 重新执行完整 attempt，内层 Iter 从该 attempt 的原始 initial state 重新开始，不恢复上一 attempt 的中间 state。

Retry 内的 IterBreak 同样可传播到外层 Iter，退出当前 attempt及其中间数据后，由该 Iter 返回自己的 current。

### 9.5 普通 Failure、未处理 Control 与耗尽

| 情况 | 最终行为 |
| --- | --- |
| Node `BodyError::Failure` | 停止路径，最终 `RunError::Body`，保留错误链 |
| Retry 信号耗尽预算 | `RetryExhausted`，保留最后 RetryError |
| Iter 正常 Continue 耗尽轮次 | `IterationLimitReached`，无正常 state 输出 |
| Control 传播至 Root 而无接收者 | `RunError::UnhandledControl` |
| 内部数据 / Scope / 调用不变量失败 | Runtime 类终止诊断 |

不自动换分支、跳过步骤、强制成功或返回部分正常结果。

### 9.6 Scope 清理

```text
Node 发出 Retry
    ↓
Chain 退出
    ↓
Iter Round 退出
    ↓
Iter 退出
    ↓
当前 Retry Attempt 清理
    ↓
最近 Retry 按预算启动下一 Attempt
```

传播中所有被退出作用域及其 descendant 必须在相关借用结束后清理；不能跳到捕获者而遗漏中间责任。下一 attempt 不继承失败 attempt 的 owned 数据，仍可借用有效 ancestor 数据。

这些是语义关系，不要求每个逻辑结构都采用新 Scope 类型，也不要求每穿越一层复制业务数据。外部副作用不在 Scope 清理的回滚保证中。

### 9.7 BuildError 与执行错误

```text
Runtime::execute
    ├─ 同步 Definition 构建与验证
    │    └─ 失败：RunError::Definition(BuildError)，不执行 Node
    └─ 异步 Execution
         └─ Body / Control / Runtime / 耗尽结果
```

类型、Shape 或可见性错误若能在 Rust 编译期拒绝，优先提供编译失败证据；归属、身份等无法静态判断但在 Definition 已知的错误应作为构建失败。数据目标有效性和运行时别名等仍由 Core 校验，不能承诺全部在编译期解决。

## 10. API Scenario Suite

本章把 S01～S10 统一更新为最终 Root 入口。所有示例假定 runtime、业务类型、Node 与 owned 输入值由使用方准备，不展开 Runtime 初始化或业务实现。各场景的同名 Node 是场景局部占位，不表示一个 Node 可以随意改变签名；S10 的 A / B 则明确复用其共同业务 Node。

| 场景 | 名称 | 当前状态 |
| --- | --- | --- |
| S01 | Root Linear Flow | 方向已确定；已更新为 Runtime::execute，待 Probe |
| S02 | Node / Query | 输入模型已选定，准确协议待 Probe |
| S03 | Chain / Inline SubFlow | Inline 语义已定；Fragment 表达暂定 |
| S04 | Each | 场景已确认，待 Probe |
| S05 | Choose | 场景已确认，待 Probe |
| S06 | Retry | 场景已确认，待 Probe |
| S07 | Iter | 场景已确认；tuple StateShape 待 Probe |
| S08 | Nested Composition | 场景设计完成，待组合验证 |
| S09 | Control Propagation | 用户接受该方案，待 Probe 验证 |
| S10 | Real SES Workflow | 阶段性保留，O01 / O02 未关闭 |

### 10.1 S01 — Root Linear Flow

**业务目标。** 以正文与规则生成 Plan，再生成 Candidate，验证最基础的数据连接及 Root owned 输入输出。

**理想 Public API。**

```rust
let candidate = runtime.execute(
    |flow, (prose, rules)| {
        let plan = flow.then(&plan_node, (prose, rules));
        flow.then(&generate_node, (plan, rules))
    },
    (prose_value, rules_value),
).await?;
```

多输出边界补例：

```rust
let (candidate, report) = runtime.execute(
    |flow, (prose, rules)| {
        let plan = flow.then(&plan_node, (prose, rules));
        let candidate = flow.then(&generate_node, (plan, rules));
        let report = flow.then(&report_node, candidate);
        (candidate, report)
    },
    (prose_value, rules_value),
).await?;
```

**预期类型关系。** Root `(Prose, Rules)` 映射为 `(Ref<Prose>, Ref<Rules>)`；plan 为 `Ref<Plan>`；candidate 为 `Ref<Candidate>`；Application 得到 owned Candidate。补例返回两个不同 Data 的 `(Candidate, Report)`。

**预期执行语义。** closure 同步定义，Plan Node 完成后才执行 Generate Node。Rules 重复使用不被消费。输出由 closure 明确声明。

**数据生命周期语义。** Root Input 进入唯一 Container；Node 输入只借用；成功时完整 Output 经 Root 预检和提取，未输出中间 Data 在执行结束时清理。

**Probe 内容。** AP01 验证从 owned input 推导 RefShape、Node 类型、输出类型及 Node 借用期；AP03 验证多输出、同类型不同实例、`()` 输出与重复目标拒绝。

**当前状态。** 旧 `Flow::build → Workflow → execute` 方案已替换。新写法已选定，未有本轮可编译性证据。

### 10.2 S02 — Node / Query

**业务目标。** 函数与结构体 Node 使用同一 Query 输入模型，覆盖零输入、单输入、多输入和无输出。

**理想 Public API。** 以下函数体只标记业务工作位置；Probe 运行时需替换为最小可观察业务实现。

```rust
async fn make_plan(
    query: Query<(&Prose, &Rules)>,
) -> Result<Plan, BodyError> {
    let (prose, rules) = query.get();
    todo!("产生 owned Plan")
}

struct GenerateNode {
    client: LlmClient,
}

impl Node for GenerateNode {
    // 准确 trait 关联项待 AP02。
    async fn run(
        &self,
        query: Query<(&Plan, &Rules)>,
    ) -> Result<Candidate, BodyError> {
        let (plan, rules) = query.get();
        todo!("异步产生 owned Candidate")
    }
}

async fn inspect(
    query: Query<&Candidate>,
) -> Result<Report, BodyError> {
    let candidate = query.get();
    todo!("单输入产生 owned Report")
}

async fn load_config(query: Query<()>) -> Result<Config, BodyError> {
    let () = query.get();
    todo!("零输入产生 owned Config")
}

async fn send(query: Query<&Report>) -> Result<(), BodyError> {
    let report = query.get();
    todo!("执行动作，无业务输出")
}

let report = runtime.execute(
    |flow, (prose, rules)| {
        let plan = flow.then(make_plan, (prose, rules));
        let candidate = flow.then(&generate_node, (plan, rules));
        let report = flow.then(inspect, candidate);
        flow.then(send, report);
        report
    },
    (prose_value, rules_value),
).await?;
```

零输入 Node 的连接另用 `flow.then(load_config, ())`，不要求零输入 Root。

**预期类型关系。** 多输入 Query 解包为只读引用 tuple；单输入直接解包为 `&Candidate`；零输入为 `()`。Report 仍是 owned 输出，send 不产生 `Ref<()>`。

**预期执行语义。** Node Future 实际执行时才借用；`&self` 与 Query 来源需覆盖跨 await 使用。send 在 Root 返回 Report 前执行，其输出未被引用不影响步骤存在。

**数据生命周期语义。** Node 不能转移输入 ownership、持有 Definition Ref 或借用 Container 中的未声明 Data；调用结束后借用不能逃逸。

**Probe 内容。** AP02 验证 trait、函数 adapter、省略 lifetime 的 Query、跨 await、`&Node` 和 Arc；AP03 验证自然 arity Shape 和不要求业务 Clone / Copy。

**当前状态。** Query / async / 单输入自然写法已选定，准确 trait 及函数适配暂定。示意 `impl Node` 不构成已合法 Rust 签名结论。

### 10.3 S03 — Chain / Inline SubFlow

**业务目标。** 候选生成与检查作为一个局部阶段，只保留 Checked，并复用其 Definition 逻辑。

**理想 Public API。**

```rust
let checked = runtime.execute(
    |flow, (input, rules)| {
        let prepared = flow.then(&prepare_node, input);
        flow.chain(|sub| {
            let candidate = sub.then(&generate_node, (prepared, rules));
            sub.then(&check_node, candidate)
        })
    },
    (input_value, rules_value),
).await?;
```

首选 Fragment 调用：

```rust
let checked = runtime.execute(
    |flow, (input, rules)| {
        let prepared = flow.then(&prepare_node, input);
        flow.chain(generate_fragment(prepared, rules))
    },
    (input_value, rules_value),
).await?;
```

factory 与接收 Flow context 的备选轮廓见 §7.4。

**预期类型关系。** prepared 为 ancestor `Ref<Prepared>`；candidate 与 checked 为 child-local Ref；chain 返回 parent `Ref<Checked>`；Application 得到 owned Checked。

**预期执行语义。** child 顺序执行，自动识别 prepared / rules 依赖并明确导入。Fragment 是普通 Rust 函数定义逻辑，不需要 `Workflow::call`。

**数据生命周期语义。** child 正常退出只 Export Checked；Candidate 等未输出 owned 中间数据结束生命周期；ancestor Prepared / Rules 仍由原 Scope 负责。

**Probe 内容。** AP04 验证真实 child 边界、capture 和非法逃逸；AP05 比较 factory 与 context 函数；另覆盖 `()` / tuple 输出及多级 chain。

**当前状态。** Inline Chain 语义已选定，Fragment 语法暂定，未验证 FnOnce / HRTB / lifetime 表达。

### 10.4 S04 — Each

**业务目标。** 对 Vec<Item> 逐项评分，必要时同时生成报告或只执行动作。

**理想 Public API。**

```rust
let scores = runtime.execute(
    |flow, (items, rules)| {
        flow.each(items, |item, each| {
            each.then(&score_node, (item, rules))
        })
    },
    (items_value, rules_value),
).await?;
```

多输出补例：

```rust
let (scores, reports) = runtime.execute(
    |flow, (items, rules)| {
        flow.each(items, |item, each| {
            let score = each.then(&score_node, (item, rules));
            let report = each.then(&report_node, (item, score));
            (score, report)
        })
    },
    (items_value, rules_value),
).await?;
```

无输出补例：

```rust
runtime.execute(
    |flow, items| {
        flow.each(items, |item, each| {
            each.then(&send_node, item);
        });
    },
    items_value,
).await?;
```

**预期类型关系。** `Ref<Vec<Item>> → Ref<Item> → Ref<Score> → Ref<Vec<Score>>`。多输出为 `(Ref<Vec<Score>>, Ref<Vec<Report>>)`，Application 得到两个 Vec；动作例得到 `()`。

**预期执行语义。** 按 item 顺序完成并收集，空集合不调用 body。祖先 Rules 无需 shared-input 包装。每个 body 具有完整 Flow 能力。

**数据生命周期语义。** Item 是集合元素借用。Score / Report 的合法 owned 输出在 ItemScope 有效期内由 Container Consume 进 collector；旧独立目标随移动失效。item 自身、ancestor 数据以及重复 owned alias 不得被错误收集。

**Probe 内容。** AP06 验证每个位置分别聚合、空 Vec / `()`、顺序、错误中止及非法消费；AP08 验证 item Ref 向 descendant 传递但不得越过 lifetime cap。

**当前状态。** 用户已确认单、多、无输出模型。Container 重组能力已明确保留，新 API 接入仍待验证。

### 10.5 S05 — Choose

**业务目标。** 根据业务路由选择不同处理流程，保持共同输出类型。

**理想 Public API。**

```rust
let result = runtime.execute(
    |flow, (route, data, rules)| {
        flow.choose(route, |choice| {
            choice.case(Route::A, |branch| {
                branch.then(&node_a, (data, rules))
            });
            choice.case(Route::B, |branch| {
                let prepared = branch.then(&prepare_b, data);
                branch.then(&node_b, (prepared, rules))
            });
            choice.otherwise(|branch| {
                branch.then(&fallback_node, (data, rules))
            });
        })
    },
    (route_value, data_value, rules_value),
).await?;
```

**预期类型关系。** route 为 `Ref<Route>`；所有 branch 输出 `Ref<ResultData>`；choose 返回 parent `Ref<ResultData>`，Runtime 返回 owned ResultData。

**预期执行语义。** 值匹配，只执行选中 branch。移除 otherwise 后未命中产生 NoMatchingCase；选中 branch 失败不会执行 fallback。Definition 中注册未选分支，不表示其 Node 会执行。

**数据生命周期语义。** 仅选中 BranchScope 进入执行，导入 data / rules，输出责任交接后清理其局部数据；未选分支无执行 Data 或副作用。

**Probe 内容。** AP06 验证 case 类型推导、otherwise、共同 OutputShape、未命中诊断、branch 失败，以及 tuple / `()` 输出。

**当前状态。** 用户已确认普通 Rust API、值匹配、可选 otherwise 和单分支语义。比较 bounds、重复注册及错误归类待验证或审定。

### 10.6 S06 — Retry

**业务目标。** 生成候选后判断；被明确拒绝时重做完整候选生成过程，成功即输出候选。

**理想 Public API。**

```rust
let candidate = runtime.execute(
    |flow, (prompt, rules)| {
        flow.retry(3, |attempt| {
            let candidate = attempt.then(&generate_node, (prompt, rules));
            attempt.then(&judge_node, (candidate, rules));
            candidate
        })
    },
    (prompt_value, rules_value),
).await?;
```

Judge 的控制协议目标示例（构造函数细节暂定）：

```rust
async fn judge(
    query: Query<(&Candidate, &Rules)>,
) -> Result<(), BodyError> {
    let (candidate, rules) = query.get();
    if acceptable(candidate, rules) {
        Ok(())
    } else {
        Err(BodyError::Control(ControlSignal::Retry(
            RetryError::new("candidate rejected"),
        )))
    }
}
```

**预期类型关系。** prompt / rules 为 ancestor Ref；candidate 是 attempt-local `Ref<Candidate>`；成功 Retry 输出对应 parent Ref；Judge 返回 `()` 或 BodyError，不按值返回 Candidate。

**预期执行语义。** `max_retries=3` 最多四次。Judge Ok 即正常完成；Retry 信号中止整个 attempt；Failure / IterBreak 不由 Retry 捕获。单、多、无输出均沿 OutputShape。

**数据生命周期语义。** 失败候选及 attempt-local 中间数据清理，下次使用原始 prompt / rules；成功 Candidate 经输出边界保留。无状态提升和默认历史，外部副作用不自动回滚。

**Probe 内容。** AP07 覆盖首次成功、重试后成功、N=0、耗尽及最后原因保留、非 Retry 信号传播、后续步骤中止和复杂 body；AP08 检查同类嵌套。

**当前状态。** 用户已确认 max_retries 定义及成功 / Retry 驱动模型。准确错误 payload 与表达待 Probe。

### 10.7 S07 — Iter

**业务目标。** 判断当前 Draft，不合格时产生 next Draft；合格时结束并保留当前状态。

**理想 Public API。**

```rust
let final_draft = runtime.execute(
    |flow, (draft, rules)| {
        flow.iter(draft, 5, |current, round| {
            round.then(&judge_node, (current, rules));
            round.then(&revise_node, (current, rules))
        })
    },
    (draft_value, rules_value),
).await?;
```

Judge 的控制协议目标示例（构造函数细节暂定）：

```rust
async fn judge(
    query: Query<(&Draft, &Rules)>,
) -> Result<(), BodyError> {
    let (draft, rules) = query.get();
    if good_enough(draft, rules) {
        Err(BodyError::Control(ControlSignal::IterBreak(
            IterBreak::new(),
        )))
    } else {
        Ok(())
    }
}
```

**预期类型关系。** initial / current / next / final 都对应 Draft；current 是当前 round 局部 `Ref<Draft>`；round 正常输出同类型 next。tuple StateShape 可另做 AP07 暂定正例，不据此冻结完整状态泛化。

**预期执行语义。** 第一轮 Judge Draft0；Ok 后 Revise 得 Draft1 并继续。IterBreak 输出被捕获时的 current，后续 Revise 不执行。五轮均正常完成则 IterationLimitReached，不输出 Draft5。Retry 信号继续传播。

**数据生命周期语义。** next 经合法 Promote 保留，其他局部分析数据清理；Imported Draft0 仍由原 owner 负责。Loop-owned 旧状态在不再使用时处置；same-target 替换不能误删。

**Probe 内容。** AP07 验证首轮 Break、后续 Break、轮次上限、零值配置拒绝、current 输出、状态类型一致和 tuple 方向；AP08 验证深层 Break 与责任清理。

**当前状态。** 用户已接受无 IterDecision、正常继续 / Break 结束及耗尽报错模型。最后 next 没有额外 Judge 的行为已确认。

### 10.8 S08 — Nested Composition

**业务目标。** 在正文迭代中逐项检查质量，根据路由选择快速或深度评分，深度评分允许显式 Retry，最后汇总评分修订正文。

**理想 Public API。**

```rust
let final_draft = runtime.execute(
    |flow, (draft, rules, checks)| {
        flow.chain(|pipeline| {
            pipeline.iter(draft, 5, |current, round| {
                round.then(&stop_if_good, (current, rules));

                let scores = round.each(checks, |check, item_flow| {
                    let prepared = item_flow.chain(|sub| {
                        sub.then(&prepare_check, (check, current))
                    });

                    let route = item_flow.then(
                        &route_check,
                        (prepared, rules),
                    );

                    item_flow.choose(route, |choice| {
                        choice.case(Route::Fast, |branch| {
                            branch.then(&fast_score, (prepared, current))
                        });

                        choice.case(Route::Deep, |branch| {
                            branch.retry(2, |attempt| {
                                let score = attempt.then(
                                    &deep_score,
                                    (prepared, current, rules),
                                );
                                attempt.then(&validate_score, score);
                                score
                            })
                        });

                        choice.otherwise(|branch| {
                            branch.then(&fallback_score, (prepared, current))
                        });
                    })
                });

                round.then(&revise, (current, scores, rules))
            })
        })
    },
    (draft_value, rules_value, checks_value),
).await?;
```

**预期类型关系。** checks 为 Root `Ref<Vec<Check>>`；check 为局部 `Ref<Check>`；current 为 Iter Round `Ref<Draft>`；prepared 是 item 阶段的 `Ref<PreparedCheck>`；所有评分 branch 返回 `Ref<Score>`；scores 为 `Ref<Vec<Score>>`；revise 产生下一 Draft。

**预期执行语义。** stop_if_good 优先于检查；Break 时跳过 Each 和 Revise。Deep branch 内验证发出 Retry 只重做最近完整 attempt，不重做全部 Each / Iter。RetryExhausted 向外终止整个 Execution。

**数据生命周期语义。** current / rules 的多级引用必须形成合法 Import；Check item cap 向 descendant 传递。Deep Score 经 Retry、Choose 和 item 输出边界保留，再由 Each Consume 进集合；不是每跨边界 Clone 一次。Iter Promote next 时清理本轮评分和未保留中间 Data。

**Probe 内容。** AP08 验证六类操作混合、真实 Container 接入、多级 Import / Export、Collect / Promote、局部重试、耗尽传播和深层清理。

**当前状态。** 场景设计完成，待 Probe。讨论曾提出的聚合疑问已由用户指明 Container 能力解决；本轮验证的是接入，不把它描述为待新增 Core 能力。最后 next 未被 Judge 的边界按 S07 保留。

### 10.9 S09 — Control Propagation / Error

**业务目标。** 检查嵌套控制器的最近捕获规则、普通失败、耗尽、未处理信号、构建失败和中途数据清理。

**理想 Public API。**

```rust
let final_draft = runtime.execute(
    |flow, (draft, rules)| {
        flow.retry(2, |attempt| {
            attempt.iter(draft, 5, |current, round| {
                round.chain(|sub| {
                    sub.then(&check_service, current);
                    sub.then(&stop_if_good, (current, rules));
                });
                round.then(&revise, (current, rules))
            })
        })
    },
    (draft_value, rules_value),
).await?;
```

check_service 可以明确发出 Retry 控制请求；stop_if_good 可发出 IterBreak；revise 正常产生 Draft。check_service 的普通 Failure 不自动转成 Retry。

同类 Retry 嵌套补例：

```rust
let result = runtime.execute(
    |flow, input| {
        flow.retry(3, |outer| {
            outer.retry(2, |inner| {
                inner.then(&node, input)
            })
        })
    },
    input_value,
).await;
```

没有对应边界的补例：

```rust
let result = runtime.execute(
    |flow, input| {
        flow.then(&judge, input)
    },
    input_value,
).await;
```

judge 若发出 Retry / IterBreak，result 必须是对应 UnhandledControl。

**预期类型关系。** 正常成功输出 Draft；Failure、耗尽、未处理 Control 或构建错误通过不同 RunError 类别返回，不产生部分 Draft。Check 与 Stop 控制 Node 正常输出 `()`。

**预期执行语义。** 主例至少覆盖四条路径：

| 路径 | 执行关系 | 结果 |
| --- | --- | --- |
| A：正常 Break | Round 1 产生 Draft1，Round 2 Stop 发出 Break | Iter 返回 Draft1；Retry 正常完成 |
| B：Iter 内 Retry | Round 2 服务检查发出 Retry，Chain / Round / Iter / Attempt 退出 | 下个 Attempt 从原始 Draft0 重启整个 Iter |
| C：普通 Failure | revise 返回 BodyError::Failure | Iter / Retry 都不捕获，最终 RunError::Body |
| D：Retry 耗尽 | max_retries=2，三次 Attempt 都发出 Retry | RetryExhausted，attempts=3，保留最后原因 |

补充传播矩阵：

| 信号 / 错误位置 | 预期行为 |
| --- | --- |
| Chain 中 Retry | 穿过 Chain，到最近 Retry |
| Choose branch 中 IterBreak | 穿过 Choose，到最近 Iter |
| Each item 中 IterBreak | 停止后续 item，结束最近外层 Iter |
| Retry 内嵌 Retry | 内层捕获 Retry；内层耗尽不被外层重新重试 |
| Iter 内嵌 Iter | 内层捕获 Break；内层正常结果仅成为外层 body 数据，不终止外层 |
| Iter 内 Retry | Iter 不捕获，继续寻找 Retry |
| Retry 内 IterBreak | Retry 不捕获，继续寻找 Iter |
| 任意深度普通 Failure | 停止路径，保留 source chain 向外传播 |
| 无对应边界的 Control | UnhandledControl，不能归成普通 BodyFailure |
| 任意层控制器耗尽 | 终止错误，不再变成控制信号 |
| Definition 非法 Ref | 构建失败；没有 Node 执行 |

**数据生命周期语义。** 信号寻找捕获者时，经过且被退出的 descendant Scope 正常履行清理责任。失败 Attempt 的所有局部状态结束生命周期，原始 Draft 与 Rules 仍由 ancestor 负责。无论失败还是 Break，都不得留下有效 Ref 指向已销毁或移走的 Data。

**Probe 内容。** AP08 逐条覆盖矩阵，观察执行顺序、调用次数、后续步骤未执行、局部数据销毁和 ancestor 数据存活；AP04 / AP01 覆盖构建失败路径。需要同类与异类嵌套的独立证据，不能只验证简单成功例。

**当前状态。** 用户明确接受 S09 方案，要求后续 Probe 验证。本基线不宣布 Gate 通过。

### 10.10 S10 — Real SES Workflow

**业务目标。** 对比单次诊断修改与多轮策略修改，验证调整执行方案能否只改 Definition，而不修改共同业务 Node 和业务数据结构。

四类 Root 输入为 CharacterState、BackgroundFacts、ExpressionTask、WritingBoundary。下列 Node 是代表性实验能力，不是已经冻结或通过质量验证的 SES 正文生产方案：

| Node | 示例职责 |
| --- | --- |
| make_basis | 根据本次输入生成正文判断依据 |
| make_plan | 规划表达 |
| write_prose | 生成正文初稿 |
| diagnose | 根据依据诊断正文 |
| revise | 进行一次修改 |
| choose_revision | 输出业务修改策略 |
| reduce_prose / supplement_prose / rewrite_prose | 三类示例修改动作 |
| stop_if_acceptable | 根据判断结果返回 `()` 或 IterBreak 的轻量控制 adapter |

**理想 Public API：方案 A，单次诊断修改。**

```rust
let prose = runtime.execute(
    |flow, (characters, facts, task, boundary)| {
        let basis = flow.then(
            &make_basis,
            (characters, facts, task, boundary),
        );

        let draft = flow.chain(|writing| {
            let plan = writing.then(
                &make_plan,
                (characters, facts, task),
            );
            writing.then(
                &write_prose,
                (plan, characters, facts, boundary),
            )
        });

        let diagnosis = flow.then(&diagnose, (draft, basis));
        flow.then(&revise, (draft, diagnosis, boundary))
    },
    (characters_value, facts_value, task_value, boundary_value),
).await?;
```

**理想 Public API：方案 B，迭代与策略选择。**

```rust
let prose = runtime.execute(
    |flow, (characters, facts, task, boundary)| {
        let basis = flow.then(
            &make_basis,
            (characters, facts, task, boundary),
        );

        let draft = flow.chain(|writing| {
            let plan = writing.then(
                &make_plan,
                (characters, facts, task),
            );
            writing.then(
                &write_prose,
                (plan, characters, facts, boundary),
            )
        });

        flow.iter(draft, 3, |current, round| {
            let diagnosis = round.then(&diagnose, (current, basis));
            round.then(&stop_if_acceptable, diagnosis);
            let action = round.then(&choose_revision, diagnosis);

            round.choose(action, |choice| {
                choice.case(RevisionAction::Reduce, |branch| {
                    branch.then(
                        &reduce_prose,
                        (current, diagnosis, boundary),
                    )
                });
                choice.case(RevisionAction::Supplement, |branch| {
                    branch.then(
                        &supplement_prose,
                        (current, diagnosis, task, boundary),
                    )
                });
                choice.case(RevisionAction::Rewrite, |branch| {
                    branch.then(
                        &rewrite_prose,
                        (current, diagnosis, characters, facts),
                    )
                });
            })
        })
    },
    (characters_value, facts_value, task_value, boundary_value),
).await?;
```

**预期类型关系。** Root 四个业务输入分别映射为 Ref；basis、plan、draft、diagnosis、action 是普通业务 Data 的 Ref；RevisionAction 是业务枚举，不是 SRFlow 控制类型。write_prose、revise 及三个策略 branch 的正文输出须与 Iter StateShape 一致，准确业务签名留给 Probe。两方案中的 make_basis / make_plan / write_prose / diagnose 使用同一输入输出契约。

**预期执行语义。** A 顺序生成依据、初稿、诊断、修改。B 复用相同生成与诊断 Node，把修改阶段改为 Iter + Choose。basis 位于 Iter 外，只生成一次；若实验希望每轮重建依据，只应改变 Definition 中的位置。

在 B 中，stop_if_acceptable 必须先于 choose_revision：它发出 Break 后，后者与所有修改 branch 不执行。二者只共享 Diagnosis，没有直接彼此依赖，因此只测类型推导不能证明该顺序成立。

**数据生命周期语义。** writing Chain 只输出正文，Plan 在阶段结束后清理。Basis 和 Root 输入跨轮只读复用；每轮 Diagnosis 和修改中间 Data 随 round 结束清理，next 正文合法 Promote。IterBreak 输出 current 正文，不能返回刚产生但尚未正式成为 current 的局部数据。

**Probe 内容。** AP01 / AP02 / AP04 / AP06 / AP07 / AP08 联合验证下列验收项，使用最小业务替身即可，不把真实模型或 SES 质量实验列为 API Probe 的前置依赖。

| 验收项 | 需要证明的行为 |
| --- | --- |
| API 自然性 | 四输入流程可以直接由 then / chain / choose / iter 表达 |
| Node 可复用 | A / B 的共同 Node 签名与实现不变 |
| 数据结构独立 | 不为 Iter / Choose 添加框架专用输入包装 |
| Scope 语义 | 多级 capture、阶段输出、状态提升与清理合法 |
| 修改成本 | 切换方案只修改 Definition 与所选已有业务能力 |
| 执行确定性 | 无直接依赖的 Stop 与 Choose 仍遵循定义顺序，Break 中止后续 |

S10 不要求为了覆盖框架能力加入 Each 或 Retry；这些已由其他场景独立验证。

**当前状态。** 用户同意 S10 先保留至此，尚未完全冻结：

- **O01：** S10 的顺序确认记录待闭环，但 Core 已有严格顺序约束。
- **O02：** stop_if_acceptable 等控制 Node 依赖 Iter 语义，在普通 Flow 中使用可能产生 UnhandledControl。讨论建议把纯业务 diagnose 与轻量控制 adapter 分开；是否还需其他隔离方式未定。

本轮接受的 BodyError 控制协议使部分 adapter 知道 Retry / Iter，不能据此宣称所有 Node 都与执行控制完全无关。也不能把两套实验 workflow 的可表达性当作 SES 正文质量验证通过。

## 11. Probe 设计与验收要求

### 11.1 Probe 定位

Probe 证明目标 Public 写法在 stable、safe Rust 中能否自然表达，以及是否保持已确定语义。它不是正式实现任务、完整 IR 设计或业务质量实验。

本次没有运行 AP Probe，没有新增 Probe 工程或测试脚本。下面的分组说明**需要证明什么**；详细任务书、工程位置、实现候选和执行授权另行落实。

验证按独立使用者视角观察：Public API 不要求用户构造内部适配类型；输入输出保持业务类型；新增依赖不引起 arity 能力迁移；不能用 unsafe、隐式业务 Clone、提前消费输入或另建 Container 来掩盖困难。

### 11.2 八组 Probe

| Probe | 验证目标 | 关联场景 | 当前状态 |
| --- | --- | --- | --- |
| AP01 | Runtime::execute 的 closure 推导、Root 边界与生命周期 | S01、S10 | 未执行 |
| AP02 | Async Function / Struct Node、Query、借用与 Arc | S02、S06、S07 | 未执行 |
| AP03 | Ref Copy、自然 InputShape / OutputShape、身份与别名 | S01～S07 | 未执行 |
| AP04 | Chain、合法 capture、child 输出与边界 | S03、S08、S10 | 未执行 |
| AP05 | 普通 Fragment 函数复用，factory / context 两种表达 | S03 | 未执行 |
| AP06 | Each / Choose 的类型关系、聚合及路由 | S04、S05、S08 | 未执行 |
| AP07 | Retry / Iter Public 协议、次数、状态和控制 | S06、S07、S10 | 未执行 |
| AP08 | Nested Composition、Control Propagation 与清理 | S08、S09、S10 | 未执行 |

### 11.3 AP01 — Runtime::execute

需要证明：

- 第二参数的 owned Input 能推导 closure 中 RefShape；单、二、四输入至少覆盖 S01 / S10 的实际写法。
- closure 不需要公开 Workflow 对象或手写 Runtime Shape，单 / tuple / `()` Output 能推导到 owned 输出。
- 同步构建 closure 与后续 Node Future 的生命周期能够分开，`&Node` 在整个实际调用期合法。
- Definition 失败从同一 execute 入口报告，且任何 Node 都未开始执行。

负例应覆盖与 Node 输入不匹配的类型、错误 OutputShape 或无关 Definition Ref。构建时可判定的错误必须在构建阶段拒绝；不能让类型推导“成功”依赖运行时猜测业务类型。

`()` Root Input 如需探索，只作为未审定选项记录，不写成必需正例。

### 11.4 AP02 — Node / Query

需要证明：

- 普通 async function 与结构体 Node 共用 `then`，Node 用户只书写 Query 与业务返回类型。
- `Query<()> / Query<&A> / Query<(&A, &B)>` 及 S10 所需更多输入保持自然写法；单输入无需 tuple 包装。
- 用户可以省略 invocation lifetime，借用可跨实际 await，Future 完成或取消前来源数据不被移动或销毁。
- `&Node` 与 Arc<具体 Node> 的合法共享，且不要求业务 Data Clone / Copy。
- `()` 不产生业务槽，BodyFailure 保留动态 source chain，控制信号与普通 Failure 区分。

准确 trait、函数 adapter、GAT / HRTB、Future bounds 和必要用户注解必须记录。若不能达到目标签名，给出最小反例及替代表达成本，不能直接把更繁琐的内部签名宣布为最终 Public 协议。

负例包括借用逃逸、按值消费已有 Data、输入类型错位和 Node 试图接触内部编排对象。

### 11.5 AP03 — Ref / Shape

需要证明：

- `Ref<T>: Copy` 不附加 `T: Copy / Clone`，复制后仍是同一逻辑依赖。
- 同类型不同实例按 Ref 身份区分，重复只读输入可合法使用。
- 自然零 / 单 / 多 Node Input，以及编排 `() / 单 Ref / tuple Ref` Output。
- 多独立 Ref 与单业务 tuple Data 不混淆；不借多输出能力新增 Bundle。
- 重复 Root 输出目标在任意 take 前被拒绝，child 再输出形成的 alias 不能绕过预检。

负例应覆盖位置错配、多个 Ref 最终落到同一 DataId 的 Root 输出，以及把 Copy Ref 当作复制业务输出。具体错误形式可以是构建或运行时诊断，但要与其实际可判定阶段一致。

### 11.6 AP04 — Chain / Capture

需要证明：

- Chain 是真实生命周期边界，child 只保留声明输出；支持单、tuple、`()`。
- 合法祖先依赖可多级直接引用，lowering 形成内部显式 Import，来源责任不变。
- child Output 在 parent 建立新的可见 Ref，子作用域内部身份不能直接逃逸。
- 非法 sibling、无关 Definition、未来引用或 child-local 泄漏在 Node 执行前拒绝。

能够用 Rust 编译失败表达的负例保留编译诊断；不能靠类型拒绝的身份 / 归属负例保留 Definition 拒绝证据。若 Public Ref 未编码 Scope，不得因此放弃动态构建验证。

S10 的 Plan 清理而 Draft 保留，是 child 边界的业务可观察实例；仅把代码写在一个 Rust block 中不足以通过。

### 11.7 AP05 — Fragment Function

优先验证 `flow.chain(fragment(a, b))`。需给出可复核的普通函数返回签名、FnOnce / lifetime 表达、Node 依赖传递和多个调用点复用。

同时比较接收 Flow context 的备选 `flow.chain(|sub| fragment(sub, a, b))`：

| 比较项 | 应记录的信息 |
| --- | --- |
| 调用方写法 | 是否需要额外类型注解、特殊框架对象或手工 adapter |
| Fragment 作者负担 | 返回 closure、Flow context 和 Node lifetime 如何表达 |
| 嵌套 | 每次调用是否建立预期 child 边界，ancestor capture 是否合法 |
| 复用 | 同一 Definition 逻辑在多个合法 Scope / Root 中可重复使用 |
| 语义保持 | 业务输入只经 Ref 依赖，输出经 child 边界，无新 Root |

两种形式都是待验证候选。factory 若失败，记录反例后评审取舍；不能未经审定恢复 `Workflow::call()`。

### 11.8 AP06 — Each / Choose

Each 需要证明局部 item Ref、祖先 capture、Vec 输入、单 / 多 / `()` 聚合、空集合、顺序和错误中止。接入既有 Container Consume，不能用隐式 Clone 或移动 imported Data 达成 Vec 结果。

Choose 需要证明 selector / case 推导、可选 otherwise、只执行一个 branch、共同 OutputShape、无匹配错误和 branch 失败不转 fallback。

必要负例：

- Each 输出 item 本身、ancestor Data 或重复 owned alias 时不能非法 Consume。
- Item 目标经 descendant 输出试图突破 lifetime cap 时拒绝。
- Choose 各分支 Shape / 类型不一致时拒绝。
- 无 otherwise 的未匹配值报告明确 NoMatchingCase。

selector bounds、重复注册诊断与 NoMatchingCase 的最终错误类别若需提出选择，明确标为候选，不把实现默认行为写为设计结论。

### 11.9 AP07 — Retry / Iter

| 结构 | 必须覆盖的行为 |
| --- | --- |
| Retry | 首次成功、重试后成功、N=0、N+1 次耗尽、最后 RetryError 保留 |
| Retry | 任意深度信号中止完整 attempt；重试使用原始输入，无 state promotion |
| Retry | Failure / IterBreak / child 耗尽不当作 Retry；支持单 / tuple / `()` Output |
| Iter | 首轮 / 后续 Break 返回 current，后续步骤不执行 |
| Iter | 正常完成 Promote next、N>0、零值配置拒绝、N 轮未 Break 耗尽 |
| Iter | Imported 初始状态存活、Loop-owned 旧状态清理、same-target 替换安全 |
| Iter | Retry / Failure 不当作 Continue 或 Break；StateShape 不一致拒绝 |
| 暂定方向 | tuple StateShape 的准确表达和可行性，不据探索结果擅自扩大冻结范围 |

失败不返回最后业务状态。控制信号 helper 和错误 payload 的 Rust 表达可由 Probe 提案，但必须保留两层错误分类和动态诊断信息。

### 11.10 AP08 — Nested / Control Propagation

至少运行 S08 的完整组合、S09 的传播矩阵及 S10 的顺序中止路径。证明最近同类型捕获、穿越异类型边界、耗尽不重新变信号、Each 深层 Break 终止最近 Iter，以及 Root UnhandledControl。

生命周期观察应能区别：当前路径中途生成的数据、已合法 Export / Promote / Consume 的数据、Imported ancestor 数据。验证失败退出没有部分正常输出、stale target 或误销毁；新 attempt 之前已完成退出路径的适用清理。

Core 已有的失败和取消保证不能因新 API 接入减弱。当前只验证顺序调用链的 Future / Scope 退出，不借此增加并发 Each 或恢复协议。

### 11.11 优先级与通过标准

**优先组为 AP01、AP02、AP04、AP05。** 这些覆盖 closure 推导、函数适配、trait lifetime、HRTB / FnOnce、嵌套上下文和 Fragment，是本轮最可能影响 Public 写法的 Rust 边界。

AP03 与基础验证相互关联；AP06 / AP07 验证控制结构；AP08 在前面具有可复核结果后检查组合。该顺序是验证组织建议，不在本文创建或自动开始详细工程任务。

必须分别记录三类证据：

| 证据层 | 能证明什么 | 不能据此宣称什么 |
| --- | --- | --- |
| Rust 类型与编译证据 | 目标写法、推导、trait 与 lifetime 成立，负例被拒绝 | 真实 Scope、Consume / Promote 已正确接入 |
| 最小执行语义证据 | 顺序、选择、次数、信号行为符合目标 | 替身存储等于正式 Core ownership |
| 既有 Core 的最小接入证据 | 在真实数据责任机制下行为保持 | 全部正式 IR / 新 Public 实现已经完成 |

每组结果至少说明对应目标示例、必要注解、正反例结果、实际覆盖范围、依赖的 Core 能力、未验证项和候选修订。必要时用最小反例说明不可行；不为了得到 PASS 静默修改场景或 Core。

历史 Compile Probe 和 V21 / G21 结果只能作为原范围参考，不授予 AP01～AP08 PASS。Probe 通过也不自动关闭正式交付 Gate 或授权源代码重构。

## 12. 设计状态与待决问题

### 12.1 已确定事项

下列“确定”指本轮讨论已选定方向，仍须对应 Probe 证据：

- Root 使用 `Runtime::execute(closure, input)`；Flow 是 Definition Context。
- Definition closure 同步，Node 全部 async；Root owned Input / Output 与内部借用 / 目标传递分离。
- `Ref<T>: Copy`，不拥有业务 Data；Query 是 invocation 输入借用视图。
- 零 / 单 / 多 Node 输入对应 `Query<()> / Query<&A> / Query<(&A, &B, ...)>`；单输入不强迫 tuple。
- `chain` 是 Inline Child Flow 的真实数据生命周期边界。
- 合法 ancestor capture 由 Definition 转为显式内部 Import；child-local Ref 不直接逃逸。
- child Flow 共用六类编排能力；复用 Definition Fragment，退出公开 Workflow / build / call 模型。
- Each 使用 Vec 与局部 item Ref；多输出按位置分别聚合，`()` 保持 `()`，Container 负责内部重组。
- Choose 使用普通 Rust API、值匹配、单 branch、可选 otherwise 和共同 OutputShape；无匹配明确失败。
- Public Retry / Iter 分离，Core 可共享 Loop；不使用 IterDecision 或 arity 能力类型。
- `max_retries=N` 最多 N+1 attempts；普通 Failure 不自动重试。
- Iter 正常完成继续，Break 返回 current；N>0，耗尽报错，不接受未判断的最后 next。
- BodyError 采用 Control / Failure 两层；最近对应边界捕获；耗尽是终止错误。
- Root RunError 区分构建和执行，动态失败保留诊断链；错误不能作为业务状态出口。

Core 已确定的顺序、唯一 Container、只读输入、责任原子交接、item cap、Root alias 预检等仍是约束，不以本轮 API 讨论重新开放。

### 12.2 暂定事项与待 Probe 项

| 项目 | 当前首选 / 目标 | 未冻结部分 |
| --- | --- | --- |
| Query | 用户书写 `Query<(&A, &B)>`，省略 invocation lifetime | 精确类型表示与 trait 适配 |
| Struct Node | `impl Node`、`async fn run(&self, query)` | 必要关联项、Future bounds、GAT / HRTB 等 |
| Function Node | 普通 async function 可直接交给 then | adapter 推导、函数引用支持与注解成本 |
| Node 复用 | `&Node`、Arc<具体 Node> | 注册 lifetime、精确传参形式；dyn Node 不承诺 |
| Shape | 自然单值 / tuple / `()` | 支持范围、更多位置的推导、alias 诊断 |
| Fragment | 返回 closure 的普通函数首选 | FnOnce / lifetime / Node 依赖签名；context 函数备选 |
| Iter StateShape | 单 Ref 基线，tuple 方向已提出 | tuple 类型与边界验证；`()` state 未确认 |
| 错误便利接口 | fail / retry / iter_break 等轻量构造目标 | 精确名字、payload、bounds、From 转换 |

### 12.3 讨论保留的开放问题

**O01 — Flow 执行顺序的 S10 确认记录。**

讨论在 S10 提出“定义顺序是否必须就是执行顺序”，倾向严格顺序，但没有正式关闭。本地总规范 §9 与 Core Design §12 已明确规定该顺序。因此本文保留**讨论确认待闭环**的状态，同时执行既有 Core 约束。后续需验证新 Definition / IR 不因纯依赖图而重排 `()` / Control gate；不能把未关闭讨论解读为自动调度授权。

**O02 — Control Node 与业务 Node 的耦合。**

BodyError 控制协议已选定；S10 仍暴露 stop_if_acceptable 对 Iter 的依赖。首选讨论方向是纯业务 diagnose 输出 Diagnosis，轻量 adapter 发出 `()` / IterBreak。是否需要进一步隔离、由什么额外能力承担尚未定案；本文不新增控制 trait、判断 DSL 或独立隔离层。

**O03 — Each 执行策略。**

当前沿用 Core 严格顺序和既有错误 / 取消清理。未来显式并发、并发下的取消与输出策略开放；这不改变当前默认行为，也不进入本轮实现范围。

**O04 — Fragment 的 Rust 表达方式。**

首选 closure factory，普通函数接收 Flow context 是备选。AP05 需要比较使用负担与生命周期证据，再审定最终形式。

**O05 — Definition 构建时机与开销。**

当前每次 execute 构建本次 Definition。缓存、预编译或公开复用对象是否有真实收益，需要成本证据后再讨论；不预先添加 Public Workflow，也不宣称构建开销可以忽略。

### 12.4 文档核对中保留的其他边界

以下是现有 Core 开放项或讨论未给出精确选择的细节，不作为新设计决定：

| 边界 | 当前处理 |
| --- | --- |
| `()` Root Flow Input | 未审定；零输入 Node 不推出零输入 Root |
| Choose selector bounds / 重复 case / 重复 otherwise | 准确契约待 Probe 提案与审定，不从候选实现默认继承 |
| NoMatchingCase | 必须是明确执行失败；最终 RunError variant 归属未指定 |
| async Send / Sync、dyn Node | 当前示例不冻结精确约束或动态 trait 支持 |
| Node 多独立输出 / Bundle | 本基线不新增；编排 tuple 输出不能当作其支持证据 |
| Ref / Output alias 的诊断时点 | 能提前识别则提前拒绝；Root 在任意 take 前完整拒绝为硬约束 |
| 新控制协议与旧 Core 文字 | 后续 SPEC 要对齐 Control / Failure 区分，不能默改 Core 或泛化自动重试 |

### 12.5 维护与下一步

Probe 发现问题时，应记录目标示例、最小反例、受影响原则 / 场景和候选修订。获审定的变化更新本文；暂定替代不得偷偷进入“已确定事项”。API 验证通过后再形成正式 SPEC，并明确要同步迁移哪些现有文档和实现。

下一步候选是 **AP01 — Runtime::execute 的类型推导和 closure 模型**，并联合观察 AP02 的最小 Node 连接。本文不自动开始 Probe、不编写工程实现、不改变 V21 / G21 状态。

### 12.6 整理记录

| 日期 | 版本 | 内容 | 验证状态 |
| --- | --- | --- | --- |
| 2026-10-08 | v0.1 | 按讨论最终 Root 模型整理十二章、P01～P16、S01～S10、AP01～AP08；显式保留 O01～O05 与迁移差异 | 文档基线；AP Probe 全部未执行 |
