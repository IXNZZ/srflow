# SRFlow Core 概念与执行模型 v0.1

> 状态：**已审定的 Core 语义基线（v0.1）**。规范切换日期：2026-10-03。本文从属于 [SRFlow Design v2.1](SRFlow_Design_v2.1.md)，补充数据身份、所有权与生命周期的完整语义；实现边界见 [Runtime Implementation Design v0.1](SRFlow_Core_Runtime_Implementation_Design_v0.1.md)。v2.0 已转为历史参考；当前工程从已清理的最小 crate 重写，历史验收记录不构成 v2.1 合规结论。真实实现反例若触及 Core 语义，应带证据回到评审。
>
> 范围：概念边界、运行语义、数据所有权和生命周期不变量。文中的 Rust 片段只帮助说明关系，不是已冻结的公开 API 或 trait 签名。

## 1. 文档目的与范围

SRFlow Core 解决的是**流程实验的执行组织问题**：让 SES 等系统能调整步骤、数据连接和控制路径，而不必每次重写执行基础设施。判断一项 Core 设计是否有价值，应看它是否降低真实流程变化的成本，并使执行关系可见、可审计。

本文定义 Data、Node、Orchestrator、Runtime 及其运行时数据边界；说明 Flow、Match、Each、Loop 的共同约束和各自语义；记录从 v2.0 迁移时需要审查的概念变化。

本文不设计上层易用 API、具体 Rust trait 签名、类型擦除与存储布局、可视化或动态 DSL、分布式执行、持久化状态、自动并行调度、技术故障重试和发布包装。业务正常结论与执行错误仍是两个通道；执行失败不隐含副作用回滚。

## 2. 设计目标与原则

### 2.1 目标

- 流程级实验尽量停留在流程定义层；业务判断仍由业务单元完成。
- 输入和输出具有明确 Data Signature，连接关系可检查，同类型不同实例可区分。
- 组合边界明确，子单元可以递归调用，数据的可见性和生命周期可追踪。
- 业务 Data 的来源、拥有者和最终移交点明确，不存在隐藏的数据注入通道。
- 默认保持顺序语义；引入异步 Rust 不自动改变执行次序。

### 2.2 非目标

SRFlow Core 不是通用工作流平台、Agent Framework、分布式调度器或状态数据库。它不替业务决定故事质量、重试是否有业务价值，或外部服务应返回什么内容。新增 Core 能力必须由真实流程需求证明，不能只为了补齐一个抽象体系。

### 2.3 原则

1. **Definition 与 Execution 分离。** Definition 描述调用位置和逻辑连接；一次 Execution 才产生真实 Data 实例及运行时身份。
2. **Node 与 Orchestrator 分离。** Node 做业务动作；Orchestrator 组织调用、控制路径和 Scope。
3. **数据依赖显式。** 一个步骤读取哪份 Data，由定义中的引用及运行时绑定决定，不靠“最近一次结果”等隐式状态。
4. **单次 Execution 的数据闭包。** Root Input 与 Node Output 是业务 Data 的合法进入来源；内部收集、移动和重组不成为外部注入通道。
5. **集中管理所有权。** Execution 内业务 Data 的持续所有权由同一个 DataContainer 管理；Scope 只登记可见性及生命周期责任。
6. **语义优先于 Rust 便利。** 类型、宏、装箱、异步适配和 `Send` 约束必须服务已审定的语义，不能反过来改变语义。

## 3. Core 概念总览

```text
Application
  └─ Runtime：创建 Root Execution 的入口与运行设施
       └─ ExecutionContext：本次 Execution 的内部调用上下文
            ├─ DataContainer：业务 Data 的物理持有者
            └─ DataScope：引用可见性与生命周期责任

执行单元
  ├─ Node：业务叶子
  └─ Orchestrator：递归编排单元
       ├─ Flow
       ├─ Match
       ├─ Each
       └─ Loop
```

Definition 侧使用 `DataRef<T>`／`RefId` 描述逻辑数据位置，使用 `Step`／`CallSite` 描述调用。Execution 侧使用 `DataId` 标识真实实例，用 `RefTarget` 表示某个逻辑引用当前解析到的运行时目标。二者不能混为一谈：同一 Definition 多次执行，可生成不同的 `DataId`。

## 4. Data 模型

### 4.1 Data

`Data` 表示可进入 SRFlow Data Core、由 DataContainer 管理、被 `DataRef` 引用并参加执行输入输出的**业务数据类型**。它不意味着 ECS Entity，也不意味着一个类型天然带有实例身份；实例身份在 Execution 中由 `DataId` 表达。Node 的固定配置、框架控制对象和 Runtime 元数据不因被某个对象持有就自动成为 Data。

`()` 表示无业务 Data 输出，不要求向容器插入一个 `()` 实例。一个 `Vec<T>` 作为整体进入容器时可以是 Data；Each 借用其中某个 `T` 时，该 item 不因此取得独立 `DataId`。

### 4.2 DataRef 与 RefId

`DataRef<T>` 是 Definition 时期对一个 `T` 数据位置的强类型逻辑引用。它不拥有值，不是 Rust 的 `&T`，也不直接携带某次 Execution 的 `DataId`。`RefId` 标识编译后 Definition 空间中的一个逻辑 DataRef；**不同逻辑 DataRef 必须具有不同 RefId，不能仅因属于不同 Flow／Scope 就允许碰撞**。同一 Flow Definition 被多次 Invocation 时，同一个 RefId 可以在各自的 DataScope 中绑定到不同 RefTarget。RefId 的分配与编码方式未定。

```text
Definition identity = RefId
Runtime binding      = DataScope.refs[RefId] -> RefTarget
Execution identity   = DataId
```

同一 `DataRef<T>` 可被多个后续步骤读取；同类型的两个 `DataRef<T>` 可以指向不同实例。其他 Flow／Scope 的本地引用不能直接作为当前 Flow／Scope 的本地引用使用。跨边界传递时，应先解析为运行时目标，再绑定到接收方自己的 `RefId`。错误连接在哪个编排阶段被检测，属于待验证的实现边界；运行时不得把外来引用误解析为本地数据。

字段级 `DataRef` 不受支持。需要向后续步骤传递某字段的业务值时，应由有明确语义的 Node 产生 Data，或在已定义的整体 Data 输入中使用它；不能通过字段投影制造一个可跨 Scope 流转的独立逻辑数据实例。

### 4.3 DataId 与 RefTarget

`DataId` 是一次 Execution 内、由 DataContainer 持有的真实 owned Data 实例的运行时身份。它与 `RefId` 不等价；同一 `RefId` 在两次 Execution 中可以解析到不同 `DataId`。被移走或销毁的 `DataId` 不得再被有效目标引用；若内部复用存储位置，必须防止旧目标误指向新实例，具体 generation 机制未定。

`RefTarget` 是 Execution 内部的轻量概念，至少需要表达两类目标：

```text
Data(DataId)             已存储的完整 Data 实例
CollectionItem { ... }   某个集合内部、在受限生命周期内可借用的 item
```

`CollectionItem` 的具体字段未定。它不是独立 Data 实例，不分配独立 `DataId`；其有效期不得超过原集合及产生它的 item 生命周期。`RefTarget` 是运行时绑定机制，不提升为普通业务代码需要构造的公共对象。

## 5. DataContainer 与所有权

### 5.1 唯一物理持有者

一次 Root Execution 只有一个 DataContainer。它负责存入 Node 新产出的 owned Data、根据有效目标提供只读借用、在内部移动或重组 Data、在生命周期结束时销毁数据，并在 Root 正常结束时提取最终输出。Flow、Match、Each、Loop 和 DataScope 不建立第二套业务数据仓库。

Node 计算并返回 owned 输出是新 Data 的产生边界；当前 ExecutionContext 的内部调用机制随即将该输出纳入 DataContainer。此后的持续所有权属于 DataContainer。Orchestrator 传递目标与生命周期责任，不通过取出值再交给子单元来表达内部组合。

### 5.2 内部重组与 Root 例外

Each 把可合法转移生命周期责任的各项 owned 输出汇成 `Vec<O>`，以及 Loop 替换当前状态，属于 **DataContainer 管辖下的内部重组**。即使实现中需要临时移动值，这也不能形成对业务层开放的另一位 owner，不能允许旧 `DataId` 继续指向已搬走的 item。收集器在 finalize 前是当前 Execution 的内部机制，不能作为普通 Data 被 Node 借用。

只有 Root Execution 正常结束、完成输出目标校验后，最终 owned Data 才可以从 DataContainer `take` 并交给 Application。内部 Orchestrator 的输出仍是运行时目标。失败、被丢弃的分支或 Scope 会清理其负责的数据；清理不代表撤销已经发生的外部副作用。

## 6. DataScope

### 6.1 两项职责

DataScope 将**可见性**与**生命周期责任**放在同一个边界，但不直接存放业务值：

```text
DataScope
  refs:  RefId -> RefTarget
  owned: DataId set
```

`refs` 规定当前 Scope 能解析哪些逻辑引用。`owned` 记录该 Scope 对哪些 Data 实例负有结束时的处置责任。同处一个 DataContainer 不意味着各 Scope 可以全局读取所有数据。

### 6.2 Import、Export 与退出

创建 child Scope 时，调用方先解析自己的输入 `RefId`，再把得到的 `RefTarget` 绑定到 child 的输入 `RefId`；两个 Scope 不共享同一个本地 `RefId` 身份。导入的 Data 仍由原 Scope 负责，不加入 child 的 `owned`。

child 作为 Definition 声明输出向 parent 暴露的数据，要绑定到 parent 的输出 `RefId`。若输出是 child-owned Data，Export 同时转移生命周期责任；若只是导入目标的再输出，则不能凭空产生第二份所有权。绑定和责任转移须作为一个原子生命周期操作：不能只更新引用、却留下错误的 owner，也不能转移 owner 后让 parent 无法解析目标。Each item 的内部收集和 Loop round 的状态提升属于内部消费／保留，不要求在父 Scope 绑定逐项、逐轮的 Definition RefId。

child 退出时，其本地 `refs` 消失；未导出、未合法提升或消费的 child-owned Data 由 DataContainer 按 Scope 责任销毁。已导出或提升的 Data 由责任方继续负责；消费进 collector 的值仍在 DataContainer 内部，旧 DataId 失效。指向 `CollectionItem` 的目标还须通过来源生命周期检查，不能仅靠跨 Scope 操作就突破有效期上限。

### 6.3 Scope finalization 的语义次序

当 child 的实际调用及所有 descendant 已完成、相关业务借用已结束，Scope 先进入冻结状态：不再启动 child 调用，也不再创建供业务执行使用的本地引用或借用。随后解析声明的输出，验证目标仍有效、类型与生命周期边界允许导出，并检查消费与别名约束。验证未通过时不得留下部分导出；按失败退出规则清理本地数据。

验证通过后，Export 将 parent 绑定与 child-owned 责任转移作为同一操作完成；需要消费的内部输出由 DataContainer 在其管辖下移动，并使旧目标失效。最后使未导出的本地引用失效，处置剩余 child-owned Data，再销毁 Scope。**引用失效与数据移除必须作为不可观察的清理边界协调**，不能留下仍有效却指向已移除 DataId 的 RefTarget。此处规定的是语义先后，不预定 Rust 的逐条操作或容器布局。

Each 的 collector 属于内部消费：可收集的 item 输出须在 ItemScope 仍有效时完成合法的生命周期责任交接，再使 ItemScope 退出；这不要求先 Export 并绑定 EachScope 的 Definition RefId，可以由 DataContainer 在同一消费边界把 ItemScope-owned 值移入 collector、撤销原责任并使旧 DataId 失效。只重新暴露 imported Data 的输出没有可转交的 item-owned 责任，不能被 collector 移走。最终 `Vec<O>` 形成后，EachScope 才能解析并导出自己的声明输出。具体直接消费与状态提升机制见 Runtime 实现设计，仍须整合验证。

## 7. Execution 与 Invocation

### 7.1 Root 边界

概念上的 `Runtime::execute(root, input)` 发起一次新的 Root Execution，创建一个 ExecutionContext、一个 DataContainer 和 Root DataScope。Application 传入的 owned Root Input 在此边界进入 DataContainer；Root 输入引用随后被绑定。这里的签名是语义示意，不预定最终 Rust API。

### 7.2 内部递归调用

Node 或 Orchestrator 的每次实际调用都是一个 Invocation。Root Invocation 由 `Runtime::execute` 创建；其后的 child Invocation 均经**当前 ExecutionContext 的内部调用机制**进入，继续使用同一个 ExecutionContext 和 DataContainer，不再次调用创建 Root Execution 的入口。这样，一棵调用树有明确父子关系，但只对应一套本次执行的数据身份空间。

Invocation 是一次调用；DataScope 是引用和生命周期边界。二者不是同义词，也不要求严格一一对应：某个 Invocation 可建立 Scope，某个轻量调用也可能沿用既有 Scope，具体划分由 Orchestrator 的语义决定。

父 Invocation 不得在仍有存活 child Scope 时完成其自身 Scope 的 finalization。不存在脱离父级存活的 child Scope；即使以后增加并行 Each，也必须在退出父边界前完成 join 与清理。

## 8. Execution Data Closure

Root Input 是外部业务 Data 进入一次 Execution 的入口。Execution 内新增业务 Data 的正常来源是 Node Output。Framework 对已有 Data 的移动、聚合、导出和状态推进属于受 DataContainer 管辖的内部重组，不是第三种任意注入来源。

因此 Core 不提供 `flow.data(value)`、`DataRef::new(value)` 等绕过调用链的业务 Data 注入方式。HTTP、文件、数据库、时间、环境变量和模型响应等外部信息若要成为可追踪的业务 Data，应由 Node 获取并通过 Output 交给当前 ExecutionContext 纳入 DataContainer。Node 可持有固定配置并访问普通业务库或外部服务，但不能把隐式 SRFlow 数据通道藏在这些依赖中；若某个值是执行的业务输入，应走 Root Input 或上游 Node Output。

## 9. Node：业务叶子

### 9.1 输入、输出与错误

Node 负责一次明确的业务动作。**当前 ExecutionContext 的内部调用机制根据输入目标从 DataContainer 取得只读借用，再把 `&Data` 作为参数交给 Node**；Node 本身不访问 DataContainer。Node 正常执行后产生新的 owned Data，或只完成动作而返回 `()`。例如 `fn judge(plan: &Plan, prose: &Prose) -> Result<Judgment>` 表示借用两份已有 Data 并产生 Judgment。参数按值取得已有 Data 的写法不属于默认 Node 输入语义，也不应靠隐式 `Clone` 实现。零输入 Node 与无输出 Node 均成立；`Result<()>` 的 `()` 不产生新的数据槽。

业务上的“不接受”“需要重试”等可以是正常 Output 中的状态；调用失败、外部服务错误等属于执行错误通道。错误默认向调用方传播，不自动重试、跳过或回滚。

### 9.2 边界

Node 不持有 `DataRef`，不解析 `RefTarget`，不访问 DataScope／DataContainer／ExecutionContext，也不通过 Runtime 启动 SRFlow child。需要选分支、循环或组合其他单元的逻辑应进入 Orchestrator。Node 可以调用普通业务库或外部服务；这些调用不因此变成隐藏的 SRFlow 子编排。

普通同步函数、普通异步函数和持有固定配置的结构体，都是候选 Node 表达形式。借用式 Probe 已在稳定 Rust 上分别验证函数与结构体 Node 可共用 `flow.then`，零至二个参数可连接，结构体可跨 `await` 使用借用；同一个 `Arc<具体 Node>` 可用克隆句柄加入两个 Flow。这些是可行性证据，不冻结 trait 名称、参数上限、异步适配机制或 `Arc<dyn Node>` 支持。Future 的 `Send` 约束仍待实现设计处理。

## 10. Orchestrator：递归编排单元

> Orchestrator 是具有明确 Data Input／Output Signature、能够在当前 ExecutionContext 中建立自己的控制或数据生命周期边界，并可递归调用 Node 或其他 Orchestrator 的编排执行单元。

当前 Core 的 Orchestrator 是 Flow、Match、Each、Loop。其 Input／Output Signature 描述所需输入与**对外暴露的输出**的 Data 形状；输出可以直接导出已导入的 Data，不一定由该 Orchestrator 新生产。这不意味着在内部按 Rust `I -> O` 转移业务值所有权。内部调用接收和返回运行时目标，由 Scope 完成绑定与生命周期转移。

| 维度 | Node | Orchestrator |
| --- | --- | --- |
| 职责 | 业务动作、判断和计算 | 执行顺序、选择、重复与组合 |
| 输入 | 当前 ExecutionContext 内部调用机制提供的只读 Data 借用 | 已解析的运行时目标 |
| 正常输出 | 新的 owned Data 或 `()` | 运行时目标或无输出 |
| ExecutionContext | 不可见 | 在当前 ExecutionContext 内使用 |
| DataScope | 不管理 | 按自身语义建立、导入、导出、结束 |
| SRFlow child | 不调用 | 可通过当前 ExecutionContext 的内部调用机制递归调用 |

Node 也会执行，但不因此归入 Orchestrator。统一的 Builder 表面写法，不要求二者实现同一个运行时 trait。

## 11. Definition Call Model

`CallSite` 是 **Orchestrator 调用 child 的通用 Definition 描述**：它表示“在这里调用一个 Node 或 Orchestrator”，保存调用对象及逻辑输入／输出关系，而不保存某次 Execution 的 `DataId`。Flow Step、Match Branch、Each Body 与 Loop Body 均通过 CallSite 指向 child。`Step` 只是 Flow 中额外具有顺序位置的 CallSite；Step 本身不拥有 DataScope。每次实际执行 CallSite 时，当前 ExecutionContext 的内部调用机制建立对应 Invocation、解析输入目标，再按被调用单元的语义执行。

概念上可以有统一的 Builder 入口：

```rust
flow.then(node, args);
flow.then(sub_flow, args);
flow.then(match_unit, args);
```

`args` 的类型必须与被调用单元的输入 Signature 对应。Node 参数 `&A` 对应编排时的 `DataRef<A>`；多参数按位置对应。P01 已局部验证普通函数 Node、结构体 Node 与 Orchestrator 共用一个 typed `then` 并异构保存 Step；与实际 DataContainer／Scope 调用链的整合仍需验证。`Binding` 不保留为独立 Core 概念或业务侧接线对象。

## 12. Flow

### 12.1 Definition 与顺序

Flow 定义自己的输入、按顺序排列的 Step 及输出。它负责显式连接 Data 依赖，不执行业务计算。`flow.input`、`flow.then`、`flow.output` 可用来说明 Definition 层职责，但具体方法名和签名仍未冻结。

Flow 必须完成明确的 Output Signature 定义后，才能作为完整 Orchestrator 被执行或组合。显式 `()` Output 表示无业务 Data 输出，不同于尚未完成 Output 定义。

Flow Input Signature 可为单个 Data 或多个 Data 的 tuple；Output Signature 支持 `()`、单个 Data 和多个相互独立的 Data。**是否允许 `()` 作为 Flow Input 暂未审定**：早期讨论倾向不支持，最终大纲提出允许，本文不把任一方向写成 Core 规则。零输入 Node 的已验证能力也不能直接推出零输入 Flow 已获批准。相同类型的多个位置由逻辑引用及位置区分，不靠类型本身猜测身份。同一 DataRef 可作为多个只读输入使用；这不等于允许同一 owned Data 在 Root Output 中被取走两次。

Flow 的 Step 按定义顺序执行：上一步完成后才开始下一步。输入输出的数据依赖和执行顺序是两件事，即使后一步不读取前一步的输出，也不能因此由 Runtime 自动并行。

### 12.2 ExecutionContext 绑定与 SubFlow

调用 Flow 时，当前 ExecutionContext 将调用方提供的目标绑定到 Flow 自己的输入 RefId。每条 Step 先在当前 Scope 中解析其输入引用，再经当前 ExecutionContext 的内部调用机制调用 Node 或 child Orchestrator；调用的正常输出目标绑定到 Step 的输出 RefId。Flow 结束时，按定义的输出引用导出目标。

SubFlow 是 child Flow 的一次调用：它建立自己的 DataScope，导入明确给出的目标，只让内部 Step 看到自己的引用，正常结束时将声明的输出导回 parent，并在 Scope 退出时清理未导出的内部 Data。SubFlow 不创建第二个 Root Execution 或 DataContainer。

## 13. Match

Match 根据**已有的路由键或判断 Data**选择一个 branch；复杂判断先由 Node 产出 Data，Match 不暗中计算业务结论。路由输入不要求是专门命名为 Judgment 的类型。只执行被选中的 branch。未选 branch 不建立运行 Scope，也不产生 Data 或副作用。被选 Branch 的 child 调用由 CallSite 描述。

被选 branch 在 BranchScope 中接收显式输入；其正常输出按 Match 的 Output Signature 导出。各 branch 的外部 Signature 必须兼容；需要表达不同业务结果时，可以把差异建模为 enum Data，而不是让 Match 返回不稳定的类型。BranchScope 的导入、owned Data 处置和导出遵循第 6 节。

沿用 v2.0 未受本次模型改变影响的路由边界：配置了 default 才在未命中时走 default；没有可选 branch 时返回错误；被选 branch 执行出错不能改走 default。具体 branch 注册、选择值表示和 API 仍属实现设计。

## 14. Each

### 14.1 集合与 item

本版 Core 只把 `Vec<T>` 作为 Each 的集合输入形状，不提前抽象通用 Collection。一个 `DataRef<Vec<T>>` 指向容器中的完整集合 Data。Each 在自身 Scope 下按顺序创建 ItemScope，逐项调用 body；空集合不调用 body，正常产生空集合输出。默认顺序语义不因 async 而变化。

每个 item 由 `CollectionItem` 目标表示，从原 `Vec<T>` 借用为 `&T`：不为 item 分配独立 `DataId`，不为了遍历而隐式 `Clone` 或移走原 item。ItemScope 可把目标导入其 descendant，前提是 descendant 不超过 item 的来源生命周期；该目标不得导出到 lifetime cap 之外。共享的普通 Data 输入也须经显式导入，不能因为与集合处于同一个 DataContainer 就自动可见。

### 14.2 输出收集与失败

Body 的 child 调用由 CallSite 描述。当前 item 调用链中新产生的 owned item 输出进入 DataContainer，并各有自己的运行时身份。collector **只能消费生命周期责任可合法转移给 EachScope 的 item 输出**，在 DataContainer 的内部重组边界把这些值移动进 `Vec<O>`，无需 `Clone`。被移动的旧 `DataId` 随之失效；未 finalize 的 collector 不能作为普通 Data 被借用。完成后，`Vec<O>` 成为普通 Data，取得自己的目标并导出到 parent。

若 body Output 只是重新暴露 imported Data，无论目标是 `CollectionItem`，还是指向 parent-owned 完整 Data 的 `Data(DataId)`，都没有可供 collector 消费的 item-owned 责任，不能将它从原 owner 处 `take` 后放进 `Vec<O>`。需要独立集合元素时，业务 Node 必须显式从借用输入产生新的 owned Data；Core 不隐式 `Clone`。是否允许这种不可收集的 Output 作为其他形式对外暴露，属于另行设计，不能借 collector 的移动路径实现。

某项执行错误时，Each 停止后续项、清理未导出的内部 Data，向外传播错误，不返回部分正常集合。已发生的外部副作用不自动回滚。并行 Each、一般化 Collection 和多输出收集细节留待后续设计。

## 15. Loop

Loop 是重复执行的 Core primitive；Retry 与 Iter 是两种不同的推进策略，不再是互相独立的 Core primitive。LoopScope 管理各 RoundScope；每轮结果可被丢弃、提升为下一轮状态、作为最终结果导出，或因执行错误终止。所有 body 调用仍经当前 ExecutionContext 的内部调用机制；下一轮只看到策略明确传递的输入和目标。

Loop Body 的 child 调用由 CallSite 描述；不同策略只决定如何使用本轮结果和形成下一轮输入，不改变 Node／Orchestrator 的调用边界。

### 15.1 Retry 策略

Retry 在同一原始业务输入下重新执行 body；上一轮 Output 不自动成为下一轮 Input。控制判断只读取正常 Output 已表达的业务状态，不在策略内部创造新的复杂业务判断。

若策略决定继续，本轮结果随 RoundScope 结束而丢弃，下一轮仍使用原始输入；若策略决定完成，本轮结果可作为正常输出导出。此前各轮的结果不默认形成历史。body 执行错误立即传播，不触发技术重试。执行次数约束、`limit = 0`、默认次数以及耗尽时的正常结果规则属于旧 v2.0 待迁移确认的 Retry 策略细节，**本版 Loop Core 不定案**。

### 15.2 Iter 策略

Iter 按顺序推进累积状态：若策略决定继续，本轮选定的正常 Output 被提升为下一轮状态。**提升仅更新当前 state 目标，不自动取得 imported Data 的 ownership。**若被替换的旧状态由 LoopScope 承担生命周期责任，则在不再需要后由 DataContainer 处置；若旧状态是导入目标，其原 owner 的生命周期责任不因退出当前 state 位置而改变。策略决定完成时导出当前状态。中途错误停止后续轮次，不把上一轮状态冒充正常最终输出。默认不保存每轮历史；需要历史时应把它显式建模为业务 Data。是否由 item 序列驱动、序列为空时如何处理，以及具体停止条件，留待后续策略设计，不属于本版 Loop Core 的必然语义。

状态替换必须区分“新旧目标是不同 `DataId`”与“本轮返回的仍是同一 `DataId`”。后者不能先删除旧状态再导出新状态，也不能给同一实例建立两份互相矛盾的 owned 责任。P06 已局部验证该不变量；正式 Promote 的目标比较与责任转移见 Runtime 实现基线，仍须在整合调用链中复验。

## 16. Root Input 与 Root Output

Application 持有的 Root Input 在 `Runtime::execute` 边界移入 DataContainer，随后绑定 Root Scope 的输入引用。内部 Orchestrator 调用则传递运行时目标，不从 DataContainer `take` 值来模拟 `I -> O`。

Root 正常结束时，Runtime 解析全部 Root Output Target，验证目标可提取，再从 DataContainer 提取 owned 值返回 Application。`()` 输出不提取业务 Data。若多个 Root 输出解析到同一 `DataId`，**必须在任何 `take` 之前拒绝本次 Root extraction**，即使它们来自不同的逻辑 `DataRef`；不自动 `Clone`。具体诊断形式及能否更早发现别名仍属实现设计。

## 17. 生命周期与安全不变量

以下规则用于审查实现与后续 Compile Probe，不能因内部优化而被静默削弱：

1. 一次 Root Execution 只有一个 ExecutionContext 和一个 DataContainer；内部递归调用不创建新的 Root 数据空间。
2. 不同逻辑 DataRef 必须具有不同 `RefId`，不因属于不同 Flow／Scope 而碰撞；`DataId` 是 Execution 中真实数据实例身份，两者不能互换。
3. Scope 只可解析已绑定到本 Scope 的引用。同一 DataContainer 不赋予跨 Scope 的全局读取权。
4. 导入的 Data 不加入 child 的 `owned`；child 退出不能销毁仍归祖先负责的数据。
5. 导出 child-owned Data 时，目标绑定与生命周期责任转移必须原子完成。
6. Scope 退出时本地引用失效；未导出的 owned Data 得到处置。parent 不得先于尚在运行的 descendant 完成 finalization。
7. `CollectionItem` 不能超过原集合及 item Scope 的有效期；任何 import／export 都不能让它逃逸。
8. 被移动、提取或销毁的 `DataId` 不得留下有效 `RefTarget`；存储位置复用不能使 stale target 重新有效。
9. Each collector 在 finalize 前是 DataContainer 管辖的内部结构，不是业务可借用 Data；它只能消费 item 调用链中新产生且责任可合法转移的 owned Data，不能移动 imported Data；搬入 collector 的 item Output 旧身份随之失效。
10. Loop 状态替换须按生命周期责任处置旧状态：导入目标仍由原 owner 负责，只有 LoopScope 负责的旧状态才可在不再需要后销毁；同一 `DataId` 的替换不能重复销毁、重复拥有或使仍需输出的状态失效。
11. Root 多输出若解析到同一 `DataId`，须在任何提取前拒绝；内部共享只读目标与最终移交所有权是不同操作。
12. 执行错误默认停止当前路径并向调用方传播；不能把正常业务拒绝偷换成技术错误，也不声称撤销已发生的外部副作用。

## 18. 命名与术语

| 术语 | 本 Core 基线中的含义 |
| --- | --- |
| `Data` | 可由 DataContainer 管理的业务数据类型 |
| `DataRef<T>` | Definition 层指向 `T` 数据位置的强类型逻辑引用 |
| `RefId` | Definition 层的逻辑位置身份 |
| `DataId` | 本次 Execution 中 owned Data 实例的运行时身份 |
| `RefTarget` | Execution 内部的目标，包含完整 Data 或受限的 CollectionItem |
| `DataScope` | 引用可见性与 Data 生命周期责任边界 |
| `DataContainer` | 本次 Execution 中业务 Data 的物理持有者 |
| `Node` | 借用 Data 输入并产生 owned Output 的业务叶子 |
| `Orchestrator` | 在当前 Execution 中递归组织调用与 Scope 的单元 |
| `Flow` | 顺序 Step 与显式数据连接 |
| `Match` | 基于已有路由键或判断 Data 的单一路由 |
| `Each` | 对 `Vec<T>` 逐项执行并收集结果 |
| `Loop` | 由 Retry 或 Iter 策略驱动的重复执行 |
| `Runtime` | Application 发起 Root Execution 的入口及运行设施；`Runtime::execute` 创建新 Execution |
| `ExecutionContext` | 一次 Root Execution 共享的运行上下文，承载内部 Invocation 的调用机制 |
| `Invocation` | 一次实际执行单元调用，不等于 DataScope |
| `CallSite` | Definition 中调用 Node／Orchestrator 的位置 |
| `Step` | Flow 中有顺序位置的一条调用记录 |

v2.1 正式采用 Node／Orchestrator 分离及 Data 命名。`Component → Data` 属于早期 Core 草案的命名收敛，不能当作正式 v2.0 API 的改名；新 crate 实现按当前术语逐任务建立。`RefTarget`、`CallSite`、`Step` 首先是 Core 内部描述，不要求成为业务使用者直接操作的公共类型。

## 19. 与旧 Core 设计的关系

本节保留**历史架构迁移对照**，用于解释新旧模型的关系。当前实施路线为从空工程重写，表中旧 API／示例的迁移影响属于历史使用方式的对照，不要求恢复已清理的代码或做逐文件盘点。

| v2.0 或早期 Core 草案的概念／写法 | 新模型中的处理 | 迁移时要核对的影响 |
| --- | --- | --- |
| 早期 Core 草案 `Component` | 命名为 `Data` | trait／derive 名称与文档、示例需统一；不是正式 v2.0 的公共命名 |
| `Executable` | 编排职责命名为 `Orchestrator` | Node 与 Orchestrator 不再因“都可执行”而被迫归为同一业务抽象 |
| `Binding` | 不作为独立 Core 概念 | 旧的投影、装配和按值输入用法须逐一复审：数据连接显式表达，业务转换归 Node，不能自然迁移的用法留待设计 |
| 不使用 `()` Flow Input 的早期讨论与最终大纲提出的零输入 Flow | 本文不定案 | 须单独审定；不能从零输入 Node 的 Probe 推出 Flow 也支持 `()` Input |
| 字段级 `Ref`／`DataRef` | 不支持 | 需检查旧字段投影示例、宏和 API 承诺 |
| 早期 Core 草案 `DataSource` | 不作为独立 Core 概念；运行时目标由 `RefTarget` 表达 | 不把来源机制误当成可注入业务 Data 的接口；不是正式 v2.0 的公共命名 |
| `Retry`、`Iter` 两个 Core primitive | 收敛为 `Loop` 的两种策略 | 本文只定继续时 discard／promote 的不同推进语义；旧 Retry 次数与耗尽规则、旧 Iter 的 item 序列规则待迁移确认 |
| 内部 `execute(I) -> O` 的值所有权理解 | 内部 Orchestrator 传递 Runtime Target | Root Input／Output 是 owned 值进出的边界；子调用不提取业务值 |
| 每次子调用独立值存储 | 一次 Root Execution 一个 DataContainer | 原有 Runtime、SubFlow 和控制器的存储／清理路径需重新验证 |

旧文档的流程目标、顺序语义、正常业务结果与执行错误区分、Node 不隐式编排等保留原则已纳入 v2.1。新工作以 v2.1 及本 Core 基线为规范；从空工程建立新机制，不能继续用已被替代的 v2.0 条款覆盖新语义。

## 20. 未冻结的实现设计与开放点

以下事项**不能从本文示意写法推定为已批准的公共 API**：

- `Data`、`Node`、`Orchestrator` 的最终 Rust trait 形式；普通函数、结构体 Node 与 Orchestrator 的统一 `then` 类型机制。
- `Targets<I>` 是否为真实类型，tuple target 的表示，CallSite runner、异构 Step 存储及类型擦除方式。
- `DataRef`／`RefId` 的内部编码、跨 Flow 归属错误的检测阶段及 Root Input 的编译期验证程度。
- `()` 是否可作为 Flow Input；零输入 Node 可行不等于零输入 Flow 的 Core 规则已审定。
- 异步适配后 Future 的 `Send` 约束、并发执行、`Arc<dyn Node>` 与 trait object 边界。现有借用式 Probe 验证了 `Arc<具体 Node>` 的双 Flow 顺序复用；不能据此推出这些扩展均已成立。
- DataContainer 的精确存储布局、借用协调与各内部接口。DataId／ScopeId 单次 Execution 内不复用、Each 直接 Consume、Loop 无 Ref-binding Promote 已在 Runtime 基线定下；具体路径仍须整合验证。
- Root 多输出别名能否在执行前发现及其错误形式、Loop same-DataId 状态替换的具体算法。
- Retry 的次数限制、零次配置与耗尽结果；Iter 的 item stream、空序列和终止策略。这些是旧 v2.0 待迁移确认或后续策略设计，不作为本版 Loop Core 结论。
- parallel Each、通用 Collection、Bundle、多输出收集的完整规则，以及新增 Scope 类型的实现表示。

这些未定问题不授权改变已明确的语义边界。若 Compile Probe 发现某条语义不能按预期实现，应带着最小反例回到设计评审，而不是在代码中静默改写。

## 21. 后续验证与文档迁移

### 21.1 已有可行性证据

独立借用式 Node Probe 的 P01～P04 已覆盖：普通同步／异步函数作为 Node、零至二个借用参数、无输出 `()`、无隐式 `Clone`、类型错接的编译失败、结构体 Node 与函数共用 `then`、结构体跨 `await` 借用，以及 `Arc<具体 Node>` 复用于两个 Flow。该 Probe 只证明这些局部候选形态可行，不是正式 SRFlow Core 的实现模板。对泛型 `AsyncFn` 返回 Future 的 `Send` 证明未通过，暂不把它视为借用语义本身的否定。

### 21.2 已完成的局部 Probe 与整合验证

第一批 Core Compile Probe 已完成，对以下边界给出了 safe Rust 下的局部正向运行与反向拒绝证据，详见 [P01～P07 结果](SRFlow_Core_Compile_Probe_Results_v0.1.md)：

1. Node 与 Orchestrator 能否共存于异构 CallSite／Flow Step，并沿当前 ExecutionContext 调用。
2. child Scope Export 时，目标绑定与 owned 责任能否原子转移。
3. Each collector 能否拒绝指向 imported 完整 Data 的 body Output，同时消费合法的 item-owned Output。
4. `CollectionItem` 能否安全借用，并阻止跨越 item 来源生命周期逃逸。
5. Iter 替换 imported 初始状态时，能否保持原 owner 的 Data 存活。
6. Iter 新旧状态指向同一 `DataId` 时，能否避免重复处置或错误失效。
7. Root 多输出指向同一 `DataId` 时，能否在任何提取前整体拒绝。

七项局部结果均 PASS，但异构 CallSite 与 Data Core 尚未端到端整合。直接 Consume、无 Ref-binding Promote 及完整 Scope finalization 应在正式调用路径重新验证；公开 API 不因局部结果而冻结。

### 21.3 文档迁移

设计规范已于 2026-10-03 按 [v2.1 §25](SRFlow_Design_v2.1.md#25-审定与规范切换记录) 切换：v2.1 为总规范，本文为 Core 语义依据，Runtime 文档为内部实现基线。AGENTS 与任务入口已同步，v2.0 与旧任务保留为历史参考。当前从空工程重写，代码、使用示例与公开 API 由新的详细任务逐步交付并验收。
