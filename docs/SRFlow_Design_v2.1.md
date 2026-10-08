# System Runtime Workflow（SRFlow）Design v2.1

> 状态：**已审定，当前总设计规范**。规范切换日期：2026-10-03。本文定义 SRFlow v2.1 的整体架构、核心概念、执行语义与组合规则，并替代 [SRFlow Design v2.0](SRFlow_Design_v2.0.md) 的现行规范地位。实施路线已由用户确认为在现有 crate 中从空工程重写 Core；旧实现、测试与示例已清理，历史设计和验收记录保留原范围，不构成 v2.1 合规结论。
>
> 本文规定 v2.1 要提供的保证；具体 Runtime 实现和公开 Rust API 的完成情况须分别验收。示例表达语义，方法名、trait 签名和宏语法不因此冻结。

## 1. 文档定位与规范层级

本文是 SRFlow v2.1 的总入口，说明 SRFlow 的能力、公开架构、使用边界和框架保证。三层规范的职责如下：

| 文档 | 职责 |
| --- | --- |
| 本文 | 整体架构、执行语义、使用与组合规则 |
| [Core Design v0.1](SRFlow_Core_Design_v0.1.md) | 数据身份、所有权、Scope 等完整 Core 语义与设计依据 |
| [Core Runtime Implementation Design v0.1](SRFlow_Core_Runtime_Implementation_Design_v0.1.md) | 在上述语义下的 Runtime 内部实现边界 |

实现选择不得改变上层语义。本文未规定的 Core 细节由相应子文档补充；若文档规定彼此冲突，须明确评审和修订，不能由实现者静默择一。v2.1 是一次架构升级，迁移影响见 §21；具体存储布局、类型擦除、Scope 提交算法不在本文展开。

## 2. SRFlow 定位

SRFlow 是面向强类型业务流程的嵌入式执行与编排框架。它将业务步骤、数据依赖和控制结构组织为可组合、可执行的流程，使流程变化尽量停留在定义层，降低反复修改基础控制代码的成本。

核心目标是强类型连接、显式依赖、可组合的控制结构、清晰的数据生命周期，以及可测试、可审计的执行关系。业务 Node 能专注于业务动作，无需理解 Runtime 内部。SES 等业务系统是需求与验证背景，不构成 SRFlow 的构建依赖；LLM 等扩展不成为核心的强制依赖。

SRFlow 不承担通用 BPM 服务、Agent Framework、分布式调度、状态数据库、event sourcing、动态 Graph DSL 或依赖注入框架的职责。

## 3. 总体架构

```text
Application
  └─ Runtime：发起 Root Execution
       └─ Root Orchestrator
            ├─ Flow：顺序编排
            ├─ Match：单一路由
            ├─ Each：集合逐项执行
            └─ Loop：重复执行
                 └─ child：Node 或 Orchestrator

Definition：DataRef<T> 描述逻辑数据位置
Execution：这些位置绑定到本次执行的真实业务 Data
```

SRFlow 的执行单元分为 Node 与 Orchestrator。Node 执行业务动作；Orchestrator 组织顺序、分支、逐项和循环，并递归调用其他单元。Runtime 创建一次 Root Execution，内部递归继续处于同一 Execution。

## 4. 核心术语

| 名称 | v2.1 含义 |
| --- | --- |
| Data | 参与 SRFlow 执行输入输出的业务数据类型 |
| DataRef<T> | Definition 中对 T 数据位置的强类型逻辑引用 |
| Node | 借用输入并执行业务动作的叶子单元 |
| Orchestrator | 具有明确输入输出 Signature 的递归编排单元 |
| Flow | 按定义顺序执行步骤的 Orchestrator |
| Match | 根据已有路由 Data 选择一个 branch 的 Orchestrator |
| Each | 按集合顺序逐项执行 body 并收集结果的 Orchestrator |
| Loop | 按策略重复执行 body 的 Orchestrator |
| Runtime | Application 发起 Root Execution 的入口与运行设施 |
| Execution | 一次 Root 执行及其全部内部调用 |
| Invocation | 某个 Node 或 Orchestrator 的一次实际调用 |
| Signature | 一个执行单元的输入输出 Data 类型、位置与数量契约 |

RefId、DataId、RefTarget、DataScope、DataContainer 属于 Core 数据身份和生命周期机制，普通业务 Node 不直接操作它们。Definition 描述调用和连接，不保存某次 Execution 的真实 Data。

## 5. Data

Data 可作为 Root Input、Node Output，以及编排单元的输入输出参与执行。一个业务类型可以有多个不同实例；类型本身不表示实例身份。Data 不带 ECS Entity 含义。

一次 Execution 中，业务 Data 的合法进入来源是 **Root Input 与 Node Output**。框架对已有数据的收集、移动和状态推进属于受控内部重组，不构成任意注入通道。Core 不提供将任意 owned 值直接塞进流程定义的旁路，例如 `flow.data(value)`。

Node 的固定配置、服务客户端及 Runtime 元数据不自动成为执行 Data。若一个值是本次执行的业务输入，应显式进入 Root Input 或上游 Node Output。`DataRef` 是逻辑引用；`()` 表示无业务 Data 输出，不创建一个业务数据实例。

## 6. DataRef 与显式数据连接

`DataRef<T>` 表示 Definition 中一份 T 数据的固定逻辑位置。它不拥有值，也不是 Rust 的 `&T`。同一引用可重复用作只读输入；同类型的不同引用可以区分不同数据位置。每次 Invocation 的本地绑定只成功赋值一次，可变 Loop 状态由运行时控制状态表达。

```rust
// 语义示例：方法签名尚未冻结。
let draft = flow.then(generate, context);
let judgment = flow.then(judge, (draft, rules));
```

读取哪份 Data 由显式引用关系决定，不能依赖“最近一次结果”等隐藏状态。另一 Flow 的本地引用不能直接当作当前 Flow 的本地引用；跨组合边界应通过明确的输入输出契约传递。

v2.1 不支持字段级 DataRef 投影。业务 Node 可读取完整输入中的字段；若后续步骤需要独立业务值，应由 Node 显式产生新的 Data。Binding 不再作为独立 Core 概念或业务接线对象。

## 7. Node

Node 是业务叶子。其输入是对已有 Data 的只读借用，正常输出是新的 owned Data 或无输出 `()`：

```rust
fn judge(plan: &Plan, prose: &Prose) -> Result<Judgment>;
fn seed() -> Result<Seed>;
fn write_log(log: &Log) -> Result<()>;
```

以上为语义示意。Node 不按值取得已有输入 Data，也不靠框架隐式 Clone 获得输入。成功返回 `()` 只表示动作完成。零输入 Node 成立；这一能力不决定零输入 Flow 是否支持。

Node 可计算、访问 HTTP／数据库／文件／模型服务、获取时间或环境信息，以及调用普通业务库。Node 不接触 DataRef、DataScope、DataContainer 或 ExecutionContext，也不通过 Runtime 编排 SRFlow child；需要分支、逐项或循环的流程逻辑交给 Orchestrator。

普通同步／异步函数、持有固定配置的结构体和 `Arc<具体 Node>` 是已获局部可行性证据的候选形态。相同 Node 句柄复用于不同 Flow 不共享这两个 Flow 的 Execution Data；具体 async 适配和线程约束仍见 §18。

## 8. Orchestrator

Orchestrator 具有明确 Data Input／Output Signature，在当前 Execution 中组织控制结构，并可递归调用 Node 或其他 Orchestrator。当前 Core 类型是 Flow、Match、Each 与 Loop。

| 维度 | Node | Orchestrator |
| --- | --- | --- |
| 职责 | 业务动作、计算和判断 | 顺序、选择、逐项、重复与组合 |
| 输入 | Data 只读借用 | 明确的数据连接契约 |
| 正常输出 | 新 owned Data 或 `()` | 对外声明的数据结果或无输出 |
| SRFlow child | 不调用 | 可递归调用 |
| 生命周期边界 | 不管理 | 按自身语义管理内部边界 |

Orchestrator 可对外重新暴露合法导入的数据，不要求每份输出都是新实例。内部组合传递目标及生命周期责任；业务值仅在 Root 边界移入或移出。Node 的执行能力不要求它实现 Orchestrator；统一编排入口也不要求二者共享运行时 trait。

## 9. Flow

Flow 定义自身输入、按顺序排列的 Step 和声明输出。输入 Signature 支持单个 Data 或按位置组成 tuple 的多个 Data；输出支持 `()`、单个 Data 或多个 Data 位置。是否允许 `()` Flow Input 仍未审定。

Flow 必须完成明确的 Output Signature 定义，才能作为完整 Orchestrator 被执行或组合。显式 `()` Output 表示无业务 Data 输出；尚未定义 Output 的 Flow 则是未完成的 Definition，不能作为完整可执行 Flow。`flow.output(())` 是表达前者的候选写法。

`then` 是统一接入 Node 和 Orchestrator 的候选表面形式，例如 `flow.then(node, args)` 与 `flow.then(sub_flow, args)`。参数必须与被调用单元的输入 Signature 对应；这一要求不冻结具体 Builder 方法或内部适配方式。

Step 定义顺序就是执行顺序：前一步完成后才开始后一步。没有数据依赖不产生自动并行授权。Flow 负责连接已有数据与组织步骤，业务计算由 Node 完成。

SubFlow 具有独立的本地数据可见性和生命周期边界，导入明确输入，暴露声明输出，清理未保留的内部数据；它继续使用当前 Execution，不新建 Root 执行。多 Data 编排输出不等于已支持 Node 一次产生多个独立输出；Bundle 仍是未来能力。

## 10. Match

Match 根据已有路由键或判断 Data 选择一个 branch。复杂判断先由 Node 产生；路由类型不要求专门命名为 Judgment。被选 branch 可以是 Node 或 Orchestrator，各 branch 必须具有兼容的对外输入输出契约；业务结果形状不同可通过 enum Data 表达。

一次调用只执行被选 branch。未选 branch 不执行、不产生 Data 或副作用。只有配置了 default，未命中才执行 default；没有匹配且没有 default 时返回执行错误。被选 branch 失败直接传播错误，不改选其他 branch 或 default。

## 11. Each

v2.1 当前集合范围为 `Vec<T>`。Each 按输入顺序逐项调用同一个 body，结果顺序与 item 顺序对应；前项完成后才开始后项。空集合不调用 body，正常返回空结果集合。

item 从原集合借用为 `&T`，不隐式 Clone、不从集合移走，也不获得独立数据实例身份。body 可重复读取 item，并接收显式 shared Data；Each 不自动把前项 Output 接到下一项 Input。

每项只收集**该 item 调用链中新产生、具有可合法转移生命周期责任的 owned 输出**。直接重新输出 shared／imported Data 或借用 item，均不能作为 owned 集合元素消费。若业务需要独立值，应由 Node 显式产生；原集合及 shared input 保持有效。

某项失败时停止后续项，清理内部结果并传播错误，不返回部分正常集合；已发生的外部副作用不回滚。当前不包含 parallel Each、通用 Collection 和多输出收集的完整规则。

## 12. Loop

Loop 是重复执行的 Core primitive，Retry 与 Iter 是其推进策略。每轮 body 可以是 Node 或 Orchestrator；策略读取正常结果已表达的业务状态，复杂判断仍由 Node 完成。

Loop 只让 Retry 与 Iter 共享重复调用 child 和 Round 生命周期机制，保留两者不同的数据推进策略。Each 继续作为独立的集合逐项语义，不归入 Loop；这种共享不将所有重复行为合并为万能循环。

Retry 决定继续时丢弃本轮结果，仍以原始业务输入执行下一轮；本轮 Output 不自动成为下一轮 Input。决定完成时暴露选定正常结果。次数约束、零次配置、默认次数和耗尽结果未在 v2.1 定案。

Iter 决定继续时，将选定正常 Output 作为下一轮状态。当前状态位置的改变不自动取得 imported Data 的所有权：原输入仍由原责任方管理；本 Loop 负责的旧状态在不再需要时清理。新旧状态指向同一实例时，不能重复销毁、重复拥有或使继续执行所需的状态失效。

Loop 默认不保存轮次历史；需要历史时显式建模为 Data。Iter 的 item stream、空序列和停止策略仍待设计。body 执行错误立即传播，停止后续轮次，不把上一轮状态冒充正常最终输出，也不触发技术重试。

## 13. Runtime 与 Execution

`runtime.execute(root, input)` 表达 Root 边界：Application 将 owned Input 移入本次 Execution，成功后取得 owned Output。方法签名为候选 API。

一次 Root Execution 只有一个业务 Data 所有权管理域。内部 Node／Orchestrator 调用经当前 ExecutionContext 的调用机制进入，不再次启动新 Root Execution，不另建业务值存储。Runtime 提供 Root 入口；业务顺序和控制决策由相应 Orchestrator 定义。

Runtime／ExecutionContext 提供共同的调用、数据、Scope、生命周期和错误传播机制，不集中实现 Flow、Match、Each、Loop 的具体控制决策。步骤顺序、分支选择、逐项遍历和状态推进分别属于相应 Orchestrator。

Root 是最终业务所有权移交给 Application 的边界。全部输出须先验证为可移交且互不重复的实例；多个逻辑引用指向同一实例时，在任何提取前整体拒绝，不隐式 Clone 或返回部分输出。`()` 输出不提取业务值。

## 14. 数据可见性与生命周期保证

子编排只能访问显式传入的数据；处于同一 Execution 不赋予全局读取权。导入数据不赋予 child 销毁责任。child 新产出且未导出、未合法保留或消费的数据，随其生命周期结束清理。

合法暴露给 caller 的输出在 child 结束后仍可使用；对应生命周期责任正确交接。Each 的借用 item 不能逃出来源有效期，Loop 的暂存状态不能通过重绑定 Definition 引用规避边界。父边界不能在存活 child 脱离其管理时结束。

这些是使用者可依赖的保证；Scope、运行时目标及 Export／Promote／Consume 的具体机制见 Core 与 Runtime 子文档。

## 15. 业务结果与错误

`Rejected`、`NeedsRetry`、`accepted: false` 等可作为正常 Data 表达业务结论。调用失败、外部服务异常和无法完成执行的运行状态属于错误通道；内部不变量破坏另须有明确诊断。具体 Rust 错误类型不在本文冻结。

执行错误默认停止当前路径并向上层传播，不自动跳过 Step／item、改选 branch、技术 Retry 或回滚外部副作用。正常结果也不因“业务不接受”自动变成执行错误；流程应按明确控制规则处理。

## 16. 类型安全

DataRef 保持强类型。Node 的 `&A` 参数对应编排时的 `DataRef<A>`；多个参数按声明位置对应。相同类型的不同 DataRef 不合并为“按类型取一个值”。

能在 Definition 构建阶段可靠判断的类型、数量和归属错误应尽早拒绝；Runtime 仍验证内部目标、生命周期和 erased representation。框架不能把正常业务接线退化为运行时类型猜测，也不承诺所有身份或所有权冲突均可编译期判断。

## 17. 组合规则

| 组合位置 | 合法 child |
| --- | --- |
| Flow Step | Node 或 Orchestrator |
| Match branch | Node 或 Orchestrator |
| Each body | Node 或 Orchestrator |
| Loop body | Node 或 Orchestrator |

例如 Loop 可包含 Match，Match 的 branch 可包含 Each，Each 的 body 可包含 Flow。父级通过 Signature 与明确连接使用 child，不依赖 child 的内部步骤；每层仍须保持数据可见性、生命周期和错误传播规则。Node 不隐藏 SRFlow 子编排。

## 18. 公开 API 状态

稳定的架构语义包括 Data、DataRef、Node／Orchestrator 分工、四类 Orchestrator、Node 借用输入、Root owned 输入输出、显式连接和顺序执行。

`flow.input(...)`、`flow.then(...)`、`flow.output(...)`、`runtime.execute(...)` 是表达这些语义的候选 API。普通函数、结构体 Node 与异构 Step 的 Probe 结果支持其可行性，不冻结方法返回类型或参数数量上限。

Node／Orchestrator 精确 trait、宏、async Future 的 Send／Sync 边界、`Arc<dyn Node>`、目标包表示、错误 API 与 `()` Flow Input 未冻结。异步执行不得自行改变顺序；多线程约束须由后续 API／实现验证明确。

## 19. 扩展原则

未来能力须由真实流程需求证明，并保持 Core 的类型、数据来源、所有权与生命周期规则。新增 Orchestrator 须有明确 Signature、控制语义和错误行为，不因接入而给 Node 开放内部编排权。

Bundle 的多独立输出不能退化为字段级引用；parallel Each 必须保持唯一责任及 child join；通用 Collection 必须定义借用和 item 生命周期。这里只规定进入 Core 的门槛，不承诺这些能力已支持。

## 20. 性能原则

流程连接和只读复用不隐式 Clone Data；Node 输入、Each item 使用借用，Root 输出移交 owned 值。业务 Node 可为业务目的显式产生独立值，但不能以此掩盖框架默认复制。

内部类型擦除、查表与存储选择优先保证正确性和可审计性，不因假想性能问题引入 unsafe 或破坏执行语义；优化依据实测瓶颈，不在本文给出未经验证的性能承诺。

## 21. v2.0 → v2.1 的架构迁移

| 实际 v2.0 规定或开放边界 | v2.1 规定与迁移影响 |
| --- | --- |
| Executable 统一承载叶子与组合单元 | Node 与 Orchestrator 分离；移除共同业务执行协议的要求 |
| Ref 与独立 Binding 接线 | DataRef 显式连接；Binding 删除，旧装配与转换逐项迁移 |
| Binding 字段投影 | 字段级 DataRef 不支持；独立业务值由 Node 产生 |
| Input／Output 的统一执行契约，所有权实现待定 | Node 借用输入、新 owned 输出；内部 Orchestrator 传目标，Root 移交 owned 值 |
| 内部 child 再经 Runtime 的统一入口 | Root 创建 Execution；内部递归使用当前 ExecutionContext |
| 值存储、Clone／Arc／Borrow 策略开放 | 一次 Execution 的唯一所有权域、无默认隐式 Clone、Scope 生命周期明确 |
| Flow 一个输入契约 | 显式支持单 Data 或多 Data 位置；多位置连接与旧结构装配需复审 |
| Retry／Iter 独立控制单元 | Loop 的两种策略；旧次数、耗尽及 Iter item 序列规则不自动迁移 |
| Ref 的 Flow 归属 | DataRef 的 Definition 身份与运行时实例分离，跨边界显式传递 |

v2.0 的顺序语义、显式依赖、Node 叶子职责、Match 单一路由、Each 顺序与空集合、正常结果／错误分离继续保留。早期 Core 草案的 `Component → Data` 与 DataSource 移除属于命名整理；实际 v2.0 正文未正式定义这两个概念，不能将其误列为既有公开 API 的改名。

## 22. 删除与弃用的概念

v2.1 Core 不保留 Binding、作为叶子与组合单元总协议的 Executable、字段级 DataRef 投影，或独立底层 Retry／Iter primitive。早期 DataSource 草案也不成为公开 Core 抽象。

旧构造器、宏和示例如需兼容，应通过单独评审的兼容层处理；兼容形式不能恢复已取消的 Core 语义。旧文档的 Retry 默认次数／耗尽规则和 Iter item stream 规则须重新审定，不能凭旧名称直接继承。

## 23. 规范依据与验证证据

规范依据为 [Core Design v0.1](SRFlow_Core_Design_v0.1.md) 与 [Runtime Implementation Design v0.1](SRFlow_Core_Runtime_Implementation_Design_v0.1.md)，按 §1 的职责协作。

[Compile Probe 计划](SRFlow_Core_Compile_Probe_v0.1.md) 与 [第一轮结论](SRFlow_Core_Compile_Probe_Results_v0.1.md) 是可行性证据。P01～P07 在限定范围内均 PASS，证明局部机制在 stable、safe Rust 下可行；它们不充当规范，也不证明完整 Runtime 已整合。直接 Consume 和无 Ref-binding 的 Promote 等后续实现选择仍须重跑相应整合正反例。

## 24. 当前开放问题

尚未冻结的语义问题是 `()` Flow Input、Retry 次数／耗尽规则、Iter item stream 与终止策略。尚未冻结的实现／API 问题是 Node／Orchestrator trait、async Send／Sync、目标包与错误 API、独立 Definition 组合的归属检查及宏形式。

Bundle、parallel Each、通用 Collection 等是后续扩展，不列入当前能力承诺。已经规定的 Node 借用边界、唯一执行数据域、DataRef 固定身份、Scope 隔离与 Root 重复提取拒绝不因开放项而重新变成可选语义。

## 25. 审定与规范切换记录

规范切换完成于 2026-10-03，核对如下：

1. 本文、Core Design 与 Runtime Implementation Design 已审定；三条规范性补充已纳入本文，状态与相互引用统一。
2. P01～P07 的局部 PASS 记录保留；直接 Consume、无 Ref-binding Promote 与完整调用链的整合验收要求仍明确保留。
3. [AGENTS](../AGENTS.md) 与[任务入口](tasks/README.md) 已引用三层 v2.1 规范；T01～T08 和 G1～G3 保留为 v2.0 历史验收，不转换为 v2.1 的完成记录。新实现阶段须先拆分和审定详细任务书。
4. v2.0 已标为 superseded，作为历史设计与代码迁移参考。

设计规范先于正式 Runtime 实现生效；代码仍须按新规范重写并通过整合验收。规范切换及空工程检查不等于 Runtime 实现验收。后续只有明确的新需求或真实实现反例触及规范时，才重新评审相应条款。
