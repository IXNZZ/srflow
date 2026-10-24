# AGENTS.md — SRFlow 独立工程协作约定

本仓库独立维护 System Runtime Workflow（SRFlow）的设计文档、Rust 代码、测试和示例。它不是 SES 仓库的子工程；SES 案例是设计验证背景，不构成构建或测试依赖。

## v2.2 设计基线（2026-10-22）

[Design v2.2](docs/SRFlow_Design_v2.2.md)、[Public API SPEC v2.2](docs/SRFlow_v2.2_Public_API_SPEC_v0.1.md) 与 [Engine v2.2 设计增量](docs/SRFlow_v2.2_Workflow_Engine_Design_v0.1.md) 的主体设计已评审通过，用户已正式确认 Root 双参数闭包（Flow + Input RefShape）及调用方期望 owned 输出方案，并要求补齐两条原则后登记设计基线。v2.2 是后续 Public API/Definition 任务设计的现行依据；T01 Definition 基础、T02 Node/Factory 内部接入及 T03 公开 Node/Root 最小闭环已限定范围验收；[T03 最终复审 R2](docs/implementation_tasks_v2.2/V22_T03_REVIEW_R2_v0.1.md)关闭 R01～R06。公开 Node/Factory、ref_of/output、新 Root 与 Chain/Retry 已限定范围验收；T05 已按[最终独立复审 R2](docs/implementation_tasks_v2.2/V22_T05_REVIEW_R2_v0.1.md)限定范围验收，R01～R05 全部关闭，登记 COMPLETED；Choose/Each 新门面及 typed 声明衔接已接入，G05 保持审定。Iter 新门面、Next 与 G06 Current 已按 [T06 独立复审 R3](docs/implementation_tasks_v2.2/V22_T06_REVIEW_R3_v0.1.md)限定范围验收并登记 COMPLETED；完整 v2.2 最终验收仍由 T07 承担，未编写/启动 T07。

正式入口保持 Runtime::execute；不引入公开 Workflow、Flow::build、require<T>()或第二套依赖协议。链式语法只是在同一Flow上连续登记，后一步按自身Query解析输入。Data Core和控制执行算法不重做；旧规范与Probe各保留历史适用范围。两项已确认规则是自动Query重复类型拒绝、Choose混合未声明/unit输出拒绝。

[Root补充Probe](examples/SRFlow_Root_API_v22_Probe/RESULTS.md) 已完成限定验证；不等于完整独立回归。文档变化与审定记录见[升级报告](docs/SRFlow_v2.2_Document_Upgrade_Report_v0.1.md)。[Implementation Plan v0.1](docs/SRFlow_v2.2_Implementation_Plan_v0.1.md) 已由用户评审通过，已补入构建错误恢复安全、旧 unit 契约映射、T05 Choose 签名审定三项约束。[V22-T01](docs/implementation_tasks_v2.2/V22_T01_Definition_Sources_And_Declarations_v0.1.md) R1.2 已于 2026-10-22 独立复审通过，R01～R04 均关闭，登记 COMPLETED，限定本任务 Definition 基础范围。验收见[复审 R1](docs/implementation_tasks_v2.2/V22_T01_REVIEW_R1_v0.1.md)，实施见 [HANDOVER](docs/implementation_tasks_v2.2/V22_T01_HANDOVER_v0.1.md)；[上轮审查](docs/implementation_tasks_v2.2/V22_T01_REVIEW_v0.1.md)及[R1.2 修订](docs/implementation_tasks_v2.2/V22_T01_TASK_REVISION_R1_2_v0.1.md)保留历史。T01 实现已入库 312cbe7；T02 R1.3 已限定范围验收并依用户本次指令提交为 271a0d7。T03 已于 2026-10-23 限定范围独立验收，验收源码现已提交为 071cd43，382 项验收指纹一致；T04 已按[最终独立复审 R2](docs/implementation_tasks_v2.2/V22_T04_REVIEW_R2_v0.1.md)限定范围验收，登记 COMPLETED；T04 验收实现现已提交为 cc0a315；T05 验收实现已提交为 a027205；T06 已限定范围验收，源码仍未提交。

[V22-T02 详细任务书](docs/implementation_tasks_v2.2/V22_T02_Node_And_Factory_Internal_Integration_v0.1.md) 当前为 COMPLETED · R1.3，R01～R03 全部关闭，限定 Node/Factory 内部接入范围。验收见 [T02 R1.3 独立复审](docs/implementation_tasks_v2.2/V22_T02_REVIEW_R2_v0.1.md)，实施报告见 [HANDOVER](docs/implementation_tasks_v2.2/V22_T02_HANDOVER_v0.1.md)。[首轮审查](docs/implementation_tasks_v2.2/V22_T02_REVIEW_v0.1.md)、[R1.2 复审](docs/implementation_tasks_v2.2/V22_T02_REVIEW_R1_v0.1.md)与各轮修订说明保留历史；准备基线仍见 [T02 PREPARATION](docs/implementation_tasks_v2.2/V22_T02_PREPARATION.json)。T02 验收时公开签名仍为 v2.1；其后 T03 已完成公开 Node/Root 切换并独立验收。原有 Cargo 根包 0.2.2 版本变更已随 T02 代码提交，不代表发布。

[V22-T03 详细任务书](docs/implementation_tasks_v2.2/V22_T03_Public_Node_And_Root_Integration_v0.1.md) 已登记 **COMPLETED · 独立复审 R2**，限定公开 Node/Root 最小闭环与相应消费者迁移；验收见 [最终复审](docs/implementation_tasks_v2.2/V22_T03_REVIEW_R2_v0.1.md)，代码指纹见 [验收基线](docs/implementation_tasks_v2.2/V22_T03_ACCEPTED_SOURCE_SHA256.json)。独立验证 624 lib + 62 integration + 1 doctest、140 UI、2 见证及六个示例通过，R04 16 槽执行断言与 R05 证据登记均关闭。[V22-T04 任务书](docs/implementation_tasks_v2.2/V22_T04_Chain_And_Retry_Integration_v0.1.md)已登记 **COMPLETED · 独立复审 R2**，限定 Chain/Retry unit Body、普通/取输出、声明交接及相应迁移；[最终验收](docs/implementation_tasks_v2.2/V22_T04_REVIEW_R2_v0.1.md)关闭 R01～R04/S01，[源码指纹](docs/implementation_tasks_v2.2/V22_T04_ACCEPTED_SOURCE_SHA256.json)为后续基线。实测 649 lib + 62 integration + 1 doctest（2 个新增示意忽略）、25 定向、141 UI、2 见证及六例通过；T04 验收代码已提交为 cc0a315；该报告与交接保留原验收时点。Choose/Each 已在 T05 接入；Iter 新门面及公开 next/next_with 已在 T06 限定范围验收；[T04 交接](docs/implementation_tasks_v2.2/V22_T04_HANDOVER_v0.1.md)保留实施时点。早期回滚、任务书与 A 准备评审保留历史；[交接](docs/implementation_tasks_v2.2/V22_T03_HANDOVER_v0.1.md)、[Root迁移执行补充](docs/implementation_tasks_v2.2/V22_T03_ROOT_MIGRATION_GUIDE_v0.1.md)、[PREPARATION](docs/implementation_tasks_v2.2/V22_T03_PREPARATION.json)、[符号账本](docs/implementation_tasks_v2.2/V22_T03_PUBLIC_SYMBOL_LEDGER_v0.1.md)与[143项case账本](docs/implementation_tasks_v2.2/V22_T03_VALIDATION_CASE_LEDGER.tsv)保留各自来源与技术契约。


[V22-T05](docs/implementation_tasks_v2.2/V22_T05_Choose_And_Each_Integration_v0.1.md) 已登记 **COMPLETED · 独立复审 R2**，限定 Choose 四门面、Each 两门面、unit Body/显式输出、typed Collector 衔接及相关迁移；[最终验收](docs/implementation_tasks_v2.2/V22_T05_REVIEW_R2_v0.1.md)关闭 R01～R05，[467 项验收指纹](docs/implementation_tasks_v2.2/V22_T05_ACCEPTED_SOURCE_SHA256.json)作为后续基线。独立 678 lib + 62 integration + 1 doctest（4 个新增签名示意 ignored）、29 定向、145 UI、2 见证、六例与两账本通过；25 项受保护文件未变。源码已提交为 a027205（历史报告保留各自"未提交"时点）。[V22-T06 任务书](docs/implementation_tasks_v2.2/V22_T06_Iter_Next_And_Final_State_Integration_v0.1.md) 已通过[任务书评审 R1](docs/implementation_tasks_v2.2/V22_T06_TASK_REVIEW_R1/REVIEW_INPUT.txt)（结论「可授权」，R01／R02 已修订、S01～S09 已落实），[G06 同类型多槽 Current](docs/implementation_tasks_v2.2/V22_T06_CURRENT_REFS_DECISION_v0.1.md) 已审定采用候选 A 并登记 SPEC §16 G06 关闭记录；用户已授权实施（Iter Next 与最终状态契约）。

[V22-T06 最终独立复审 R3](docs/implementation_tasks_v2.2/V22_T06_REVIEW_R3_v0.1.md)已关闭 R01～R05，登记 **COMPLETED**，限定 Iter 五门面、Next/Current 声明与失败隔离及相关迁移；[492 项验收指纹](docs/implementation_tasks_v2.2/V22_T06_ACCEPTED_SOURCE_SHA256.json)为后续基线。独立 718 lib + 62 integration + 1 doctest（4 原有 ignored）、40 定向、155 UI/2 见证、六例及两账本通过；25 项受保护文件未变。T06 源码未提交/推送/发布；未编写/启动 T07，旧报告保留历史时点。

## 权威顺序

遇到冲突时，按以下顺序判断：

1. 用户当前明确指令；
2. 本文件；
3. [Design v2.2](docs/SRFlow_Design_v2.2.md) 的总体语义、[Public API SPEC v2.2](docs/SRFlow_v2.2_Public_API_SPEC_v0.1.md) 的目标契约与 [Engine v2.2设计增量](docs/SRFlow_v2.2_Workflow_Engine_Design_v0.1.md) 的衔接规则；
4. [`docs/SRFlow_Core_Design_v0.1.md`](docs/SRFlow_Core_Design_v0.1.md) 中的 Core 语义；
5. [`docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md`](docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md) 中的内部实现基线；
6. 经审定的[v2.2实施规划](docs/SRFlow_v2.2_Implementation_Plan_v0.1.md)及其[任务入口](docs/implementation_tasks_v2.2/README.md)（规划已通过，T01～T05 已各自限定范围验收并登记 COMPLETED；T05 最终复审 R2 关闭 R01～R05；T06 已按独立复审 R3 限定范围验收并登记 COMPLETED，T07 尚未编写/实施、不自动获得开工授权）；旧WE/V21入口仅保留原阶段边界；
7. 当前经用户审定的详细任务书；
8. 现有代码、测试、示例和历史 Compile Probe 惯例。

2026-10-22 起以 v2.2 为新 Public API/Definition 设计基线；v2.1 继续作为现有实现和历史验收依据，未变化的 Core 语义与已确认执行规则继承。2026-10-03 的 v2.1 规范切换保留历史记录。v2.0、T01～T08 和 G1～G3 是旧设计及历史验收依据，不授予新模型合规结论。上述设计未规定的精确泛型排列、支持桥布局及接口命名细节由经审定任务书落实，不改变目标能力与行为；若文档冲突，先提交具体证据评审。

具体 Rust 实现可以调整，但不得以实现便利为由静默改变设计语义。若发现任务书与上位设计冲突，停止冲突部分，提交具体证据供评审；不要自行修改设计文档来迁就代码。

用户已确认从空工程重写 Core。旧实现、测试和示例已清理，2026-10-03 的清理基线仅为 `src/lib.rs` 与空的 `src/core/mod.rs`；后续任务在同一个 crate 中逐步建立新能力。旧逐文件盘点和渐进迁移要求已退出，历史任务记录与独立 Probe 保留其原验证范围。

## 单任务执行与交接

v2.2 新任务以[已审定实施规划](docs/SRFlow_v2.2_Implementation_Plan_v0.1.md)及其[任务入口](docs/implementation_tasks_v2.2/README.md)推进。旧[任务规范草案](docs/tasks/SRFlow_Task_Protocol_v2.1.md)和[任务规划草案](docs/tasks/SRFlow_Task_Plan_v2.1.md)保留历史准备语境，不替代当前规划或授予开工权限。任务入口负责标明准备状态，执行遵守下列约定。

- 一次只执行一份已审定、已获用户授权的详细任务书。不得顺手实现后续任务或扩大本次范围。
- 执行前先阅读当前任务书所引用的设计章节；任务书未覆盖但实现将触及的规范性章节，也应补读并在交接中说明。
- 执行者负责实现、同步补齐本任务引入的公开 Rustdoc／示例、运行相关验证，并报告结果；不得自行将任务标为 `COMPLETED`、关闭阶段 Gate 或开始下一任务。
- 交接时说明：工作所基于的提交、修改文件、关键实现取舍、运行的验证命令及结果、未通过或未覆盖的项目。若有提交或 PR，也提供其标识。
- 复审由用户指定的审查者完成。未通过时继续修订同一任务；通过后由审查者更新任务状态与验收记录，再写下一份任务书。
- 提交、推送、合并、打标签和发布是不同操作；只按当前任务或用户的明确要求执行。

## 核心实现边界

- 本工程先保持单个 `srflow` crate；核心与可选扩展分层。LLM 等扩展不得成为核心的强制依赖。
- 实现使用 stable、safe Rust；异步执行保持 Flow、Each、Loop 的顺序语义，不自动并发。Future 的线程约束须按已审定任务和明确接口处理，不能用隐式 Clone 或转移输入所有权绕过借用边界。
- Runtime 只创建 Root Execution；内部 Invocation 经当前 ExecutionContext 进入，共享唯一 DataContainer。共同调用、数据与生命周期机制由 Runtime／Context 提供，各 Orchestrator 自行实现控制语义。
- Node 是业务叶子，只接收已有 Data 的只读借用，返回新的 owned Data 或 `()`；它不接触 DataRef 或 Runtime 内部，不编排 SRFlow child。Node 与 Orchestrator 分离，统一 Builder 不要求统一业务运行时 trait。
- Flow 负责顺序与定义期固定的数据连接，自动解析和显式 Ref 都落实为具体依赖。v2.2 的 Body 输出声明、无声明及 Root unit 例外按 SPEC 执行；显式 `()` 与未声明保持可区分。Binding 和字段级 DataRef 不进入 v2.1 Core；业务判断、转换与计算属于 Node。
- `DataRef<T>` 强类型、只读、可复用，Scope-local RefId 单赋值；跨组合边界须有明确 Import/输出映射，即使调用侧使用自动接线。业务 Data 来源为 Root Input／Node Output，Scope 不全局读取，不把 imported Data 变成 child-owned。
- Each 只 Consume 当前 ItemScope 的合法 owned 输出；Loop 用 Promote 更新运行时状态，不重绑父 RefId。DataId／ScopeId 单次 Execution 内不复用；Root 全部输出先验证，重复 DataId 在任何 take 前拒绝。
- 业务正常结论属于 Output；执行错误向上传播，不隐含重试、换分支或副作用回滚。Retry/Iter预算与耗尽按v2.2继承的已确认规则，不将早期历史开放项或旧任务行为重新带入。
- 不提前加入日志、Trace、持久化、恢复、动态 Graph DSL、自动并行或技术故障重试。

## 文档与验证

- 新增公共能力时同步编写使用者能独立理解的 Rustdoc，包括用途、关键边界、错误行为和尽量可运行的用法。内部注释解释不变量与不明显的取舍，不逐行复述代码。
- 按任务总览维护循序渐进的 `examples/`；核心示例和自动测试必须能够离线运行，不访问真实模型、外部网络或用户正式数据。
- 用正常、边界和失败路径证明任务验收条件；需要编译期拒绝的错误连接要有可复核的编译失败证据。公共 API 应从独立使用者视角验证。
- 历史 Compile Probe 是可行性证据，不是正式实现模板；不要照搬其值存储、Clone、宏或类型擦除选择。
