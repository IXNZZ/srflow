# AGENTS.md — SRFlow 独立工程协作约定

本仓库独立维护 System Runtime Workflow（SRFlow）的设计文档、Rust 代码、测试和示例。它不是 SES 仓库的子工程；SES 案例是设计验证背景，不构成构建或测试依赖。

## 权威顺序

遇到冲突时，按以下顺序判断：

1. 用户当前明确指令；
2. 本文件；
3. [`docs/SRFlow_Design_v2.1.md`](docs/SRFlow_Design_v2.1.md) 中的总体架构与规范性语义；
4. [`docs/SRFlow_Core_Design_v0.1.md`](docs/SRFlow_Core_Design_v0.1.md) 中的 Core 语义；
5. [`docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md`](docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md) 中的内部实现基线；
6. [`docs/tasks/README.md`](docs/tasks/README.md) 中的当前任务边界与验收顺序；
7. 当前经用户审定的详细任务书；
8. 现有代码、测试、示例和历史 Compile Probe 惯例。

规范已于 2026-10-03 切换到 v2.1。v2.0、T01～T08 和 G1～G3 是旧设计及历史验收依据，不授予新模型合规结论。三层设计未规定的精确 API／实现选择由任务书落实；若文档冲突，先提交具体证据评审。

具体 Rust 实现可以调整，但不得以实现便利为由静默改变设计语义。若发现任务书与上位设计冲突，停止冲突部分，提交具体证据供评审；不要自行修改设计文档来迁就代码。

用户已确认从空工程重写 Core。旧实现、测试和示例已清理，2026-10-03 的清理基线仅为 `src/lib.rs` 与空的 `src/core/mod.rs`；后续任务在同一个 crate 中逐步建立新能力。旧逐文件盘点和渐进迁移要求已退出，历史任务记录与独立 Probe 保留其原验证范围。

## 单任务执行与交接

新任务体系正在按[任务规范草案](docs/tasks/SRFlow_Task_Protocol_v2.1.md)和[任务规划草案](docs/tasks/SRFlow_Task_Plan_v2.1.md)重建。两份草案须先经用户审定，当前执行仍遵守下列约定；任务入口负责标明当前准备状态。

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
- Flow 负责顺序与显式数据连接，必须声明 Output Signature 才是完整 Orchestrator。显式 `()` Output 不等于尚未定义 Output。Binding 和字段级 DataRef 不进入 v2.1 Core；业务判断、转换与计算属于 Node。
- `DataRef<T>` 强类型、只读、可复用，Scope-local RefId 单赋值；跨组合边界须显式传递。业务 Data 来源为 Root Input／Node Output，Scope 不全局读取，不把 imported Data 变成 child-owned。
- Each 只 Consume 当前 ItemScope 的合法 owned 输出；Loop 用 Promote 更新运行时状态，不重绑父 RefId。DataId／ScopeId 单次 Execution 内不复用；Root 全部输出先验证，重复 DataId 在任何 take 前拒绝。
- 业务正常结论属于 Output；执行错误向上传播，不隐含重试、换分支或副作用回滚。Retry 次数／耗尽和 Iter item stream 等开放项不得擅自继承旧任务规则。
- 不提前加入日志、Trace、持久化、恢复、动态 Graph DSL、自动并行或技术故障重试。

## 文档与验证

- 新增公共能力时同步编写使用者能独立理解的 Rustdoc，包括用途、关键边界、错误行为和尽量可运行的用法。内部注释解释不变量与不明显的取舍，不逐行复述代码。
- 按任务总览维护循序渐进的 `examples/`；核心示例和自动测试必须能够离线运行，不访问真实模型、外部网络或用户正式数据。
- 用正常、边界和失败路径证明任务验收条件；需要编译期拒绝的错误连接要有可复核的编译失败证据。公共 API 应从独立使用者视角验证。
- 历史 Compile Probe 是可行性证据，不是正式实现模板；不要照搬其值存储、Clone、宏或类型擦除选择。
