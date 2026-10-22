# SRFlow

System Runtime Workflow（SRFlow）是面向强类型业务流程的 Rust 执行与编排框架。本工程独立维护设计、实现、测试和示例，SES 等业务场景不构成构建依赖。

## 当前设计与实现

**v2.2 主体设计已评审通过，并经用户确认登记设计基线；当前正式代码仍是已交付的 v2.1 实现。** 本次只更新 Public API、Definition 构建及相关文档，不实施框架重构。

- [v2.2 总设计](docs/SRFlow_Design_v2.2.md)
- [v2.2 Public API SPEC](docs/SRFlow_v2.2_Public_API_SPEC_v0.1.md)
- [v2.2 Engine 设计增量](docs/SRFlow_v2.2_Workflow_Engine_Design_v0.1.md)
- [v2.2 使用示例](docs/SRFlow_v2.2_Public_API_Usage_Examples_v0.1.md)
- [文档升级清单、版本变化与未定项](docs/SRFlow_v2.2_Document_Upgrade_Report_v0.1.md)

v2.2 目标是定义期自动依赖、链式调用、运行期同步 Factory 和显式 output/next。Root 继续使用 Runtime::execute；不引入实验性公开 Workflow 或 Flow::build。自动连接最终形成固定 Ref 关系，运行期不按类型搜索 Data。

当前正式 Public API 是 Runtime::execute 接收定义闭包和 owned Root Input；闭包当前通过返回 RefShape 声明输出，Node 调用当前需要显式 Ref。Node/Query、Chain/Choose/Each/Retry/Iter 与现有 Scope/Container 已建立。新文档中的 then 单参数、provide、ref_of、output、next 等目标接口尚未正式实现。

[WE-T01～WE-T06](docs/workflow_engine_tasks/README.md) 已按各任务最新报告完成限定范围验收；原 V21/G21 历史记录保留其适用范围。旧 FlowBuilder/Match/Loop 门面和早期“从空工程重写”是历史过程，不是 v2.2 再次重写 Core 的路线。

## 设计与证据入口

| 材料 | 用途 |
| --- | --- |
| [v2.1 总设计](docs/SRFlow_Design_v2.1.md)、[v2.1 Public API SPEC](docs/SRFlow_v2.1_Public_API_SPEC_v0.1.md) | 已审定的历史/实现设计基线；不覆盖 v2.2 新目标 |
| [v2.1 Engine 设计](docs/SRFlow_v2.1_Workflow_Engine_Design_v0.1.md) | 已建立的静态与执行机制及原阶段证据 |
| [Core Design](docs/SRFlow_Core_Design_v0.1.md)、[Core Runtime 实现设计](docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md) | 数据身份、所有权和调用生命周期基础，保留原版本 |
| [2026-10-21 Public API Probe](examples/SRFlow_Auto_API_v21_Probe/RESULTS.md) | 新编排方向的限定可行性；其中公开 Workflow 是已被后续讨论排除的实验选择 |
| [WE 任务入口](docs/workflow_engine_tasks/README.md)、[历史任务入口](docs/tasks/README.md) | 各阶段实际验收状态与未覆盖边界，不因设计升级自动改变 |

SPEC 是目标契约，Probe 是证据，源码是当前实现。任何材料中的“通过”都须保留原验证范围；v2.2设计基线不等于新API的正式实施与完整回归验收。

[2026-10-22 Root 补充 Probe](examples/SRFlow_Root_API_v22_Probe/RESULTS.md) 已补双参数 Root 与期望 owned 类型推导证据。自动 Query 重复类型及 Choose 混合缺省/unit 输出的拒绝规则已由用户确认，用户已正式采用双参数Root与期望owned输出方案；链式结构语义和不引入require<T>()原则已补齐，设计审定不等于实施验收。

## v2.2 实施准备

[实施规划 v0.1](docs/SRFlow_v2.2_Implementation_Plan_v0.1.md) 已评审通过，保留 V22-T01～T07 顺序，并补入错误恢复、旧 unit 映射和 Choose 签名审定约束。当前 [V22-T01 详细任务书](docs/implementation_tasks_v2.2/V22_T01_Definition_Sources_And_Declarations_v0.1.md) 为待评审草案，仅涉及 Definition 内部基础；[任务入口](docs/implementation_tasks_v2.2/README.md)记录后续状态。尚未实施正式代码。

## 开发入口

先阅读 [AGENTS](AGENTS.md) 和当前已审定任务；v2.2 设计基线与实施规划已通过，当前进入 T01 详细任务书评审，尚无已授权实施的 V22 任务。

现有工程工具链基线 Rust 1.97、edition 2024，常用离线检查：

```sh
cargo check --offline --all-targets
cargo test --offline
cargo clippy --offline --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --offline --no-deps
cargo run --offline --example node_chain_runtime
```

核心不绑定外部 executor；Data derive 使用本地宏 crate 及编译期依赖，futures 仅用于测试/示例。上列命令供正式代码开发使用；此前独立 Root Probe 只检查其明确目标，本轮仅维护规划/任务文档，没有重跑正式工程套件。

## 仓库协作约定

- `docs/` 目录下的内容不提交到 Git 仓库。
- 每个任务完成后，对该任务的代码变更统一提交一次；提交、推送与发布仍须按用户明确授权和 AGENTS 执行。

当前只修订 v2.2 实施规划并编写 T01 任务书，不修改生产 Rust、Probe 证据、提交或发布。v2.1 历史文档和 Probe 原始证据继续保留。
