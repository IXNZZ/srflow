# SRFlow

> 2026-10-24 T07 最终独立验收 R3：**COMPLETED**。[报告](docs/implementation_tasks_v2.2/V22_T07_REVIEW_R3_v0.1.md)关闭 R03，R01/R02/S01/S02 均已关闭；SRFlow v2.2 本轮 Public API 升级验收通过。[524 项验收指纹](docs/implementation_tasks_v2.2/V22_T07_ACCEPTED_SOURCE_SHA256.json)作为最终基线，25 项保护文件未变。未提交/推送/打标签/发布；以下准备、授权及执行者交接文字保留原时点。

System Runtime Workflow（SRFlow）是面向强类型业务流程的 Rust 执行与编排框架。本工程独立维护设计、实现、测试和示例，SES 等业务场景不构成构建依赖。

## 当前设计与实现

**SRFlow v2.2 本轮 Public API 升级已通过最终验收，T01～T07 均已登记 COMPLETED。** [T07 最终独立验收 R3](docs/implementation_tasks_v2.2/V22_T07_REVIEW_R3_v0.1.md)关闭 R01～R03；公开 Node/Factory、定义期自动依赖、Root owned 输出、output/next 与五种控制结构已接入，保留原有 Execution/Data Core 语义。最终证据为 766 lib + 72 integration + 1 doctest（4 原有 ignored）、155 UI/2 见证、六例与独立消费者，25 项保护文件未变；[524 项验收指纹](docs/implementation_tasks_v2.2/V22_T07_ACCEPTED_SOURCE_SHA256.json)为后续工程基线。T06 提交为 bce1a6b；T07 当前验收工作区尚未提交、推送或发布。

- [v2.2 总设计](docs/SRFlow_Design_v2.2.md)
- [v2.2 Public API SPEC](docs/SRFlow_v2.2_Public_API_SPEC_v0.1.md)
- [v2.2 Engine 设计增量](docs/SRFlow_v2.2_Workflow_Engine_Design_v0.1.md)
- [v2.2 使用示例](docs/SRFlow_v2.2_Public_API_Usage_Examples_v0.1.md)
- [文档升级清单、版本变化与未定项](docs/SRFlow_v2.2_Document_Upgrade_Report_v0.1.md)

v2.2 目标是定义期自动依赖、链式调用、运行期同步 Factory 和显式 output/next。Root 继续使用 Runtime::execute；不引入实验性公开 Workflow 或 Flow::build。自动连接最终形成固定 Ref 关系，运行期不按类型搜索 Data。

当前 Root 使用 `Runtime::execute` 的双参数 unit 定义闭包，通过 `flow.output` / `output_with` 声明输出，由调用方期望 owned Out 类型。`then` / `then_output` 自动解析输入，`then_with` / `then_with_output` 保留显式 Ref；`provide` / `provide_output` 与 `ref_of` 已公开。Chain/Retry、Choose 的分支和 Each Item Body 使用单参数 unit 构建闭包，以 `output` / `output_with` 声明输出；普通形式返回 Flow，取输出形式返回父域 RefShape。Iter Round 同样使用单参数 unit 构建闭包，以 `next` / `next_with` 声明推进及最终状态契约；`iter` 返回 Flow，`iter_output` 返回最终父域 RefShape，`current::<T>()` 在直接 Round 中取得完整有序 formal Current。Scope/Container 及执行算法保持原语义。

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

## v2.2 实施状态

[实施规划 v0.1](docs/SRFlow_v2.2_Implementation_Plan_v0.1.md) 已评审通过，保留 V22-T01～T07 顺序。[V22-T01](docs/implementation_tasks_v2.2/V22_T01_Definition_Sources_And_Declarations_v0.1.md) R1.2 已独立复审验收通过，R01～R04 均关闭，登记 COMPLETED，限定 Definition 基础范围；[复审 R1](docs/implementation_tasks_v2.2/V22_T01_REVIEW_R1_v0.1.md)记录独立验证及边界，[HANDOVER](docs/implementation_tasks_v2.2/V22_T01_HANDOVER_v0.1.md)保留实施记录，[任务入口](docs/implementation_tasks_v2.2/README.md)维护状态。T01 实现已入库 312cbe7；T02 R1.3 已限定范围验收通过并提交为 271a0d7；T03 已按[最终复审 R2](docs/implementation_tasks_v2.2/V22_T03_REVIEW_R2_v0.1.md)验收并登记 COMPLETED，已提交为 071cd43；T04 验收实现已提交为 cc0a315；T05 验收实现已提交 a027205；T06 已限定范围验收并提交 bce1a6b；T07 仅编写任务书，未授权实施，本轮未提交/推送/发布。

[V22-T02 详细任务书](docs/implementation_tasks_v2.2/V22_T02_Node_And_Factory_Internal_Integration_v0.1.md) 已登记 COMPLETED · R1.3，[独立验收](docs/implementation_tasks_v2.2/V22_T02_REVIEW_R2_v0.1.md)确认 R01～R03 全部关闭；[HANDOVER](docs/implementation_tasks_v2.2/V22_T02_HANDOVER_v0.1.md)保留实施记录。T02 完成内部接入；其后 T03 已完成公开 then/Runtime 切换，限定范围独立验收通过。

[V22-T03 任务书](docs/implementation_tasks_v2.2/V22_T03_Public_Node_And_Root_Integration_v0.1.md) 已登记 COMPLETED · 独立复审 R2；Root → Node/Factory → output → owned Result 最小闭环及调用迁移通过。验收代码现已提交为 `071cd43`，382 项指纹一致，见[验收基线](docs/implementation_tasks_v2.2/V22_T03_ACCEPTED_SOURCE_SHA256.json)。早期回滚、[A 准备评审](docs/implementation_tasks_v2.2/V22_T03_CHECKPOINT_A_REVIEW_R3_v0.1.md)及[Root执行补充](docs/implementation_tasks_v2.2/V22_T03_ROOT_MIGRATION_GUIDE_v0.1.md)保留历史；[T04 任务书](docs/implementation_tasks_v2.2/V22_T04_Chain_And_Retry_Integration_v0.1.md)已登记 COMPLETED · [独立复审 R2](docs/implementation_tasks_v2.2/V22_T04_REVIEW_R2_v0.1.md)，限定 Chain/Retry 四方法、声明交接及相应迁移；[源码指纹](docs/implementation_tasks_v2.2/V22_T04_ACCEPTED_SOURCE_SHA256.json)为后续基线。T04 已提交为 cc0a315；[交接说明](docs/implementation_tasks_v2.2/V22_T04_HANDOVER_v0.1.md)保留实施时点。

## 开发入口

先阅读 [AGENTS](AGENTS.md) 和当前已审定任务；T01～T06 已各自限定范围验收通过（T05 源码已提交 a027205，T06 源码已提交 bce1a6b）；T07 已按评审 R01～R07 修订（REVISED R1）并实施完成；独立复审 R1 的 R01～R03 已修订，登记 IMPLEMENTED · 待复审；不自动获得提交/发布授权。

现有工程工具链基线 Rust 1.97、edition 2024，常用离线检查：

```sh
cargo check --offline --all-targets
cargo test --offline
cargo clippy --offline --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --offline --no-deps
cargo run --offline --example node_chain_runtime
```

核心不绑定外部 executor；Data derive 使用本地宏 crate 及编译期依赖，futures 仅用于测试/示例。上列命令供正式代码开发使用；独立 Root Probe 只保留其限定证据范围；正式 T03 验收已独立运行上述套件及 UI/消费者检查，见[最终复审](docs/implementation_tasks_v2.2/V22_T03_REVIEW_R2_v0.1.md)。

## 仓库协作约定

- `docs/` 下新增设计与验收文档沿用本地管理，不整体纳入 Git；正式代码验证所需的脚本/清单可单独跟踪（当前为 T02 UI 驱动及两个清单），历史已跟踪文档保持原状。
- 每个任务完成后，对该任务的代码变更统一提交一次；提交、推送与发布仍须按用户明确授权和 AGENTS 执行。

历史记录（T03 准备时点）：T01 的 Definition 内部支撑已验收并入库 312cbe7；T02 R1.3 已限定范围验收并提交为 271a0d7（含原有根包 0.2.2 版本变更）。本轮随后只编写 T03 文档，没有修改 Rust 或测试，不推送/发布；v2.1 历史文档和 Probe 原始证据继续保留。

T07 材料：[任务书](docs/implementation_tasks_v2.2/V22_T07_Final_Regression_And_Acceptance_v0.1.md)（REVISED R1 · IMPLEMENTED 待复审）、[任务书评审归档](docs/implementation_tasks_v2.2/V22_T07_TASK_REVIEW/REVIEW_INPUT.txt)、[修订说明 R1](docs/implementation_tasks_v2.2/V22_T07_TASK_REVISION_R1_v0.1.md)、[交接](docs/implementation_tasks_v2.2/V22_T07_HANDOVER_v0.1.md)、[Q 索引](docs/implementation_tasks_v2.2/V22_T07_VALIDATION/V22_T07_Q_INDEX_v0.1.md)、[未覆盖](docs/implementation_tasks_v2.2/V22_T07_UNCOVERED_v0.1.md)、[覆盖盘点](docs/implementation_tasks_v2.2/V22_T07_COVERAGE_INVENTORY_v0.1.md)、[准备记录](docs/implementation_tasks_v2.2/V22_T07_PREPARATION.json)。旧报告及指纹保留各自时点。
