# SRFlow

System Runtime Workflow（SRFlow）是一个以强类型 Flow 为中心的 Rust 执行框架。本仓库独立维护其设计、实现、测试和示例。

> 当前总规范为 [SRFlow Design v2.1](docs/SRFlow_Design_v2.1.md)，已于 2026-10-03 完成规范切换。以下代码用法和 T01～T08 验收对应现存 v2.0 实现，是迁移起点；v2.1 Runtime 尚待按[新任务入口](docs/tasks/README.md)实施和整合验收。

T01～T08 已通过复审，G1／G2／G3 已分别完成独立验收。仓库是一个可编译的 `srflow` library crate，提供统一异步执行基础 `Runtime`、`Executable`、`Node`，最小 Flow（`FlowBuilder`、`Flow`、`Ref`），Binding（整值读取、字段投影、2～8 元 tuple、命名结构装配，由 `consume`、`field!`、`bind!` 表达），以及四种控制型 Executable：`Retry`（正常业务 Output 驱动的有限重做）、`Match`（依据已有路由值执行唯一分支）、`Each`（按顺序逐项执行并收集结果）与 `Iter`（携带上一轮状态的顺序推进）。四者可以在同一条流程里组合使用。

## 首次使用（最短路径）

1. `cargo test --all-targets` 跑通全部离线测试；
2. 只实现 `Node` 并交给 `Runtime` 执行：`cargo run --example node_only`；
3. 需要编排顺序与数据连接时用 `FlowBuilder`／`Ref`／Binding：`cargo run --example basic_flow`；
4. 需要重做、路由、逐项处理或跨轮推进时加控制型 Executable：`retry`／`match`／`each`／`iter`；
5. 想看它们在同一条流程里如何组合：`cargo run --example story_workflow`。

（示例与文档测试用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor，使用者需要在自己的项目里选择并添加一个——本仓库的开发依赖不会随 `srflow` 提供。）

## 示例（按学习顺序）

```
cargo test --all-targets                  # 测试
# 基础：执行协议与数据连接
cargo run --example node_only             # 普通使用者：只实现 Node
cargo run --example composite_executable  # 扩展者：组合型 Executable 经 Runtime 调用 child
cargo run --example basic_flow            # 基础 Flow：Ref 复用与显式 Output
# 数据装配：Binding 与 SubFlow
cargo run --example binding_projection    # Binding：非 Clone 根 + 字段投影 + tuple
cargo run --example binding_assembly      # Binding：四来源命名装配与嵌套装配
cargo run --example subflow               # SubFlow：Flow 作为另一个 Flow 的普通 child
# 控制语义：四种控制器
cargo run --example retry                 # Retry：生成→检查→重做、早停、耗尽、作为 Flow child
cargo run --example match                 # Match：JudgeNode 产出路由值、tuple Binding 组装 (K, I)、default
cargo run --example each                  # Each：顺序逐项执行、Flow Body、非 Clone 元素、作为 Flow child
cargo run --example iter                  # Iter：跨轮状态推进、不变上下文保持、非 Clone 状态、作为 Flow child
# 端到端：四种控制器在同一条流程里组合
cargo run --example story_workflow        # 计划 → 路由 → 逐项加工 → 逐轮推进 → 最终结果（全离线 Fake）
```

## 仓库协作约定

- `docs/` 目录下的内容不提交到 Git 仓库。
- 每个任务完成后，对该任务的代码变更统一提交一次。

## 设计文档

- [当前总规范 v2.1](docs/SRFlow_Design_v2.1.md)
- [Core 语义基线](docs/SRFlow_Core_Design_v0.1.md)
- [Runtime 内部实现基线](docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md)
- [v2.0 历史规范](docs/SRFlow_Design_v2.0.md)
- [开发任务总览](docs/tasks/README.md)
- [仓库协作约定](AGENTS.md)

设计文档中的 SES 场景用于验证框架语义；本工程不依赖 SES 仓库。详细任务按总览逐项编写、执行和复审。
