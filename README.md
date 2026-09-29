# SRFlow

System Runtime Workflow（SRFlow）是一个以强类型 Flow 为中心的 Rust 执行框架。本仓库独立维护其设计、实现、测试和示例。

T01 与 T02 已通过复审。仓库是一个可编译的 `srflow` library crate，提供统一异步执行基础 `Runtime`、`Executable`、`Node`，以及最小 Flow（`FlowBuilder`、`Flow`、`Ref`：按声明顺序编排异构 child，并用整值 `Ref` 连接数据）。字段投影、多值组合、命名结构装配与四种控制型 Executable 仍在后续任务中。

```
cargo test --all-targets                  # 测试
cargo run --example node_only             # 普通使用者：只实现 Node
cargo run --example composite_executable  # 扩展者：组合型 Executable 经 Runtime 调用 child
cargo run --example basic_flow            # 基础 Flow：Ref 复用与显式 Output
cargo run --example subflow               # SubFlow：Flow 作为另一个 Flow 的普通 child
```

示例与文档测试用 `futures::executor::block_on` 驱动异步代码；`srflow` 本身不依赖任何 executor，使用者需要在自己的项目里选择并添加一个（本仓库的开发依赖不会随 `srflow` 提供）。

- [规范性设计文档](docs/SRFlow_Design_v2.0.md)
- [开发任务总览](docs/tasks/README.md)
- [仓库协作约定](AGENTS.md)

设计文档中的 SES 场景用于验证框架语义；本工程不依赖 SES 仓库。详细任务按总览逐项编写、执行和复审。
