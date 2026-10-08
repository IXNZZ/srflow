# SRFlow v2.1 Public API 独立 Probe

结论：**PASS WITH DOCUMENTED API REVISIONS**。外部契约见 [Public API SPEC v0.1](../../srflow/docs/SRFlow_v2.1_Public_API_SPEC_v0.1.md)，逐项证据和限制见 [RESULTS](RESULTS.md)。本工程为可复跑的验证产物，不是正式 SRFlow 实现。

日期：2026-10-08。实际工具链：Rust 1.97.1 stable / Cargo 1.97.1，edition 2024，safe Rust。执行核心不依赖第三方库；Data 派生通过本地 proc-macro crate 提供，使用 syn / quote 解析并生成空实现；futures 仅用于离线测试 / 示例驱动。全部检查离线完成，不访问真实模型、网络服务或用户业务数据。

在本目录执行：

```sh
cargo test --offline
cargo clippy --offline --workspace --tests --examples -- -D warnings
cargo fmt --all -- --check
cargo run --offline --example spec_minimal
```

编译拒绝样本由 compile_fail 测试直接调用当前 rustc 验证，诊断保存在 target/ui；需查看逐项拒绝可执行 `cargo test --offline --test compile_fail -- --nocapture`。Cargo.lock 固定派生与测试依赖。已有同一 Rust 工具链及缓存依赖时可直接复跑；本项目不自动安装工具链或依赖。

## 项目内容

| 路径 | 用途 |
| --- | --- |
| TASK.md | 用户对整个 Probe 到 SPEC 的授权与范围 |
| src/shape.rs | Data / Query / Node、自然 RefShape 与函数适配 |
| data-derive/ | `#[derive(Data)]` 的最小 proc-macro 实现；Probe 包名路径不冻结为生产布局 |
| src/flow.rs | 证明用的同步 Definition、顺序执行与六类编排操作 |
| src/error.rs | Control / Failure、Root 终止类别及动态错误链 |
| src/core/ | 原 Core 的独立快照；scope.rs 仅有两项标注扩展 |
| tests/query_signature.rs、shapes.rs、large_shapes.rs | 协议、推导、借用和 1～16 位置边界 |
| tests/composition.rs | Chain / Fragment / Each / Choose 与构建拒绝 |
| tests/control.rs | 次数、状态、最近捕获、错误与耗尽 |
| tests/lifecycle.rs | 真实 Data drop、部分结果清理、取消与 owned alias |
| tests/ses.rs | S08 完整嵌套、S10 两套 SES 流程和顺序 gate |
| tests/data_derive.rs、tests/zero_root.rs | 用户增补：Data 派生、泛型 / 字段约束与零输入 Root |
| tests/ui/、tests/compile_fail.rs | 18 个原候选反例 / 编译期拒绝样本 |
| examples/spec_minimal.rs | SPEC 中的完整最小用法 |

## 两项 Public 调整

1. Function Node 仍使用 Query<(&A, &B)>；Struct Node 使用 Input / Output 声明与 Query<&Self::Input>（或 Query<&(A, B)>）。原 GAT 候选的直接 tuple-of-references 方法签名被 E0195 拒绝。新形式不需要手写 invocation lifetime，不合成 owned 输入 tuple。
2. 非 unit 业务叶子采用一次 Data 声明（`#[derive(Data)]` 或手写 impl），以保持普通 `then` 自然返回 Ref<T> 或 `()`。默认 tuple 作为连接 Shape；单组合值使用业务 struct 或 Box<(A, B)>。未注册外部类型不能宣称直接接入。

用户于 2026-10-08 确认其余 SPEC 并增补 Data 派生、零输入 Root。现支持 `runtime.execute(|flow, ()| flow.then(&load_data, ()), ()).await`。零输入用 `()` 表示零个位置，无占位 Data；原零输入 Root 拒绝样本已转为正向执行证据。

## 与原 Core 的关系

快照来源提交 `f9b6e2054b89e6295bc21766313aa8197d2e2ba2`。所有普通快照文件保持原内容；scope.rs 添加 `consume_item_group_probe` 和 `promote_group_probe`，分别验证多输出 Each / 多状态 Iter 的整组收口，保留原所有权不变量。详见 [Core 来源记录](CORE_PROVENANCE.md)。

实际执行使用快照的 DataContainer / ScopeCoordinator，而非另一套测试值仓库。原 ExecutionContext / Invocation / Orchestrator 不是本门面的正式调用入口；原控制协议也未原样复用。完整正式接入、故障诊断、性能与内部 IR 仍属于后续工作。

快照内的原 cfg(test) 验收模块不作为本 Probe 的测试集；Cargo 的 lib.test=false 明确避免将旧测试误算为新接口证据。当前证据全部来自独立消费者测试。本工程不修改原 srflow 源码，不改变 V21 / G21 状态，不提交或发布。
