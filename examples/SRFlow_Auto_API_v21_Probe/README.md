# SRFlow v2.1 自动依赖 Public API 重构 Probe

日期：2026-10-21。结论：**在本 Probe 验证范围内可行，需要少量内部类型适配与 Definition 构建调整，不需要改变 Data Core 或控制执行算法。**

本工程是当前 `srflow` 的隔离源码副本。新增 API 位于 `srflow::probe`，旧 API 保留以运行已有库测试。它不是正式重构、生产接口或新的规范冻结。

- [结果与可行性矩阵](RESULTS.md)：P01～P10、接口差异、实施建议、未定项。
- [最小可运行示例](examples/auto_minimal.rs)：统一编排函数、链式调用、自动注入、显式输出、构建一次执行两次。
- [任务边界](TASK.md)：用户给定语义及本次授权范围。
- [来源及差异](BASELINE_DELTA.patch)：相对于基线已有文件的全部修改；新增 API 见 [probe_api.rs](src/core/engine/probe_api.rs)。
- [原工程指纹](BASELINE_SHA256.json)、[来源提交](BASELINE_COMMIT.txt)、[源码审计](evidence/source_audit.json)。

在本目录复跑（已验证 Rust/Cargo 1.97.1 stable，edition 2024）：

```sh
cargo test --offline
cargo test --offline --test ui_probe -- --nocapture
cargo run --offline --example auto_minimal
cargo clippy --offline --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

依赖与本地 Data derive 均保留原 Cargo.lock；已有缓存依赖可离线复跑。这里保持包名 `srflow`，使现有 derive 及库测试原样工作；独立 `[workspace]` 保证不加入上层正式工程。

当前证据：539 项库测试（536 项既有＋3 项新结构检查）、32 项新消费者测试、1 个 UI 验证入口（13 个编译拒绝样本＋1 个编译并运行的正例）、1 个既有文档示例全部通过。原工程另行复跑 536 库＋62 集成＋1 文档示例全部通过。不要把原工程的 62 项集成测试算入隔离副本的测试数量。

编译诊断每次保存到 `target/auto_ui/`；本次诊断副本见 [evidence/ui](evidence/ui)。本次完整日志见 [evidence](evidence)。不访问网络、模型服务或用户业务数据；没有修改正式源码、设计规范或任务状态，没有提交或发布。
