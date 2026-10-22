# SRFlow v2.2 Root API 最小补充 Probe

日期：2026-10-22。**限定范围通过，方案待设计复审；不是正式实现。**

只回答 O01/O02：保留 `Runtime::execute(closure, owned_input)`，Root closure 保留 Flow 和 Input RefShape 参数、返回 unit；Root 在 Body 内声明 output，调用方期望的 owned Out 选择既有类型化提取能力。

本工程直接修改隔离副本中现有的 `src/core/engine/runtime.rs::Runtime::execute`，crate 根导出的仍是该 Runtime，不是另建一个公开 Runtime 包装对象。新 Flow 对接现有 Builder，真实执行继续走原 Root Context/Scope/Container/提取函数。**没有公开 Workflow 或 Flow::build**。

- [结果、限制与证据](RESULTS.md)
- [可运行最小入口](examples/root_minimal.rs)
- [17 项运行检查](tests/root_api.rs)
- [独立消费者编译检查](tests/ui_probe.rs)：6 拒绝样本＋1 编译并运行正例
- [源码差异](BASELINE_DELTA.patch)、[来源指纹](BASELINE_SHA256.json)、[源码审计](evidence/source_audit.json)

在本目录离线复跑：

```sh
cargo test --offline
cargo run --offline --example root_minimal
cargo clippy --offline --lib --test root_api --test ui_probe --example root_minimal -- -D warnings
cargo fmt --all -- --check
```

Rust/Cargo 1.97.1 stable、edition 2024、safe Rust，依赖沿用原锁文件和本地 Data derive。复制源码来自上轮 Auto API Probe 的 Core 快照并对照正式基线；仅保留 Root 必需的自动 Node、Provide、显式声明与 Chain 支持。未重构或补验其他控制结构。旧库测试/旧 doctest 依赖旧 API，已用 lib.test=false/doctest=false 排除，不把旧套件算入本次证据；clippy 也只检查明确列出的本轮目标。

正式源码和上轮 Probe 证据均保持原状。本次不生成实施 Task，不提交或冻结 SPEC。
