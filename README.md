# SRFlow

System Runtime Workflow（SRFlow）是面向强类型业务流程的 Rust 执行与编排框架。本工程独立维护其设计、实现、测试和示例。

> 当前总规范为 [SRFlow Design v2.1](docs/SRFlow_Design_v2.1.md)。实施路线是在同一 crate 中从空工程重写 Core；V21-00～V21-08 已验收为 **COMPLETED**。[G21-A](docs/tasks/G21_A_Foundation_Review.md)／[G21-B](docs/tasks/G21_B_Typed_Flow_Review.md) 为 PASS；V21-08 的 R1～R6 全部关闭，262 项测试、44 个编译负例及六项工程检查通过，见[最终验收记录 §19](docs/tasks/V21_08_RESULTS.md#19-最终独立复审与验收2026-10-05)。后续阶段状态和任务书以[任务入口](docs/tasks/README.md)为准。

## 当前工程状态

旧实现、测试和示例已清理。V21-01 在非公开的 `src/core/` 内交付了 Execution 身份（`DataId`／`ScopeId` 在单次 Execution 内单调分配且不复用）、异构 owned `DataContainer` 与短期只读借用。V21-02 在同一内部层已实现 Definition `RefId` 来源、Scope registry（`refs`／`owned`、Active／Finalizing／Closed tombstone）、显式 Import、整组 Export 预检与责任交接，以及正常／同步失败退出清理；责任链、数据保护、输出契约、祖先保留与失败清理诊断已验收通过。V21-03 已验收：`CollectorId` 身份与分配、控制状态登记与状态来源导入、无父 Ref 绑定的 Promote、受控 pending 回收、container 内 collector 建构区与内部移动、ItemScope 直接 Consume、collector 完成与最终输出一次性绑定，以及随控制器退出的状态／collector 清理。

V21-04 已验收内部单 Context／Invocation 与三类退出设施：真实异步输入借用、建立失败终止、清理／控制权限、原始错误及取消定位、不可恢复终止、frame 关系检查，以及提交与分路径取消析构证据。V21-05 已验收内部强类型接线与双路径分派：`DataRef<T>` 逻辑位置句柄、`OutKind` 输出分类（数据／显式 unit／两个异构位置）、五种接线 Marker、业务 Node 协议（函数 item 的 HRTB 与结构体／`Arc<具体 Node>` 的单一短生命周期 boxed Future）、独立 Orchestrator 协议与 child-local 端口／输入 pack、异型 `CallSite`／`Step` 保存、Definition 声明表与 checked 整组分配，以及基础执行样本。普通 `Result<()>` 函数的构建期拒绝已有证据；任意编排体 Data 注入、Signature 关联、零输入 Arc、叶子失败终止和原子性／取消证据缺口均已关闭。V21-06 已验收内部完整 `FlowBuilder<I>`／`Flow<I, K>`：显式完成态、单／双非空输入、unit／单 Data／两个独立输出位置、完成输出整组类型校验、真实 SubFlow Import／Export 与顺序执行、定义复用及错误／取消清理。Root 测试驱动只观察并按空声明收口，尚不移交 owned 输出。

V21-07 已验收内部完成态 Match：已有路由 Data 与显式业务输入、函数／结构体／Arc Node 或完整 Flow branch、三种共同输出、不可变分支登记与来源校验、单一路由及 default、真实 BranchScope 与两层 Export、imported alias、错误终止及 Future 取消清理。未选 branch 不创建 Scope 或执行业务；子调用失败不改选分支。

V21-08 已验收内部 Each：显式集合与可选 shared 输入、CollectionItem／ItemScope cap、真实 Node 或完成态 Flow body、按项顺序执行与直接 Consume、collector 一次完成并 Export 最终 Vec、imported 输出拒绝、正常错误与 Future 取消清理。Each 会话核对当前实际 Definition，构建失败保留合法状态；状态收口和诊断观察的验收缺口均已关闭。当前范围为单 owned 输出、可选一个 shared、单线程顺序执行。

当前 crate 仍**没有任何公开 API**：`core` 不对外导出，业务侧无法按整数构造 ID、取得存储入口或访问 Scope 内部。公开 Node／Orchestrator／Flow／Match／Each API、真实 Loop 与完整 Root 移交由后续任务交付；当前内部能力的验收不等于公开入口或完整 Runtime 已完成。

历史 T01～T08 和 G1～G3 的通过记录对应旧 v2.0，不表示新实现完成。[P01～P07 Probe](docs/SRFlow_Core_Compile_Probe_Results_v0.1.md) 是局部可行性证据，正式调用链仍需重新验收。

## 开发入口

先阅读 [AGENTS](AGENTS.md)、[任务入口](docs/tasks/README.md)和当前经审定的详细任务书，再阅读任务涉及的设计章节。当前开发工具链基线为 Rust 1.97、edition 2024；离线检查命令为：

```sh
cargo check --offline --all-targets
cargo test --offline --all-targets
cargo clippy --offline --all-targets -- -D warnings
cargo fmt --all -- --check
```

`tests/ui/` 下是编译负例夹具，不是 Cargo target，需要按各文件头部注释中的 `rustc` 命令单独编译并确认预期失败。

核心当前没有普通依赖。`futures` 仅作为开发依赖，用于后续离线测试和示例驱动异步代码；线程 bounds 与适配接口由对应任务落实。公开能力交付时同步补充可运行的用法和 Rustdoc。

## 仓库协作约定

- `docs/` 目录下的内容不提交到 Git 仓库。
- 每个任务完成后，对该任务的代码变更统一提交一次。

## 设计文档

- [当前总规范 v2.1](docs/SRFlow_Design_v2.1.md)
- [Core 语义基线](docs/SRFlow_Core_Design_v0.1.md)
- [Runtime 内部实现基线](docs/SRFlow_Core_Runtime_Implementation_Design_v0.1.md)
- [Probe 第一轮结论](docs/SRFlow_Core_Compile_Probe_Results_v0.1.md)
- [任务入口](docs/tasks/README.md)
- [v2.0 历史规范](docs/SRFlow_Design_v2.0.md)

设计文档中的 SES 场景用于验证框架语义；本工程不依赖 SES 的构建或业务数据。
