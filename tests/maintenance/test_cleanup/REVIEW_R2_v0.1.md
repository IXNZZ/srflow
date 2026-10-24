# TEST-CLEANUP-01 独立复审 R2

> 日期：2026-10-24。结论：**代码迁移及验证通过；仅剩 R02 一项 P2，修正完成态映射后再收口，不登记 COMPLETED。**
> 本轮没有修改正式 Rust、测试断言、作者证据、暂存区或 docs。HEAD 仍为 `614bddd241c14c20965370507b1ccc90d650f991`。独立证据：`target/test-cleanup-review-R3/`；全量测试原始日志：`target/test-cleanup-review-R3-tests.log`。

## 1. 上轮问题处置

| 项 | 本轮独立结论 |
| --- | --- |
| R01：src 完整测试辅助实现未迁出 | **关闭**。内部测试/辅助文件现为 48 个；src 无独立测试文件、测试主体及受 cfg(test) 隔离的完整函数/impl/枚举/结构体实现，保留加载、导入、字段与窄调用点。三个私有类型探针已迁入原测试子模块 |
| R03.1：生产不变性检查漏检初始化 | **关闭**。相同内存反例现已被检出；构造初始化保留在对比文本中。对全部改动 src 路径独立运行 compare：exit 0，四项指纹例外已人工核对，属于空白、测试方法迁出后的空 impl 及剔除加载语句留下的文本差异 |
| R03.2：B 只改了断言期望 | **关闭**。保留 Merged(17) 期望，仅将业务产物计算加 1；独立副本编译成功，真实运行在目标断言失败，实际 Merged(18)、期望 Merged(17)，exit 101 |
| R02：清单计数/契约承接 | 计数已从计划转向实际执行；仍有下述一项 P2：映射内容错配、遗漏和成员集合不完整 |

未发现本轮引入的生产行为缺陷。必要的 cfg(test) 接点可以保留，不要求将它们全部清零。

## 2. 独立检查结果

| 检查 | 结果 |
| --- | --- |
| 全量 Rust 测试 | **524 lib + 72 integration + 1 doctest passed / 4 ignored**，0 failed |
| fmt / check all-targets / build lib / clippy -D warnings / rustdoc -D warnings | 全部 exit 0 |
| UI 主检（含独立 Cargo 消费者）/ 自检 / 见证 | **62/62、13/13、2/2** |
| docs 非构建材料 | **10,432** 项全部一致，无新增、删除或内容差异 |
| 源码布局扫描 | src 为 23 个 Rust 文件；tests/internal 为 48 个文件；未检出 src 内完整测试专用实现 |
| 被审输入与索引 | 审查期间保持不变；本轮仅新增此报告与 docs 外的审查证据 |

UI 调用原驱动，仅将输出位置改到独立审查目录；没有覆盖作者日志。未重新执行六个示例，不将作者示例结果登记为本轮独立验证结果。

## 3. R02 · P2 · CASE_MAP 仍将不同契约误配为等价承接

位置：`CASE_MAP.tsv:70`～`:103`、`:223`～`:247`。

### 3.1 UI 删除组仍有明确语义错配

| 当前表中关系 | 不成立的原因 |
| --- | --- |
| :99 `we_t04_e04_iter_zero_rounds_rejected.rs` → `legacy_api__two_arg_iter_rejected.rs` | 前者验证零预算的 Definition 拒绝；后者仅以 E0593 拒绝旧闭包参数个数，不运行零预算验证 |
| :72 foreign Ref / Retry 预算组 → `types__non_static_data_rejected.rs` | 后者仅拒绝非 static Data 的 E0478/E0803，不能承接跨 Definition 引用或 Retry 预算规则 |
| :100 collection element mismatch → `borrow__blocks_round_collection.rs` | 元素类型契约与借用冲突是不同的检查，不能由 E0502 借用拒绝替代 |
| :87 `d22_reentering_borrowed_context.rs` → `builder__missing_out_context.rs` | Context 重入借用与 owned Out 类型推导不足没有等价关系 |

这不是要求增加历史夹具数量。既有 Rust 测试可以承接，例如零轮迭代已有 `tests/internal/engine/tests_retry_iter.rs::e02_iter_zero_rounds_is_rejected_at_build_time`，跨 Definition Ref 已有 `tests/internal/engine/tests.rs::a09_foreign_definition_ref_is_rejected`。清单应指向实际检查相同判据的载体；旧协议确实退出的项则说明退出的具体符号或契约。

多成员组仍只写前五个名称加省略号。当前 `ui_prune_plan.tsv` 删除行的目标仍是 `-`，无法由它还原每个组的完整成员集合。请给出完整成员或一个能准确还原集合的清单引用，并区分 226 个历史非活跃项、93 个原活跃项与 2 个故障控制文件。无需为每个重复断言写记录。

### 3.2 K 和关键缺陷回归的映射仍不完整

- 表中虽有 13 个 K 行，实际只有 **11 个不同编号**：**缺 K05、K07，K04、K10 各重复一次**。
- 多行的文件与函数不对应。例如 :231 指向 `tests_runtime.rs`，却列 `context_tests.rs` 中的 `d01_root_creation_is_unique_and_shared`、`d06_leaf_await_registers_output_once_and_unit_leaves_allocate_nothing`；:234 指向外部六形式文件，却仍使用 Context/Definition 的函数作“代表判据”。这些名字存在，不等于它们位于该行载体中或证明该行契约。
- 任务书 §5 的 **B11/B12 DataId/owner 与同一实例交接判据仍未登记**；“被污染 child 含正对照”行只列了负例；Root 期望三例被标作“与 16 槽边界”，却没有实际 16 槽载体。对应测试仍在且通过，应修正映射而非重写测试。

可直接引用的既有载体包括：

| 待补项 | 当前实际载体（示例，按判据选择必要项即可） |
| --- | --- |
| K05 Query / Shape | `tests/internal/engine/tests_v22_t02.rs::b05_duplicate_query_rejects_before_any_business_execution`；`tests/v21_12_public_api.rs::tuple_root_inputs_support_sixteen_positions`；显式重复与非法形状另引用对应现有用例/fixture |
| K07 Chain / Choose | `tests/internal/engine/tests_v22_t04.rs::t04_chain_plain_and_output_share_registration_and_execution`；`tests/internal/engine/tests_runtime.rs::c09_selection_runs_one_branch_and_creates_one_boundary`，并指向相关 Import/Export 和失败边界断言 |
| §5 B11/B12 | `tests/internal/engine/tests_v22_t02_repeat.rs::b11_retry_rebuilds_factory_per_attempt_and_cleans_before_next`、`b12_child_factory_registers_in_current_scope_and_exports_owned` |
| §5 child 合法正对照 | `tests/internal/engine/tests_v22_t06.rs::t06_clean_child_declaration_still_publishes_after_an_earlier_error` |

**最小修订范围**：核对并修正 CASE_MAP、相关清单引用与交接摘要即可。每行给出正确的“契约 → 实际文件/函数 → 关键判据”，删除没有维护价值的总数和重复行也可以。不要求新增覆盖矩阵、恢复全部旧测试、重跑历史变异或再次调整 Rust 迁移方案。若只改映射，下一轮按本轮输入指纹复核并核表，无须因表格变化重跑整套 Cargo/UI。

## 4. 非阻塞文字同步

README 与交接 §2 仍有“42 文件”的旧计数，实际为 48；CASE_MAP 有单列注释行，交接不宜称“250 行全部五列”，可注明跳过 `#` 注释的读取约定；测试计数宜持续写成 lib / integration / doctest / ignored 分列。顺手同步即可，不另立问题。

证据索引：`CASE_MAP_AUDIT.json`、`MAPPING_EXAMPLES.json`、`SOURCE_LAYOUT_AUDIT.json`、`DOCS_AUDIT.json`、`PRODUCTION_COMPARE.log`、`PRODUCTION_CHECKER_CONTROL.json`、`WRONG_PRODUCT_*.log`、`CHECK_RESULTS.json`。状态保持 IMPLEMENTED，等待 R02 修订后复审；本轮不提交、推送或发布。
