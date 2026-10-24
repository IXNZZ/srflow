# TEST-CLEANUP-01 交接（R1 复审后修订：**IMPLEMENTED · 待独立复审**）

> 修订说明：首轮复审（`REVIEW_v0.1.md`）、R1 复审（`REVIEW_R1_v0.1.md`）、R2 复审（`REVIEW_R2_v0.1.md`）与 R3 复审（`REVIEW_R3_v0.1.md`，清单语义）的问题均已处置，见 §1。本文所有数字来自实测（`cargo test -- --list`、`tools/prod_blocks.py`、`tests/support/ui_check.py`）。

## 1. 两轮复审问题处置

| 复审项 | 处置与证据 |
| --- | --- |
| **v0.1-R02** 暂存区残留 | `AD` 条目 0；暂存区新增文件在工作区全部存在；无"删除但工作区仍存在"幽灵项 |
| **v0.1-R03** CASE_MAP 结构 | 首行五列表头；本次重写后为五列表（`tests/maintenance/test_cleanup/CASE_MAP.tsv`，328 行，段注释行以 `#` 开头；删除组完整成员见同目录 `UI_DELETION_GROUPS.tsv`） |
| **v0.1-R01 / R1-R01** 测试主体与辅助实现仍在 `src/` | **已彻底迁出**（见 §2）；R1 中指出的三个探针确认可迁出，交接旧结论失效 |
| **R1-R03.1** 不变性检查器漏检 | `item_end` 边界修正（字段以行尾逗号收尾、`use` 以分号收尾、if-else 链续扫）；窄反例证明能检出（见 §5） |
| **R1-R03.2** B 反对照只改断言期望 | 重做为"让受观察业务产物出错"：改产物计算值，期望值不动，断言失败（见 §5） |
| **R5-R02** 阶段与方向 | 零轮/预算→Definition 拒绝（`e02_iter_*`）；异步/单次/可变/unit/值型 Factory→编译拒绝（`factory__*` 各夹具）；缺失 aggregate→定义期拒绝（无逐一对应函数，以 `tests_v22_t05.rs` 声明校验族代表）；a14/h15 借用→编译拒绝（`borrow__*`，不再引用运行期正例）；42 成员兜底组按真实规则拆分（借用／unit‑Ref／根输出／输入类型与 arity／元素形状／键与协议／可达性／旧装配接口退出／renamed 正例） |
| **R4-R02** 行为方向 | `-/-` 混合组按**实际行为**拆分：非 static Node 正例→`b03_fragment_forms_execute_with_non_static_nodes`（执行成功）；旧类型/arity 负例→`node__wrong_input_type.rs`/`node__arity_mismatch.rs`（编译拒绝）；a14/b19/c23→`borrow__*`（编译借用规则，不再指向运行期取消用例）；T02 外部可见性负例→阶段契约退出（该约束在 T03 公开门面后失效，现行可用性由公开入口正例承接）；witness fault 控制文件→判定器自检 |
| **R3-R02** 清单语义 | 全部 321 个删除成员按真实断言语义（基准清单的 code/符号/anchor）重新归组为行为明确的组（编译拒绝／Definition 拒绝／执行成功／执行期失败或清理／判定器自检／阶段契约退出），逐组标注实际行为：载体取自保留夹具（同 code/符号规则）或实测存在的 Rust 用例，并对每条引用做**存在性校验**（0 失败）；仅 **9 个 V21-12 禁令对象**保留为契约退出并点名具体符号；Iter 17 槽/unit state、dyn Node、非 Data 元素、零预算、跨 Definition Ref、Ref/Choice 不可构造等按复审示例逐一改指；修正消费者路径（`tests/support/consumer/src/main.rs`）与 K05 Query 措辞（**自动** Query 重复在 Definition 拒绝；显式只读重复 Ref 允许）、p35 用例名 |
| **R2-R02** 映射错配 / 成员集合 / K 与 §5 缺口 | 4 处语义错配改为同判据 Rust 载体（零预算、跨 Definition Ref/预算、集合元素类型、Context 重入）；删除组完整成员改由 `UI_DELETION_GROUPS.tsv`（118 组、321 成员，含 226/93/2 分类）逐名列出；K 段改为按 K×文件 分组并补齐 K05/K07；§5 补 B11/B12 实例所有权、污染 child 正对照、16 槽独立载体 |
| **R1-R02** 计数与承接缺失 | CASE_MAP 全部计数改用实测执行列表；93 个原活跃 UI 删除项按契约类别给出成员集合、代表载体与关键判据 |

## 2. R01 迁移结果（§3.1）

`src/` 中已无任何测试主体、测试辅助实现或观测/故障注入实现：

| 类别 | 现状 |
| --- | --- |
| 测试专用文件 | **0**（全部迁入 `tests/internal/**`，48 个文件） |
| `#[test]` / 异步测试主体 | **0** |
| 完整函数 / impl / 枚举 / thread_local | **0**（全部为加载声明、结构字段或运行函数内的窄调用点） |
| 加载声明 | 48 条 `#[cfg(test)] #[path = "…"] mod …;`（测试主体 36 个 + 观测/故障注入 12 个） |
| 结构字段 | 87 处 `#[cfg(test)]` 字段（观察计数/故障槽/`#[cfg(test)] use` 命名接点，依附运行结构，无法单独搬迁） |
| 运行函数内窄调用点 | 其余条目：单条 `observe::…` 调用、或"注入判定→立即 `return Err`"的窄接点（多语句逻辑已抽入 `tests/internal/**/*_observe.rs`） |

R1 复审指出的三个探针（`record_promote_group_state_probe`、`replace_item_cap_probe`、`snapshot_targets_probe`）**已迁入 `tests/internal/core/scope_observe.rs`**：Rust 子模块可访问祖先模块的私有类型，故无需放宽 `PreparedPromote` 等生产可见性；`record_promote_group_state_probe` 以 `pub(super)` 保持原调用点有效。首轮"因私有规划类型无法迁出"的判断**撤回**。

迁移执行方式：`tests/internal/**` 文件按原逻辑模块名由 `#[cfg(test)] #[path]` 加载，仍是**库内单元测试**（私有访问边界不变）；普通 `cargo build --lib` 不含它们。

## 3. 精简结果（实测）

- 测试计数（分列）：库内 **524 passed**；集成 **72 passed**；doctest **1 passed / 4 ignored**；合计 **0 failed**（`cargo test -- --list` 列表条目 601）。
- 删除实况（逐条核对表 `target/test-cleanup-evidence/deleted_tests.tsv` 的 `applied` 列，以 `cargo test -- --list` 为准）：**已删除 242 条**；**仍执行 33 条**（属未落地删除的保留项，不再计入"已删"）。
- `src/` 行数 26,517 → **14,685**；`tests/internal/` 43,638 行（48 文件）。
- 删除判据：`duplicate`（同模板 arity、同语义多套快照）、`historical`（迁移期画像）；保留 `canonical`。arity 模板只留 0/1/2/16 边界实例。

## 4. 日常入口与旧入口关系

| 旧入口（docs/ 内，历史） | 现状 | 接替者 |
| --- | --- | --- |
| `docs/**/ui_check.py` + 各清单 | 不适用（指向已删夹具） | `python3 tests/support/ui_check.py`（`--self-test` 13 项 / `--witnesses` 2 项） |
| `docs/**/consumer` | 历史证据，不再运行 | `tests/support/consumer/`（依赖别名 `roundkit`，`--locked` + 外置 target） |
| `src/core/engine/tests*.rs`、Core 内联测试模块、`src/core/{test_support.rs,engine/observe.rs}` | 已迁出并删除 | `tests/internal/**`（由 `src` 的 `#[cfg(test)] #[path]` 加载） |
| T07 的 524 项源码指纹与矩阵脚本 | 保留为 T07 时点历史证据 | 无需接替（本任务未改生产与公开 API） |

## 5. 生产不变性与反对照（`target/test-cleanup-evidence/`）

- **检查器**：`tools/prod_blocks.py`（修正 `item_end`/块边界后）。结果：**10 OK + 4 已登记差异 + 29 测试专用文件删除**，exit 0（`PROD_INVARIANCE.txt`）。
  - 已登记差异（`tools/PROD_EXCEPTIONS.tsv`，按统一 diff 的 sha256 指纹校验，指纹不符即判 DIFF）：3 处为"删除 cfg(test) 块后 rustfmt 吞掉残留空行"（空白差异）；1 处为 `src/core/scope.rs` 的 `impl CollectionRoot`——它原先只承载被迁出的 `cfg(test) root_data`，迁出后为空壳，按"不留死重"删除。
- **反例（必须被检出）**：`negative_controls/C_prod_checker.log` —— 把 `DataIdAllocator::new(Arc::clone(&execution))` 换成 `ExecutionIdentity::new()`，检查器报 `DIFF` 且 **exit=1**；恢复后 SHA 一致并回到 `OK`。
- **反例（产物出错必须失败）**：`negative_controls/B_wrong_value.log` —— 保留 `Merged(17)`／`Merged(14)` 期望不变，把产物计算 `key.0 + amount.0` 改为 `+ 1`，`cargo test --test we_t07_final_scenario` **exit=101**，失败断言 `普通 chain 必须发布其声明输出供自动下游读取`；文件逐字节恢复（sha 前缀 `888294b92cbbb00a`）。
- **反例（跳过普通登记必须失败）**：`negative_controls/A_skip_chain.log` —— 移除普通 `chain` 登记后 `Definition(MissingSource{Merged})` 失败。

## 6. 未解决项

1. `docs/` 未改动（10,432 个非构建文件逐项指纹一致）。
2. 顶层集成测试仍为 8 个文件（未合并）；§5 判据均保留并执行（见 CASE_MAP 的 §5 段）。
3. `src` 保留的加载声明/字段/窄调用点是任务书 §3.1 允许的最小接点；如需进一步收敛，须在"删除运行期观察能力"与"保留"之间取舍。
4. 本任务不改变 T07 历史结论；未提交、未推送、未打标签。
