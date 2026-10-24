# TEST-CLEANUP-01 独立复审 R3

> 日期：2026-10-24。结论：**代码验收结论保持；R02 清单语义仍未通过，暂不登记 COMPLETED。**
> 本轮只核表、核对代码/测试指纹及 docs 不变性，未运行 Cargo/UI，未修改正式代码、测试、作者材料或暂存区。独立证据：`target/test-cleanup-review-R4/`。

## 1. 本轮已通过的部分

- 与 R2 审查输入相比，所有 `src` 文件、Rust 测试、UI 夹具、驱动、消费者 manifest/lock 均保持一致。原 R01/R03 继续关闭，沿用上轮独立通过的 **524 lib + 72 integration + 1 doctest / 4 ignored、62 UI、13 自检、2 见证**；不把这些登记为本轮新执行结果。
- `UI_DELETION_GROUPS.tsv` 的 **118 组、321 个互异成员**完整，组内数量一致，分类恰为 **226 历史非活跃 + 93 原活跃 + 2 故障控制**。
- CASE_MAP 跳过 `#` 注释后均为五列；**K01～K13 均已出现**，按文件分组的代表函数已改善；B11/B12、child 正对照和独立 16 槽载体已补。
- docs 的 **10,432** 项非构建文件内容与路径保持不变。

这些结构性修订接受，接下来只需修正下列语义及引用内容。

## 2. R02 · P2 · 完整成员表仍包含错误的退役和等价承接结论

### 2.1 不能把整个 v22 前缀组判作契约退出

位置：`UI_DELETION_GROUPS.tsv:2`、`CASE_MAP.tsv:4`。

`former-active|v22|RETIRED` 把 **72 个原活跃成员**统一标为“现无对应运行期入口”。其中包含：

- `v22_t06_current_outside_round_rejected.rs`：仍由 `tests/internal/engine/tests_v22_t06.rs::t06_current_is_rejected_outside_a_direct_round_body` 明确断言 `CurrentOutsideRound`；规则没有退出。
- `v22_t03_from_we_t02_foreign_ref_rejected.rs`：跨 Definition Ref 拒绝仍有效；已有 `tests/internal/engine/tests.rs::a09_foreign_definition_ref_is_rejected`。
- `v22_t03_from_we_t01_ref_has_no_accessor.rs`：Ref 不暴露运行值的规则仍有效；已有 `tests/ui/ref_api__no_value_accessor.rs`。

该组还混入旧签名/旧可见性阶段已失效用例、现行 Shape 上限、Item/Current 可见性及所有权规则。它们不能因为文件名都有 v22 前缀就共享一条退役理由。没有找到自动匹配的载体，也不等于契约已经退出；静态类型约束尤其不要求存在某个“运行期入口”。

### 2.2 “同域”分组仍有不同契约的替代

例如：

| 当前关系 | 应按实际判据核对 |
| --- | --- |
| `CASE_MAP.tsv:11`：Iter 17 槽、unit state、零预算等 → `iter__round_body_returns_ref.rs` | 后者只拒绝 Round 闭包返回 Ref；17 槽、unit 状态、零预算各有不同契约。已有 `shape__iter_17_slots_state.rs`、`shape__iter_unit_state.rs`、`e02_iter_zero_rounds_is_rejected_at_build_time` 等对应载体 |
| dyn Node → `node__arity_mismatch.rs` | 对象安全与输入 arity 不是同一拒绝；已有 `tests/ui/node__dyn_not_object_safe.rs` |
| 非 Data 元素 → `p22_duplicate_targets_and_element_types_are_rejected` | p22 检查运行期 collector/元素类型元数据，不能证明 `T: Data` 的编译约束；已有 `tests/ui/shape__each_element_not_data.rs` |

请按完整成员的真实断言分组：同一契约的重复载体合并，仍有效的约束指向对应 Rust/编译夹具，确实退出的旧协议给具体失效原因。现有完整成员清单可以继续使用，不必增加历史用例数量。

### 2.3 三处可直接修正的引用/规则错误

- **不存在的消费者路径**：CASE_MAP `:28`、`:112`、`:175` 使用 `tests/ui/consumer/src/main.rs`，实际消费者在 `tests/support/consumer/src/main.rs`。其中 Ref 不可构造负例更应指向 `tests/ui/ref_api__not_constructible.rs`，合法消费者不能证明构造入口被编译器拒绝。
- **Query 规则写反**：`:282` 将 `b05_duplicate_query_rejects_before_any_business_execution` 描述为“显式重复查询被拒绝”。该测试实际调用自动 `then_output`；应写 **自动 Query 重复类型在 Definition 拒绝**。显式只读重复 Ref 允许，这一冻结规则保持不变。
- **取消用例名称不存在**：`:325` 的 `p35_pending_cancel_ends_the_borrowed_round` 无对应函数。实际载体是 `tests/internal/engine/tests_each_control.rs::p35_pending_cancel_ends_the_borrow_first_and_drops_each_instance_once`。

## 3. 最小收口要求

继续同一 R02：修正两个 TSV 的分组/承接/判据与上述三类引用，同步交接摘要。无需修改已通过的 Rust 代码，也无需因映射变化重新跑全套 Cargo/UI。最终核表应确认：

1. 每个保留载体路径/函数确实存在，指向该组的同一契约；
2. RETIRED 只包含确实退出的契约，不能由文件名前缀或未找到匹配项推定；
3. 321 成员集合及 226/93/2 分类继续完整、不重复；
4. K 与 §5 判据不与已冻结规则矛盾。

本轮没有新增执行机制或测试覆盖问题；剩余问题全部属于清单正确性。被审输入指纹与核对结果见 `INPUT_STATE.json`、`MAPPING_AUDIT.json`、`DOCS_AUDIT.json`。状态保持 IMPLEMENTED，待 R02 修订后收口；未提交/推送/发布。
