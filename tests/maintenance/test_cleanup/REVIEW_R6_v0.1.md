# TEST-CLEANUP-01 独立复审 R6

> 日期：2026-10-24。结论：**R02 仍未关闭；代码整理验收结论保持，不登记 COMPLETED。**
> 本轮未运行 Cargo/UI，未修改正式代码、测试、作者清单或暂存区。独立证据：`target/test-cleanup-review-R7/`。

## 1. 已确认的修订

- R5 指出的零轮预算、Factory 编译约束、借用编译负例已换到相应载体；缺失 aggregate 已取消成功聚合正例的错误引用。
- Rust、测试、夹具、驱动和消费者均与此前通过独立验证的版本一致，沿用原结果；R01/R03 保持关闭。
- 当前 103 组覆盖 **321 个互异成员**，分类仍为 **226/93/2**；docs 的 **10,432** 项非构建材料保持不变。

## 2. R02 · P2 · 不能只修已举出的行，其余分组仍有同一问题

本次把当前 **47 个标为“Definition 拒绝”的组逐组核对到载体断言**，结果已完整列入：

[DEFINITION_GROUP_REVIEW.tsv](/Users/vinter/Dev/Mir2Docs/Novel_md/SES_v1/engineering/srflow/target/test-cleanup-review-R7/DEFINITION_GROUP_REVIEW.tsv)

其中 **30 组需要修订**，另 **17 组的 Definition 拒绝阶段成立**。该表逐行给出原组 ID、完整成员、现行载体、实际行为及修正方向；这 30 组仍属于同一个 R02，不是新增 30 项问题。

### 2.1 阶段退出与 Definition 拒绝互相矛盾

`UI_DELETION_GROUPS.tsv:3`、`:4` 的两组共 **37 个成员**，contract 仍为 Definition 拒绝，carrier 却改为字面量 `RETIRED`，criterion 仍写 RunError::Definition。CASE_MAP 又把相应 carrier 展示为“阶段契约退出，见 basis”。这既没有实际拒绝载体，也没有具体退出原因。

请按成员真实规则分组：仍有效的规则指向对应载体；确已退出的旧协议给出具体阶段原因。不能通过替换 carrier 文本来解决混合组。

### 2.2 成功断言继续被记为 Definition 拒绝

完整清单已在附表中列出。代表例包括：

| 现行载体 | 实际断言 |
| --- | --- |
| `a07_chain_builds_child_body_and_parent_refs` | `expect("chain should build")`，检查成功构建的 Chain 结构 |
| `a07_fragment_forms_build_with_non_static_nodes` | `expect("both fragment forms should build")` |
| `p01_frozen_usage_feeds_later_then_chain_and_root` | `expect("…must build and run")`，正常输出 |
| `c01_frozen_choose_usage_runs_case_case_and_fallback` | `expect("frozen choose run")`，分支输出值正确 |
| `c02_minimal_key_and_borrowed_node_run` | `expect("minimal key run")`，结果为 7 |
| `t04_output_expectation_accepts_matching_shapes_and_unit_pairs` | 明确断言匹配期望时运行成功 |
| `tuple_root_inputs_support_sixteen_positions` | 16 槽正常往返，不能作为超过 16 槽的拒绝证据 |

有些行只需修正行为方向；如果原成员本身是负例，还要换成同规则的拒绝载体，不能只改标签。

### 2.3 执行期/Core 拒绝也继续记成 Definition 拒绝

- `p13_engine_item_output_is_rejected_by_the_real_group_preflight` 明确匹配 **RunError::Runtime**。
- `p15_duplicate_owned_alias_group_is_rejected_before_any_move` 检查 **ScopeError::DuplicateConsumeTarget**，位于真实 Consume 路径。
- 两个 `e21_*_mismatch_*` 载体检查真实 Promote 调用的 **ScopeError::Invariant**。
- `p32b_no_matching_case_is_terminal_and_switches_no_branch` 检查 **RunError::NoMatchingCase**。

这些载体可能仍是有价值的长期场景；需要如实登记阶段，并确认原成员与它检查的是同一条规则。旧分支签名约束不能由 Promote 状态数量或运行期无匹配来代替。

## 3. 其它行为组中的现存错配一并列出

[OTHER_MAPPING_REVIEW.tsv](/Users/vinter/Dev/Mir2Docs/Novel_md/SES_v1/engineering/srflow/target/test-cleanup-review-R7/OTHER_MAPPING_REVIEW.tsv) 给出其它 **12 个组、25 个成员**的核对方向，便于集中修订。明确例子包括：

- `v22_t06_current_arity_mismatch_definition_error.rs` 仍指向 Node arity 编译拒绝，应核对现有 `t06_current_arity_and_shape_mismatch_are_rejected` 的 Definition 判据。
- `we_t01_borrowed_data.rs` 仍指向 Pending 借用冲突；原规则是 **Data 的 static 边界**，现有 `types__non_static_data_rejected.rs` 已承接。
- `we_t03_c05_key_without_partialeq.rs` 仍指向“键类型错接”；现有 `choose__key_without_partial_eq.rs` 检查缺少 PartialEq。
- `we_t03_c22_no_choose_macro.rs` 仍指向“没有 predicate 方法”；现有 `reachability__no_choose_macro.rs` 才检查宏入口不存在。
- `we_t01_illegal_node_output.rs` 的 tuple NodeOutput 规则，不能由 Root 闭包尾表达式被拒绝来代替。
- `v22_t05_from_we_t05_p02_collection_element_mismatch.rs` 中 Item 与 Amount **都实现 Data**，错误在显式 Node 输入 RefShape 不匹配；“元素未实现 Data”夹具不能证明该错接被拒绝。
- 一组旧“未完成 Builder 不可执行”负例被标为执行成功。原 f15 夹具明确是拒绝样本；若旧完成态封装已退出，应给出阶段原因，合法装配正例不是其拒绝证据。

对共用 Shape/借用实现的冗余成员，允许说明共同实现约束后选代表；已有相应夹具时直接引用即可。附表是核对与修改指引，不要求新增这些夹具，也不要求把所有历史断言恢复。

## 4. 最小收口建议

按两份附表集中处理剩余行；不要继续只修报告正文里举出的几个名称。载体至少要满足“存在、行为方向正确、失败阶段正确、规则对应”四项。

`HANDOVER.md` 的“缺失 aggregate 没有逐一对应函数”也可直接订正：已有 `tests/internal/engine/tests_v22_t05.rs::t05_each_output_missing_aggregation_rejected_with_fields`，明确断言 DeclarationShapeMismatch、期望/实际类型和业务零调用；这项无需再寻找或新增测试。

代码与测试继续保持当前通过版本。仅改清单时，后续核表和指纹即可；不重复全套 Cargo/UI。原表内容和审查指纹分别见 `INPUT_STATE.json`、`AUDIT.json`，本轮没有新增运行代码问题；未提交、推送或发布。
