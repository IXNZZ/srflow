# SRFlow v2.2 Root API 补充 Probe 结果

日期：2026-10-22。正式来源基线：`bb8d19497c70c828f35140a43edd2ba692b2371b`。状态：**限定可行性通过，待设计复审**。单执行者自检，不代替独立审查、正式实现回归或SPEC冻结。

## 1. 结论

O01/O02 的 Rust 可行性已补齐：保留 Flow 和 Root Input RefShape 两个闭包参数；将闭包返回改为unit，并通过Body内output声明业务结果；以调用方期望owned Out确定原有RefShape类型化提取桥。公开调用仍是Runtime::execute，没有Workflow/Flow::build。

```rust
let report: Report = runtime.execute(
    |flow, (original, revised)| {
        flow.then_with(compare, (original, revised));
        flow.output::<Report>();
    },
    (original_value, revised_value),
).await?;
```

该形式见 [完整运行例子](examples/root_minimal.rs)，两个同类型Root输入有各自显式Ref；统一业务函数`fn build_business(&mut Flow)`仍能在Root/Chain中使用。

## 2. 实际验证的类型桥

修改的是隔离副本现有 [Runtime::execute](src/core/engine/runtime.rs)，不是在另一个类上模拟签名。核心类型关系为：

```text
expected owned Out
  -> existing RootInput::Refs for Out
  -> RefShape<Owned = Out> + existing ShapeOps
  -> original execute_built / extract_root
```

方法返回`Result<Out,RunError>`，要求`Out: RootInput<Owned=Out>`及其Refs具备既有Shape能力；closure为`FnOnce(&mut Flow,In::Refs)->()`。没有新增使用者必须实现的Public Trait，也没有把Any下转型失败当作输出类型检查。

build_root在RootExecution创建、输入登记和任何Node/Factory前比较声明槽类型与Out对应Shape。缺声明、重复声明、类型不符等沿现有公开`RunError::Definition(BuildError)`返回。副本给内部BuildErrorDetail增加了一个携带静态说明的实验项，诊断布局/词句不冻结；公开RunError、BuildError形状不改。

类型上下文可来自局部变量、外层函数`Result<Report,RunError>`返回类型，或显式输出泛型实参。**若完全无Out类型上下文，Body内部output::<Report>()不会参与外层泛型推导，rustc以E0283拒绝。** 最小用法补结果类型注解即可；本实验的`execute::<_,Report,_>`参数顺序不是正式冻结结论。

## 3. 四项目标与额外边界

| 目标 | 证据 | 结果 |
| --- | --- | --- |
| 期望Report推导owned类型化提取 | r01局部注解/问号、r02外层返回类型、r17显式Out | 通过 |
| 声明与期望不符在执行前拒绝 | r05含真实Node/Factory计数均为0；r06缺声明、r08unit/非unit不互转 | 通过 |
| 同类型Root输入分别取得显式Ref | r03 `(Request,Request)` 比较得到19；r04同一Ref两次显式借用得到22 | 通过 |
| 保持Root公开入口与构建方式 | 实际Runtime::execute、unit closure、r12每次重建、r13未poll不构建、消费者正例 | 通过 |

其他限定检查：r07unit省略/显式均合法；r09多owned输出不需要Clone；r10用户新确认的重复类型自动Query在Definition拒绝且Factory不运行；r11重复声明拒绝；r14普通Body函数经Chain复用；r15非Send借用跨await；r16不同逻辑alias指同一实际Data仍被Root提取前拒绝。

6个编译拒绝样本：无期望Out（E0283）、错误Root输入Shape（E0308）、旧式Body返回Ref（E0308）、公开Workflow不存在（E0432）、Flow::build不存在（E0599）、短寿命Node句柄（E0597）。消费者正例独立编译并运行，输出12。

## 4. 运行结果

| 检查 | 结果/日志 |
| --- | --- |
| `cargo test --offline` | 17运行测试＋1编译验证入口，通过：[tests.log](evidence/tests.log) |
| 编译验证展开 | 6拒绝＋1编译运行正例：[ui.log](evidence/ui.log)、[诊断](evidence/ui) |
| `root_minimal` | `before -> after`：[example.log](evidence/example.log) |
| clippy明确目标，`-D warnings` | 通过：[clippy.log](evidence/clippy.log) |
| fmt | 通过：[fmt.log](evidence/fmt.log) |

原Auto API Probe中的536库测试没有迁移到新Root门面：它们调用旧式then和closure返回值。Cargo的lib.test=false排除它们，本次不宣称旧库/全部控制结构回归。初次误用clippy --all-targets曾强制纳入这些未迁移库测试并失败；随后改用本范围的显式目标检查并通过。不是通过修改旧断言来声称兼容。

## 5. 源码来源与范围

[BASELINE_SHA256.json](BASELINE_SHA256.json)记录正式Rust/Cargo输入指纹；[BASELINE_DELTA.patch](BASELINE_DELTA.patch)列出副本相对正式基线既有文件差异。Builder入口/来源解析和execute_built拆分复用前一实验的已标注修改，新增Root前端替换其Workflow路径。实际Node/Chain运行仍接同一现有Definition与Container。

adapter.rs、boundary.rs、context.rs、scope.rs、data_container.rs和Core runtime.rs与正式来源逐字相同；未创建新的执行计划、数据仓库或Runtime业务类型搜索。Root输入登记/执行/owned整组预检提取算法保持；变更集中于构建及类型衔接。

正式工程Rust/Cargo输入未修改，原Auto Probe证据也未修改。来源审计见[source_audit.json](evidence/source_audit.json)。

## 6. 设计审定与剩余限制

建议接受已验证的双参数Root closure及期望owned类型桥；最终签名布局随v2.2设计复审定案，技术PASS不自动冻结API。

本实验仅覆盖unit/单值/双槽代表Shape，不补做全部中间arity、全部控制嵌套、故障注入、取消回归、性能或发布验证。Choose混合输出声明拒绝已由用户确认并写入文档，但本Root补充没有实现/验证该新Choose规则。正式实现须重新接回全部控制结构并回归。

自动Query重复类型拒绝只在本实验的定义期自动Node入口补最小见证；所有正式函数/结构体/arity适配路径仍需后续完整验证。自动output重复位置的合法性仍由Root/Each/Chain/Iter各自原交接语义决定，不受这项Query禁令替代。
