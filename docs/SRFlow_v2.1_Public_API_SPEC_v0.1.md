# SRFlow v2.1 Public API SPEC v0.1

> 状态：**USER CONFIRMED — FROZEN CONTRACT v0.1**。冻结日期：2026-10-08。
>
> 本文冻结独立 Rust Probe 已验证的 Public 契约和支持范围。原设计基线没有逐字通过：Struct Node 的 Query 表达已修订，业务 Data 的声明及 tuple 边界已明确。用户于 2026-10-08 增补 `#[derive(Data)]` 与零输入 Root，两项均已补充 Probe 验证；在理解 Data 的 'static 约束后，用户最终确认本版完整契约。原 SRFlow 工程尚未提供这些接口。
>
> 依据：[Public API Probe Baseline v0.1](SRFlow_v2.1_Public_API_Probe_Baseline_v0.1.md)；验证：[独立 Probe 结果](../../v3/SRFlow_Public_API_v21_Probe/RESULTS.md)；复跑入口：[Probe README](../../v3/SRFlow_Public_API_v21_Probe/README.md)。Core ownership、Scope、只读借用和 Root 输出约束继续适用。本次工作到本文为止；正式 Definition / IR 设计和原工程改造未开始。

## 1. 契约范围与依据

本规范规定业务使用者如何定义与执行 workflow，以及必须保持的可观察行为。Root 入口、Node、Query、Ref、Chain、Fragment、Each、Choose、Retry、Iter、错误传播和数据生命周期属于本规范；存储布局、类型擦除、内部 adapter、IR 结构和内部 ID 表示不在冻结范围内。

Public 使用模型以本文为准。旧规范中的 `FlowBuilder / Workflow / NodeCallN / EachShared / RetryN / IterN` 示例不描述这一新模型。若后续实现不能满足本文，不得静默迁就旧实现；须提交具体反例并修订契约。

Core 数据语义仍依据 [Core Design](SRFlow_Core_Design_v0.1.md) 与 [Runtime 内部实现基线](SRFlow_Core_Runtime_Implementation_Design_v0.1.md)。本 Probe 使用其数据责任机制的快照，并在独立副本验证成组 Consume / Promote；不授予完整旧 Invocation / Orchestrator 的新接口合规结论。

## 2. Public 模型

| 概念 | 契约 |
| --- | --- |
| Runtime | 接收一次 Definition closure 和 owned Root Input，执行后返回 owned Output 或 RunError |
| Flow | 同步定义期的编排上下文；不持有本次执行的业务值 |
| Ref<T> | 强类型逻辑依赖身份；Copy，不拥有、读取或复制 T |
| Query | 本次 Node invocation 的只读输入视图；只提供已声明输入 |
| Node | 异步业务叶子；结构体通过 Input / Output 和 run 声明执行契约 |
| Data | 可作为非 unit 业务叶子进入数据流的类型声明 |
| Choice | 注册值匹配分支的定义上下文；case 的 branch 仍是完整 Flow |
| BodyError | Node 的 Control / Failure 通道 |
| RunError | Root 构建失败或最终执行失败的报告 |

InputShape、OutputShape 和 StateShape 是类型边界描述。使用者不操作内部 Signature、Import、Export、Collector、Scope、CallSite 或 Runtime Target。

## 3. Data、Shape 与支持范围

### 3.1 Data 声明

Data 保留空 marker 的设计方向。非 unit owned 业务 struct / enum 以 `#[derive(Data)]` 作为标准使用方式，进行一次性声明：

```rust
use srflow_public_api_v21_probe::Data;

#[derive(Data)]
struct Prose { text: String }
```

Data 是空 marker，要求类型满足 Any / 'static，不要求 Clone、Copy、Send、Sync 或序列化。声明不增加字段、不实现控制逻辑，不随 Node 输入数量变化。

`#[derive(Data)]` 自动生成该 trait 的空实现；手写 `impl Data for Prose {}` 仍合法，两种形式择一使用。派生不要求字段类型也实现 Data。泛型类型保留原有泛型参数与 where 约束，仅在完整业务类型满足 'static 时实现 Data；派生不能使短生命周期借用成为可存储业务值。Data trait 与 derive macro 通过同名 Public 导出提供。

标准整数、浮点、bool、char、String，以及满足静态类型约束的 Vec、Option、Box、Rc、Arc，已有 Data 声明。Data 不 blanket 实现给任意 Any：本版需要在 stable Rust 中明确区分非 unit 数据与 `()`。

第三方类型包装限制是本版已接受的已知使用成本。未实现 Data 的第三方类型须通过已注册的容器（如 Box<T>）或实现 Data 的本地业务包装类型进入，本版不承诺任意第三方 Any 值均可直接接入。包装后的类型成为实际 Node Input / Output 类型，因此已有 Node 签名可能需要适配；`#[derive(Data)]` 不消除这项成本。

### 3.2 Shape

| 边界 | 支持的自然形式 |
| --- | --- |
| Root Input | `()`；单 Data 值；2～16 个 Data 值的 flat tuple |
| Node Input | `()`；单 Ref；2～16 个 Ref 的 flat tuple |
| 编排 Output | `()`；单 Ref；2～16 个 Ref 的 flat tuple |
| Root 成功 Output | 对应的 `()`、单 owned Data、owned 值 tuple |
| Iter State | 单 Ref；2～16 个 Ref 的 flat tuple；输入、next、最终输出 Shape 相同 |
| Each 集合 | Ref<Vec<T>>；T 为已声明 Data 类型 |

单输入不要求 singleton tuple。零输入 Node 与零输入 Root 均合法，以 `()` 表示零个输入位置；不分配占位业务槽，也不产生 Ref<()>。无状态 `()` Iter 不支持。超过 16 个位置和 nested Ref tuple 不属于本版承诺；方法名称不按 arity 分类。

### 3.3 tuple Shape 与单业务值

`(Ref<A>, Ref<B>)` 声明两个独立数据位置；不等于单个 tuple Data。直接 native tuple 本版作为 Shape 使用，不作为默认 Data 叶子。单组合业务值可用已声明业务 struct，或明确的 `Box<(A, B)>` Data 叶子表达。

后者产生 `Ref<Box<(A, B)>>`，只占一个位置；Node 借用整个 Box 内的业务值，不能把字段变成可跨 Scope 的 Ref。Node 一次产生多个独立数据输出和 Bundle 未进入本版；多个编排输出来自已有的多个 Ref。

## 4. Runtime 与 Definition

唯一 Root 入口：

```rust
let output = runtime.execute(
    |flow, (a, b)| {
        let c = flow.then(&node1, (a, b));
        flow.then(&node2, c)
    },
    (value_a, value_b),
).await?;
```

第二参数确定 owned Input 类型；closure 接收相应 RefShape；返回值明确声明 Root Output。没有公开 Workflow 对象、Flow::build、Workflow::call 或额外 finish / output 步骤。

零输入 Root 保留同一入口，第二参数与 closure 的输入均为 `()`：

```rust
let result = runtime.execute(
    |flow, ()| {
        flow.then(&load_data, ())
    },
    (),
).await?;
```

这里 load_data 是零输入 Node（函数形式使用 Query<()>），由它产生新的 owned 业务值。Root 不预先提供业务数据，仍可执行完整编排并返回 `()`、单值或多个值；错误传播与生命周期规则相同。零输入不意味着省略 execute 的第二参数。

execute 的 Future 被驱动时，先同步构建、验证本次 Definition，再进入异步 Node 执行。Definition closure 不 await、不读取 Ref 对应业务值。构建失败返回 RunError::Definition，任何 Node 均不开始执行；构建 closure 自身的普通 Rust 副作用不在该保证中。

每次 execute 建立独立 Root Execution。其全部 child 调用共享本次数据责任域和唯一 Container，不重新建立 Root。当前每次重新构建 Definition；缓存和预编译未进入契约。

本版采用顺序、单线程、允许非 Send Future / Data 的执行模型，不绑定特定 executor。Arc<Node> 支持不意味着 Root Future 可以发送到另一线程。

## 5. Node 与 Query

### 5.1 Function Node

```rust
async fn diagnose(
    query: Query<(&Prose, &Basis)>,
) -> Result<Diagnosis, BodyError> {
    let (prose, basis) = query.get();
    // 对只读业务输入计算，并返回新的 owned Diagnosis。
    todo!()
}
```

输入对应 `Query<()> / Query<&A> / Query<(&A, &B, ...)>`。普通 async function item、函数引用和满足相同异步签名的 async closure 可直接连接，无需 NodeCallN、函数指针转换或 Signature 注解。普通同步 Result 函数不符合此异步协议。

### 5.2 Struct Node

Node trait 采用 `Query<&Self::Input>` 作为统一协议，零输入、单输入与多输入共用这一 run 签名；Function Node 保持 5.1 节的自然 Query 写法。完整协议形态：

```rust
pub trait Node {
    type Input: InputSpec;
    type Output: NodeOutput;

    async fn run(
        &self,
        query: Query<&Self::Input>,
    ) -> Result<Self::Output, BodyError>;
}
```

InputSpec / NodeOutput 是框架提供的类型映射约束，使用者声明业务 Input / Output，不实现 adapter 或存储操作。

```rust
impl Node for GenerateNode {
    type Input = (Plan, Rules);
    type Output = Candidate;

    async fn run(
        &self,
        query: Query<&Self::Input>,
    ) -> Result<Candidate, BodyError> {
        let (plan, rules) = query.get();
        // plan: &Plan；rules: &Rules。
        todo!()
    }
}
```

也可明确写作 `Query<&(Plan, Rules)>`；单输入可写 Query<&A>。两种已验证形式均不要求 invocation lifetime 注解。

**此处 `&Self::Input` 是输入声明的借用标记。** Input 为 `(A, B)` 时，get 返回 `(&A, &B)`，不是制造并借用一份新的 owned `(A, B)`。Input 为 A 时返回 &A；Input 为 `()` 时返回 `()`。

原候选 `impl Node` 中直接使用 `Query<(&A, &B)>` 未通过统一 trait 匹配，不能继续作为本版 Struct Node 签名。函数形式的该写法保持有效。

### 5.3 借用、Output 与 Node 复用

Node 只从 Query 获取声明输入的 Rust 不可变借用；可跨 await，来源在实际 Future 使用期间不得移动或清理。不可变借用不等于对业务类型内部可变性的额外冻结保证。

正常返回新的 owned Data 或 `()`。`()` 不分配业务槽、不产生 Ref<()>。借用输入不能作为可逃逸 owned Output。Node 不接收 Ref、Runtime Target 或 Context，不获取任意容器数据；SRFlow 子编排仍属于 Flow / 控制结构的职责。

`&具体 Node`、owned 具体 Node 和 Arc<具体 Node> 均可注册；引用与服务客户端必须存活到实际调用结束。Arc 克隆只共享 Node 句柄，不复制业务数据。dyn Node 不在本版支持范围。

## 6. Ref 与执行顺序

Ref<T> 为 Copy，不附加 T: Copy / Clone。复制后仍是同一逻辑依赖；同类型不同 Ref 可表示不同数据实例。Ref 没有 get、borrow、字段投影、DataId 或 ScopeId 访问器。

同一 Flow 内步骤严格按定义顺序执行；前一步完成后才开始后一步。没有数据依赖不授权重排或并发；输出未被使用的 `()` 步骤仍执行。Failure 或传播中的 Control 中止被退出路径的后续步骤。

Child 只可引用当前位置合法可见的祖先 Ref；Definition 为每个跨边界依赖形成显式 Import。无关 Definition、sibling 内部引用和 child-local 直接泄漏，在 Node 执行前拒绝。典型未来变量的自然写法首先受 Rust 定义先后规则约束；框架不开放预造 Ref 或前向接线入口。

Child 返回的 RefShape 经该结构的输出语义转为 parent 可见的新 Ref，不直接授予其内部位置的访问权。

## 7. Chain 与 Fragment

```rust
let checked = flow.chain(|sub| {
    let candidate = sub.then(&generate, (prepared, rules));
    sub.then(check, candidate)
});
```

Chain 建立真实 child 数据生命周期边界。只保留声明输出，清理其他 child-owned 中间数据；不销毁 imported ancestor 数据。普通 Rust block 不提供该边界。

所有 child Flow 具有 then / chain / each / choose / retry / iter 能力。支持 `()`、单 Ref 和 flat tuple Output。

Fragment 首选普通函数返回 closure：

```rust
fn fragment<'n>(
    node: &'n MyNode,
    input: Ref<Input>,
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Output> + 'n {
    move |sub| sub.then(node, input)
}

let output = flow.chain(fragment(&my_node, input));
```

Fragment 可按普通 Rust lifetime 描述其 Node 依赖的借用；这是定义期 Node 句柄的寿命，不是 Node invocation lifetime。接收 `&mut Flow<'n>` 的普通函数也合法，可在 chain closure 内调用。

复用的是 Definition 逻辑；每个调用建立自己的 child 边界并校验捕获，不复用已经构建的 Workflow 对象，也不将 Ref 复制成业务值。

## 8. Each

```rust
let (scores, reports) = flow.each(items, |item, each| {
    let score = each.then(&score, (item, rules));
    let report = each.then(&report, (item, score));
    (score, report)
});
```

items 为 Ref<Vec<Item>>，局部 item 为 Ref<Item>。各 item 按输入顺序执行，其 body 使用普通 ancestor capture，没有 shared-input 分类。

| body Output | Each Output |
| --- | --- |
| Ref<O> | Ref<Vec<O>> |
| 多 Ref tuple | 每个位置分别形成 Ref<Vec<对应类型>> |
| `()` | `()` |

空集合不调用 body，正常输出相应空 Vec 或 `()`。失败 / 向外传播的 Control 停止后续 item，不产生部分正常集合。

item 是集合元素的受限借用，不获得独立 DataId，不被隐式 Clone 或移走。只有 item 调用链产生且责任可合法转移的 owned 输出能被收集。item 本身、ancestor Data 或重复 owned alias 不能被收集器消费。

多位置输出必须在任何物理移动前对整组预检，通过后成组消费并关闭 ItemScope。Collector 在完成前不是 Node 可借用的业务 Data。此能力需要成组 Consume 接入扩展；原 Core 单输出 Consume 不能被反复调用来冒充它。

## 9. Choose

```rust
let result = flow.choose(route, |choice| {
    choice.case(Route::A, |branch| branch.then(&node_a, data));
    choice.case(Route::B, |branch| branch.then(&node_b, data));
    choice.otherwise(|branch| branch.then(&fallback, data));
});
```

selector 为 Ref<K>，K 是已声明 Data 并支持 PartialEq 的业务键。case 用值匹配，key 是 Definition 的匹配配置，不因此成为新业务 Data 输入。复杂路由判断由上游 Node 产生 selector。

所有 case / otherwise 的 OutputShape 和每个位置类型一致。运行时只执行一个选中 branch，未选分支不建立执行 Scope、不执行其 Node；其 Definition closure 仍在构建时运行。

otherwise 可选，不要求编译期穷举。无匹配且无 otherwise 返回 RunError::NoMatchingCase；选中 branch 失败不改走 otherwise。重复 case 值、重复 otherwise、无任何分支的配置在构建时拒绝。不存在 predicate case 或 choose macro。

## 10. Retry

```rust
let candidate = flow.retry(3, |attempt| {
    let candidate = attempt.then(&generate, (prompt, rules));
    attempt.then(&judge, (candidate, rules));
    candidate
});
```

max_retries=N 指首次 attempt 外最多再执行 N 次；总数最多 N+1。N=0 仍执行首次 attempt；无法表达 N+1 的 usize::MAX 配置构建失败。没有默认次数。

正常完成导出 body Output，Retry 结束。最近 Retry 捕获显式 Retry 信号，清理整个 attempt 再重做；重试单位是完整 body，使用原始 ancestor dependencies，不提升失败状态、不保存默认历史。

Failure、IterBreak、子控制器耗尽或 Runtime 失败不自动成为 Retry。预算耗尽返回 RetryExhausted，保留 max_retries、实际 attempts 与最后 RetryError，包括动态原因 / source chain。最后候选不作为正常结果或错误 payload 的业务输出。

支持 `()`、单 Ref 和多 Ref tuple；attempt 是完整 Flow。已发生的外部副作用不自动回滚。

## 11. Iter

```rust
let (draft, context) = flow.iter(
    (draft, context),
    5,
    |(current_draft, current_context), round| {
        round.then(&judge, (current_draft, current_context));
        let next_draft = round.then(&revise, current_draft);
        let next_context = round.then(&update, current_context);
        (next_draft, next_context)
    },
);
```

单状态和 flat tuple StateShape 均成立。正常 body Output 为同一 StateShape 的 next，表示 Continue；每个位置经 Promote 成为下一轮 current，不重绑 parent Definition RefId。

最近 Iter 捕获 IterBreak 后返回本轮输入 current 的完整 Shape。即使本轮已产生其他 next 数据，Break 也不以它们代替 current；未保留局部数据清理。

max_iterations=N 要求 N>0，第一轮计入 N。第 N 轮正常完成仍要求 Continue，则返回 IterationLimitReached，不强制成功、不额外执行一次 Judge，不通过错误返回最后状态。

Imported 初始状态保持原 owner；Loop-owned 被替换状态在无有效引用 / 控制状态保留时回收。same-target 替换以及多个状态位置共享同一目标不能产生重复 owner 或提前销毁。多个 next 位置须成组预检、更新状态、关闭 RoundScope，不能用多次单状态 Promote 提前关闭该轮。

Retry / Failure 向外传播，Iter 不解释它们。不采用 IterDecision、不默认保存历史、不引入 Iter1 / Iter2。

## 12. Error 与动态控制传播

```text
BodyError
├─ Control(ControlSignal)
│   ├─ Retry(RetryError)
│   └─ IterBreak(IterBreak)
└─ Failure(BodyFailure)

RunError
├─ Definition(BuildError)
├─ Body(BodyFailure)
├─ RetryExhausted { max_retries, attempts, last_error }
├─ IterationLimitReached { max_iterations, completed_iterations }
├─ UnhandledControl(ControlSignal)
├─ NoMatchingCase
└─ Runtime(框架错误诊断)
```

Node 返回 Result<Output, BodyError>。BodyFailure 保存动态 Error 对象及 source chain；RetryError 保存动态原因并可关联 source。便利构造支持 BodyError::fail、BodyError::retry、BodyError::iter_break；不提供宽泛隐式错误到控制信号转换。

Control 沿动态调用链寻找最近对应类型边界。Chain / Each / Choose 不吞信号；Retry 不捕获 IterBreak，Iter 不捕获 Retry。同类嵌套由内层先处理；内层耗尽是终止错误，不再被外层当信号重试。

Each item 内的 IterBreak 会结束最近外层 Iter，并停止剩余 item；不是跳过当前 item。Iter 内的 Retry 可以重启整个外层 attempt，其下 Iter 从该 attempt 的原始 state 开始。

找不到对应边界的 Control 形成 UnhandledControl。普通 Failure 立即终止路径，保留源错误；错误不驱动其他业务步骤，也不能作为未正式输出业务 state 的旁路。

所有被退出作用域必须在相关借用结束后清理其责任数据，保留 ancestor-owned 数据。取消整个 Root Future 会结束其所有内部调用和业务数据寿命，不授予 child 脱离 Root 存活的能力。此约束不承诺回滚外部副作用或处理任意业务 panic。

## 13. Root 输出与别名

Root 成功前校验全部输出。直接重复同一逻辑 Ref 在 Definition 阶段拒绝；不同 Ref 最终指向同一 DataId 的 owned 输出，在任何 take 前整组拒绝。

只读 alias 可以用于 Node 输入、child 输出或状态位置；它不产生第二份 Data ownership，也不保证可被取走两次。Root 失败无部分正常 Output，`()` 不提取任何业务 Data。

## 14. 完整最小用法

以下代码对应已运行的 [spec_minimal 示例](../../v3/SRFlow_Public_API_v21_Probe/examples/spec_minimal.rs)：

```rust
use srflow_public_api_v21_probe::{BodyError, Data, Node, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

struct AddOne;
impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(
        &self,
        query: Query<&Self::Input>,
    ) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let output = futures::executor::block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime.execute(
            |flow, amount| {
                let doubled = flow.then(double, amount);
                flow.chain(|sub| sub.then(&add, doubled))
            },
            Amount(21),
        ).await
    }).unwrap();
    assert_eq!(output.0, 43);
}
```

生产包名不由 Probe 包名冻结。其他高层例子假定相应业务类型 / Node 已声明；完整可运行 S08 / S10 见 [SES 场景](../../v3/SRFlow_Public_API_v21_Probe/tests/ses.rs)。这些只证明 API 可表达、可执行，不证明真实 SES 质量策略通过。

## 15. 冻结结果与后续边界

2026-10-08 最终确认：用户明确确认“这个 API SPEC 就定下来了”。本版作为后续内部设计与实现的 Public API 基准；契约变更须明确记录原因、影响与验证证据，并经用户确认后修订，不得由实现细节静默改变。

2026-10-08 增补：Data 支持派生与手写两种实现形式；Root Input 支持 `()`。增补证据见 [Data 派生测试](../../v3/SRFlow_Public_API_v21_Probe/tests/data_derive.rs)、[零输入 Root 测试](../../v3/SRFlow_Public_API_v21_Probe/tests/zero_root.rs) 与 [派生借用逃逸拒绝样本](../../v3/SRFlow_Public_API_v21_Probe/tests/ui/derive_borrow_escape.rs)。其余契约保持本版已确认内容。

2026-10-08 小范围修订：按用户提出的范围明确以下契约。此修订整理已验证、已确认的 API，不改变其他接口或执行语义；版本继续为 v0.1。

| 项目 | 修订后的明确表述 |
| --- | --- |
| Data | 保留空 marker；`#[derive(Data)]` 为标准使用方式，手写 impl 仍合法 |
| Node | 接受 `Query<&Self::Input>` 的统一 trait 协议；Function Node 用法保持不变 |
| Runtime::execute | 支持 `()` Root Input，保留第二参数 |
| 第三方类型 | 未实现 Data 的类型需要包装，明确为已知使用成本 |
| 其他 API | 保持不变 |

原基线 O01 按既有 Core 规范落实严格定义顺序，且 S10 证明 Stop 在 Choose 前执行。O02 采用纯业务诊断 + 明确控制 adapter，保留控制 adapter 对 Retry / Iter 的显式依赖，不宣称彻底消除耦合。O04 的 closure factory 成立；接收 Flow context 的普通函数同样成立。

Each 并发、缓存 / 预编译、dyn Node、Send / Sync 多线程保证、任意 Any 自动接入、native tuple 单叶 Data、nested Shape、超过 16 个位置、Bundle、Trace、持久化、恢复、性能与 panic 策略不属于本版契约。

内部 helper trait、Box Future、Definition 表结构和 Core bridge 是 Probe 选择，不冻结为正式实现布局。正式实现须保持本文行为，并重新验证真实 ExecutionContext / Invocation 的调用权限、Control 与全局终止状态的区分，以及完整错误 / 取消诊断。

本次没有修改原工程或关闭 V21 / G21 Gate。后续工作另行规划；不能把本 Probe 源码直接作为正式实现提交。
