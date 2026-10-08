# SRFlow Public API 使用示例草案 v0.1

状态：DRAFT（调用方目标示例，尚未与当前实现逐项对齐）

本文先从调用方视角描述希望写出的代码，再据此检查和调整 Builder、类型映射与内部实现。示例优先表达业务流程；除非类型推导确实需要，不在普通调用中显式写 `SyncFnSig`、`AsyncFnSig`、`OrchSig` 等 Marker。

本文暂按已提出的输入形式书写：单输入以业务类型 `A` 表示，多输入以 tuple `(A, B, ...)` 表示；Root 执行也遵循相同形式。具体如何在 stable Rust 下区分两种输入形状，留待与示例对照时验证。

## 1. 业务类型与 Node

```rust
struct Order { id: u64, amount: u32 }
struct ValidOrder { id: u64, amount: u32 }
struct Receipt { id: u64, total: u32 }

fn validate(order: &Order) -> Result<ValidOrder, BodyError> { /* ... */ }
async fn settle(order: &ValidOrder) -> Result<Receipt, BodyError> { /* ... */ }
```

Node 读取已有业务 Data 的共享借用，成功时返回新的 owned Data；异步函数由 Runtime 调用，executor 由应用决定。

### 1.1 结构体 Node 与共享实例

```rust
struct TaxCalculator { rate: u32 }

impl NodeCall1<Order, Data<Receipt>> for TaxCalculator {
    fn call<'a>(&'a self, order: &'a Order) -> NodeFut<'a, Receipt> {
        Box::pin(async move {
            Ok(Receipt { id: order.id, total: order.amount + self.rate })
        })
    }
}

let calculator = Arc::new(TaxCalculator { rate: 3 });
flow.then(calculator.clone(), order_ref)?;
flow.then(calculator.clone(), another_order_ref)?;
```

`Arc` 共享的是 Node 实例；它不共享不同 Execution 的业务 Data。

### 1.2 无输出 Node

```rust
fn write_audit(order: &Order) -> Result<(), BodyError> { /* side effect */ }
flow.then(write_audit, order_ref)?;
```

结构体 Node 也可声明 `Unit` 输出。普通函数返回 `Result<()>` 的支持方式及拒绝时机另见待确认项。

### 1.3 双输入 Node

```rust
fn price(order: &Order, policy: &Policy) -> Result<Quote, BodyError> { /* ... */ }
flow.then(price, (order_ref, policy_ref))?;
```

Node 的参数个数仍按 Node 协议支持范围处理；Flow／Root 输入的 tuple 上限不自动扩大 Node 的函数参数上限。

## 2. Flow

### 2.1 单输入 Flow

```rust
let (mut flow, order) = FlowBuilder::<Order>::start()?;
let valid = flow.then(validate, order)?;
let receipt = flow.then(settle, valid)?;
let flow = flow.finish(receipt)?;
```

`finish(())` 表示显式无业务输出；没有调用 `finish` 的 Builder 仍是不完整定义。

### 2.2 多输入 Flow

```rust
let (mut flow, (order, policy, locale)) =
    FlowBuilder::<(Order, Policy, Locale)>::start()?;

let quote = flow.then(price, (order, policy))?;
let localized = flow.then(render, (quote, locale))?;
let flow = flow.finish(localized)?;
```

希望同一 `FlowBuilder` 接受 1～16 个位置。tuple 表示多个独立输入位置；Flow 不做字段级投影，也不把一个 tuple Data 自动拆成多个位置。

### 2.3 输出形状

```rust
let flow = builder.finish(())?;                 // Unit
let flow = builder.finish(one_data_ref)?;       // Data<O>
let flow = builder.finish((first_ref, second_ref))?; // Out2<O1, O2>
```

完成时检查所选位置是否属于该 Definition、是否已声明、类型是否匹配，以及是否重复选择。Root 输出在执行完成后统一预检，才移交 owned 值。

### 2.4 组合完成态 Flow

```rust
let output = parent.then(child_flow, (input_a, input_b))?;
```

完成态 Flow 可作为 Root，也可作为另一个 Flow 的 child；每次调用使用当前 ExecutionContext，child 的输入与输出通过调用边界显式连接。

## 3. Match

```rust
let mut matcher = MatchBuilder::<Route, Order, Data<Receipt>>::start()?;
matcher.branch(Route::Express, express_flow)?;
matcher.branch(Route::Standard, standard_flow)?;
matcher.default(fallback_node)?;
let matcher = matcher.finish()?;

let receipt = flow.then(matcher, (route_ref, order_ref))?;
```

所有 branch 声明同一个输出 Signature。没有匹配项且没有 default 时执行失败；未选中的 branch 不执行。

## 4. Each

### 4.1 只使用集合 item

```rust
let mut each = EachBuilder::<EachOnly<Span>, Length>::start()?;
each.then_body(length)?;
let each = each.finish()?;

let lengths = flow.then(each, spans_ref)?;
```

每个 item 按顺序借用，body 产生的 owned 输出由 Each 收集为 `Vec<Length>`。业务需要独立结果时由 Node 显式产生新值；collector 不隐式 clone imported Data。

### 4.2 带一个 shared 输入

```rust
let mut each = EachBuilder::<EachShared<Span, Calibration>, Length>::start()?;
each.then_body(measure)?;
let each = each.finish()?;

let lengths = flow.then(each, (spans_ref, calibration_ref))?;
```

`EachOnly`／`EachShared` 是当前公开的形状描述写法；是否保留这些标记或提供更简单的构建语法，待示例整体评审。

## 5. Loop

### 5.1 Iter 状态推进

```rust
impl LoopControl for State {
    fn loop_decision(&self) -> LoopDecision {
        if self.done { LoopDecision::Finish } else { LoopDecision::Continue }
    }
}

let mut loop_builder = LoopBuilder::<Iter1<State>>::start()?;
loop_builder.then_body(advance)?;
let looped = loop_builder.finish()?;

let final_state = flow.then(looped, state_ref)?;
```

每轮处理 current state；Iter 的具体停止语义由 `LoopControl` 明确表达。

### 5.2 Retry

```rust
let mut retry = LoopBuilder::<Retry1<Request, Decision>>::start()?;
retry.then_body(check_request)?;
let retry = retry.finish()?;

let decision = flow.then(retry, request_ref)?;
```

Retry 次数上限与耗尽策略仍属待定语义，示例不预设其最终 API 或结果规则。

## 6. Root Runtime

### 6.1 单输入 Root

```rust
let flow = build_order_flow()?;
let receipt = Runtime::execute(&flow, order).await?;
```

### 6.2 多输入 Root

```rust
let flow = build_order_policy_flow()?;
let receipt = Runtime::execute(&flow, (order, policy, locale)).await?;
```

每次 `execute` 创建新的 Root Execution。Root 成功时返回声明的 owned 输出；失败时不返回部分输出。同步应用可自行使用 executor 驱动 Future。

### 6.3 错误观察

```rust
match Runtime::execute(&flow, input).await {
    Ok(output) => consume(output),
    Err(error) => {
        eprintln!("{}: {}", error.stage(), error.message());
        if let Some(note) = error.business_note() {
            eprintln!("business error: {note}");
        }
    }
}
```

## 7. 复用与执行隔离

```rust
let flow = build_flow()?;
let first = Runtime::execute(&flow, input_a).await?;
let second = Runtime::execute(&flow, input_b).await?;
```

同一完成态 Definition 可重复执行；每次有独立的数据与 Scope 身份。Flow／Node 的 `Clone` 或 `Arc` 不复制、共享或绕过 Execution 内的业务 Data 所有权。

## 8. 需要用示例反查的边界

- 单输入写 `FlowBuilder::<A>`，多输入写 `FlowBuilder::<(A, B, ...)>`；Root 单输入传 `A`，多输入传 tuple。
- Flow／Root 最多 16 个输入；Node 的函数调用 arity 仍按其自身协议限制。
- `then` 与控制器 body 尽量不要求调用方书写 Marker；若某个形状需要显式类型，应由实际推导失败的最小示例证明。
- `EachOnly`／`EachShared` 与 `Iter1`／`Retry1` 是否属于调用方必须看见的类型，需由完整用例判断。
- 普通函数 `Result<()>`、tuple-valued 业务 Data、Flow 无输入、多个 shared 输入、三项以上 Root 输出是否纳入目标 API，需要单独确认；本文不将开放项写成已支持能力。

