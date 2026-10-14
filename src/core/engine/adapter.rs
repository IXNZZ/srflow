//! 类型化 Node Adapter：借用 → Query → await → 类型化登记 → 三路收口（Engine §5）。
//!
//! 具体 `N`／`Input`／`Output` 类型在这里仍然已知：Adapter 从定义登记的静态 Core
//! RefId 解析只读借用、构造 Query 并 await 具体 Future，全部借用结束后再登记 owned
//! 输出。它不创建 Root／Guard／Container，也不把业务输出退化为 `Any` 结果包。

use crate::core::context::{
    BodyError as CoreBodyError, ExecutionContext, InvocationGuard, InvocationKind, TerminationKind,
};
use crate::core::engine::definition::{Definition, NodeStep, StepEntry};
use crate::core::engine::error::{BodyError, ExecFailure};
use crate::core::engine::node::Callable;
use crate::core::engine::shape::{
    BorrowSource, InputOps, InputSpec, NodeOutput, OutputOps, Query, RefShape, ShapeOps,
};
use crate::core::identity::ScopeId;
use crate::core::ref_id::RefId;

/// 在当前 invocation 的 Scope 中执行一个 Node Step。
pub(crate) async fn invoke_node<N, A, K>(
    ctx: &mut ExecutionContext,
    scope: &ScopeId,
    definition: &Definition<'_>,
    step: &NodeStep<N, A, K>,
) -> Result<(), ExecFailure>
where
    N: Callable<A, K>,
    A: RefShape + ShapeOps,
    A::Spec: InputOps,
    N::Output: OutputOps,
    <N::Output as NodeOutput>::Refs: ShapeOps,
    K: 'static,
{
    let core_inputs = resolve_inputs(definition, step);
    let core_outputs = resolve_outputs(definition, step);

    // 输出位置预检在任何登记与业务调用之前完成；失败时业务体不执行。
    for position in &core_outputs {
        if let Err(error) = ctx.precheck_output(scope, position) {
            ctx.terminate(
                TerminationKind::BodyError,
                Some(scope.clone()),
                "node output precheck failed",
                Some(error.clone()),
            );
            return Err(ExecFailure::runtime("node output precheck failed", &error));
        }
    }

    let mut guard = match ctx.enter(InvocationKind::Leaf, scope, false) {
        Ok(guard) => guard,
        Err(error) => return Err(ExecFailure::runtime("node leaf entry rejected", &error)),
    };
    #[cfg(test)]
    crate::core::engine::observe::record_frame(crate::core::engine::observe::FrameRecord {
        kind: "leaf",
        phase: "enter",
        scope: scope.clone(),
    });
    let outcome = call_and_store(&mut guard, scope, step, &core_inputs, &core_outputs).await;
    let result = match outcome {
        Ok(()) => {
            guard.complete();
            Ok(())
        }
        Err(ExecFailure::Control(signal)) => {
            // Leaf 不承担 Scope 退出责任：complete 只结束本 frame，Control 原样返回，
            // 不记录失败、不当作取消。
            guard.complete();
            Err(ExecFailure::Control(signal))
        }
        Err(error) => {
            let note = match &error {
                ExecFailure::Failure(_) => "node returned a body failure",
                _ => "node runtime rejection",
            };
            guard.failed_with(&CoreBodyError::new(note));
            Err(error)
        }
    };
    #[cfg(test)]
    crate::core::engine::observe::record_frame(crate::core::engine::observe::FrameRecord {
        kind: "leaf",
        phase: "exit",
        scope: scope.clone(),
    });
    result
}

/// 借用、调用与登记：Query 与其借用在块内结束，之后才以 `&mut` 登记 owned 输出。
async fn call_and_store<N, A, K>(
    guard: &mut InvocationGuard<'_>,
    scope: &ScopeId,
    step: &NodeStep<N, A, K>,
    core_inputs: &[RefId],
    core_outputs: &[RefId],
) -> Result<(), ExecFailure>
where
    N: Callable<A, K>,
    A: RefShape,
    A::Spec: InputOps,
    N::Output: OutputOps,
    K: 'static,
{
    let produced = {
        let shared: &ExecutionContext = guard;
        let view = <A::Spec as InputOps>::read(BorrowSource {
            ctx: shared,
            scope,
            positions: core_inputs,
        })?;
        let query = Query::<<A::Spec as InputSpec>::Borrowed<'_>>::from_view(view);
        step.invoke(query).await
    };
    match produced {
        Ok(output) => output.store(guard, scope, core_outputs),
        Err(error) => Err(match error {
            BodyError::Control(signal) => ExecFailure::Control(signal),
            BodyError::Failure(failure) => ExecFailure::Failure(failure),
        }),
    }
}

/// 已登记输入位置的静态 Core RefId（构建验证保证存在）。
fn resolve_inputs<N, A, K>(definition: &Definition<'_>, step: &NodeStep<N, A, K>) -> Vec<RefId>
where
    N: Callable<A, K>,
    A: RefShape + ShapeOps,
    A::Spec: InputOps,
    N::Output: OutputOps,
    <N::Output as NodeOutput>::Refs: ShapeOps,
    K: 'static,
{
    step.inputs()
        .iter()
        .map(|desc| {
            definition
                .record(desc.handle)
                .expect("validated node input position")
                .core()
                .clone()
        })
        .collect()
}

/// 已登记输出位置的静态 Core RefId（构建验证保证存在）。
fn resolve_outputs<N, A, K>(definition: &Definition<'_>, step: &NodeStep<N, A, K>) -> Vec<RefId>
where
    N: Callable<A, K>,
    A: RefShape + ShapeOps,
    A::Spec: InputOps,
    N::Output: OutputOps,
    <N::Output as NodeOutput>::Refs: ShapeOps,
    K: 'static,
{
    step.outputs()
        .iter()
        .map(|position| {
            definition
                .record(crate::core::engine::identity::PositionHandle {
                    definition: definition_id_of(definition),
                    position: *position,
                })
                .expect("validated node output position")
                .core()
                .clone()
        })
        .collect()
}

fn definition_id_of(definition: &Definition<'_>) -> crate::core::engine::identity::DefinitionId {
    definition.id()
}
