//! V21-10 验收样本（保留）：Root 提取／关闭权限矩阵。
//!
//! B 阶段只保留 `K17`：它断言 B 阶段保留的 Root Guard／ScopeCoordinator 提取权限矩阵
//! （合法 Root 可冻结／提交／关闭；Leaf／Boundary 以 `OutsideInvocation`、foreign 以
//! `ForeignExecution`、活跃 descendant 以 `ActiveDescendants`、Finalizing／Closed 重入
//! 各自拒绝），删除旧上层协议后仍成立。驱动只提供合法 Root frame，不解释旧 Definition。
use std::cell::RefCell;

use super::context::InvocationKind;
use super::identity::{ExecutionIdentity, ScopeId, ScopeIdAllocator};
use super::internal_error::ScopeError;
use super::ref_id::{RefId, RefIdAllocator, RefIdSource};
use super::scope::ScopeState;
use super::signature::DeclaredPort;
use super::test_support::{RootView, observe_root};

// ---------------------------------------------------------------- 辅助

/// 一个不属于本 Execution 的 `ScopeId`。
fn foreign_scope_id() -> ScopeId {
    let identity = ExecutionIdentity::new();
    let allocator = ScopeIdAllocator::new(identity);
    allocator.allocate().expect("foreign scope")
}

/// 一个未绑定、也不属于任何真实 Definition 的位置。
fn fresh_position() -> RefId {
    let allocator = RefIdAllocator::new(RefIdSource::new());
    allocator.allocate().expect("fresh position")
}

// ---------------------------------------------------------------- K17：Root 提取／关闭权限矩阵

#[test]
fn k17_root_close_permission_matrix() {
    // 合法 Root 路径对照：零声明输出的真实 Root 可以冻结、提交、关闭。
    let mut ok = false;
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        let plan = view
            .guard
            .prepare_root_extraction(&root, &[])
            .expect("合法 Root 路径");
        assert_eq!(plan.take_count(), 0);
        let (values, targets) = view.guard.commit_root_extraction(&root, plan);
        assert!(values.is_empty());
        view.guard.close_root(&root, targets).expect("close");
        assert_eq!(view.state(&root)?, ScopeState::Closed);
        ok = true;
        Ok(())
    });
    assert!(ok, "合法 Root 收口执行");

    // Leaf 沿用 RootScope：不能据此提取。
    let leaf_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        let mut leaf = view.guard.enter(InvocationKind::Leaf, &root, false)?;
        let error = leaf
            .prepare_root_extraction(&root, &[])
            .expect_err("Leaf 不能提取 Root owned 输出");
        *leaf_error.borrow_mut() = Some(error);
        Ok(())
    });
    match leaf_error.borrow().as_ref() {
        Some(ScopeError::OutsideInvocation { .. }) => {}
        other => panic!("expected OutsideInvocation for Leaf, got {other:?}"),
    }

    // Boundary 同样拒绝。
    let boundary_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        // Boundary 建立并拥有一个真实 child Scope，但对 RootScope 没有提取权。
        let child = view.guard.create_child(&root)?;
        let mut boundary = view.guard.enter(InvocationKind::Boundary, &child, true)?;
        let error = boundary
            .prepare_root_extraction(&root, &[])
            .expect_err("Boundary 不能提取 Root owned 输出");
        *boundary_error.borrow_mut() = Some(error);
        drop(boundary);
        Ok(())
    });
    match boundary_error.borrow().as_ref() {
        Some(ScopeError::OutsideInvocation { .. }) => {}
        other => panic!("expected OutsideInvocation for Boundary, got {other:?}"),
    }

    // foreign Root：不属于本 Execution 的 RootScope。
    let foreign = foreign_scope_id();
    let foreign_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let error = view
            .guard
            .prepare_root_extraction(&foreign, &[])
            .expect_err("foreign Root 拒绝");
        *foreign_error.borrow_mut() = Some(error);
        Ok(())
    });
    match foreign_error.borrow().as_ref() {
        Some(ScopeError::ForeignExecution { scope }) => assert_eq!(*scope, foreign),
        other => panic!("expected ForeignExecution, got {other:?}"),
    }

    // 活跃 descendant：冻结之前拒绝。
    let descendant_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        let _child = view.guard.create_child(&root)?;
        let error = view
            .guard
            .prepare_root_extraction(&root, &[])
            .expect_err("仍有活跃 descendant 时拒绝");
        *descendant_error.borrow_mut() = Some(error);
        Ok(())
    });
    match descendant_error.borrow().as_ref() {
        Some(ScopeError::ActiveDescendants { scope }) => assert_eq!(scope.seq(), 0),
        other => panic!("expected ActiveDescendants, got {other:?}"),
    }

    // Finalizing 重入：冻结一次后再次冻结被拒绝。
    let reentry_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        let _plan = view.guard.prepare_root_extraction(&root, &[])?;
        let error = view
            .guard
            .prepare_root_extraction(&root, &[])
            .expect_err("Finalizing 重入拒绝");
        *reentry_error.borrow_mut() = Some(error);
        Ok(())
    });
    match reentry_error.borrow().as_ref() {
        Some(ScopeError::ScopeNotActive { state, .. }) => {
            assert_eq!(*state, ScopeState::Finalizing)
        }
        other => panic!("expected ScopeNotActive, got {other:?}"),
    }

    // Closed 重入：正常收口之后再次提取被拒绝。
    let closed_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        let plan = view.guard.prepare_root_extraction(&root, &[])?;
        let (_, targets) = view.guard.commit_root_extraction(&root, plan);
        view.guard.close_root(&root, targets)?;
        let error = view
            .guard
            .prepare_root_extraction(&root, &[])
            .expect_err("Closed 重入拒绝");
        *closed_error.borrow_mut() = Some(error);
        Ok(())
    });
    match closed_error.borrow().as_ref() {
        Some(ScopeError::ScopeClosed { .. }) => {}
        other => panic!("expected ScopeClosed, got {other:?}"),
    }
}

// ---------------------------------------------------------------- K13：失效目标预检拒绝

/// 保留不变量：Root 某个声明输出位置的目标在提取预检之前失效时，整组以
/// `TargetNotAlive` 在任何 take 之前拒绝，且拒绝不改变 Root 的 refs／owned（take 数为零）。
///
/// 迁移自旧 `k13_dead_selected_target_is_rejected`（旧链经 `FlowBuilder`＋
/// `install_root_fault` 外壳）；这里直接调用 B 阶段保留的 Core 入口
/// `ScopeCoordinator::prepare_root_extraction`，用正式 `register_owned`＋`destroy_probe`
/// 破坏真实前提，不复制第二个提取算法。
#[test]
fn k13_dead_selected_target_is_rejected() {
    let mut observed = false;
    let _ = observe_root(|view: &mut RootView<'_, '_>| {
        let root = view.root().clone();
        // 用一个真实 owned 值占住一个 Root 本地位置。
        let position = fresh_position();
        let id = view.guard.register_owned::<u32>(&root, &position, 7)?;
        // 预检之前销毁该物理实例：refs 仍指向它，但已无存活目标。
        view.guard.destroy_probe(&id);
        let before = view.guard.snapshot_targets_probe(&root)?;

        let ports = [DeclaredPort::with_type(
            position.clone(),
            std::any::TypeId::of::<u32>(),
            std::any::type_name::<u32>(),
        )];
        let error = view
            .guard
            .prepare_root_extraction(&root, &ports)
            .expect_err("失效目标必须在任何 take 之前被拒绝");
        assert!(
            matches!(error, ScopeError::TargetNotAlive { position: ref dead } if dead == &position),
            "expected TargetNotAlive, got {error:?}"
        );

        // 拒绝前后 Root 的 refs／owned 逐项相等：没有任何 take／责任变化。
        let after = view.guard.snapshot_targets_probe(&root)?;
        assert_eq!(before, after, "拒绝不改变 Root refs／owned（take 数为零）");
        observed = true;
        Ok(())
    });
    assert!(observed, "失效目标样本已执行");
}
