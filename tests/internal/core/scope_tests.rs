use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    CollectorRecord, ControlState, ControlStateId, ExportSlot, ImportSlot, RefTarget, Scope,
    ScopeCoordinator, ScopeRegistry, ScopeState, StateImportSlot,
};
use crate::core::identity::{CollectorId, DataId, ExecutionIdentity, ScopeId, ScopeIdAllocator};
use crate::core::internal_error::{IdKind, InternalError, ScopeError};
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};

/// 非 Clone 业务值，带 Drop 观测。
struct Tracked {
    num: u32,
    drops: Arc<AtomicUsize>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 另一类型，用于类型不符与异构多输出。
struct Other(u32);

struct Fixture {
    coordinator: ScopeCoordinator,
    ids: RefIdAllocator,
    drops: Arc<AtomicUsize>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            coordinator: ScopeCoordinator::new(ExecutionIdentity::new()),
            ids: RefIdAllocator::new(RefIdSource::new()),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn ref_id(&self) -> RefId {
        self.ids.allocate().unwrap()
    }

    fn tracked(&self, num: u32) -> Tracked {
        Tracked {
            num,
            drops: Arc::clone(&self.drops),
        }
    }

    fn root(&self) -> ScopeId {
        self.coordinator.root()
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }

    /// 以单输出声明调用正常退出：声明位置即该 slot 的 child 位置。
    fn finalize_one<T: Any>(
        &mut self,
        scope: &ScopeId,
        child: &RefId,
        caller: &RefId,
    ) -> Result<(), ScopeError> {
        let mut slots = vec![ExportSlot::new::<T>(child, caller)];
        self.coordinator
            .finalize(scope, std::slice::from_ref(child), &mut slots)
    }

    /// 以显式多输出声明调用正常退出。
    fn finalize_many(
        &mut self,
        scope: &ScopeId,
        declared: &[RefId],
        mut slots: Vec<ExportSlot>,
    ) -> Result<(), ScopeError> {
        self.coordinator.finalize(scope, declared, &mut slots)
    }

    /// 以显式无输出调用正常退出。
    fn finalize_none(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        let mut slots: Vec<ExportSlot> = Vec::new();
        self.coordinator.finalize(scope, &[], &mut slots)
    }

    fn owner(&self, id: &DataId) -> Result<ScopeId, ScopeError> {
        self.coordinator.owner_of(id)
    }

    fn child(&mut self, parent: &ScopeId) -> ScopeId {
        self.coordinator.create_child(parent).unwrap()
    }

    /// 以本地引用初始化控制状态。
    fn state<T: Any>(&mut self, controller: &ScopeId, from: &RefId) -> ControlStateId {
        self.coordinator
            .register_state::<T>(controller, from)
            .unwrap()
    }

    /// 登记尚未初始化的控制状态。
    fn uninit_state<T: Any>(&mut self, controller: &ScopeId) -> ControlStateId {
        self.coordinator
            .register_uninitialized_state::<T>(controller)
            .unwrap()
    }

    fn collector<O: Any>(&mut self, owner: &ScopeId) -> CollectorId {
        self.coordinator.begin_collector::<O>(owner).unwrap()
    }

    /// 是否仍能按类型读取该 DataId（用于观测旧身份是否失效）。
    fn readable_as<T: Any>(&self, id: &DataId) -> bool {
        self.coordinator.container.borrow::<T>(id).is_ok()
    }

    fn alive(&self, id: &DataId) -> bool {
        self.coordinator.container.validate(id).is_ok()
    }

    fn scope_state(&self, scope: &ScopeId) -> ScopeState {
        self.coordinator.state(scope).unwrap()
    }

    fn refs_len(&self, scope: &ScopeId) -> usize {
        self.coordinator.registry.lookup(scope).unwrap().refs.len()
    }

    fn owned_len(&self, scope: &ScopeId) -> usize {
        self.coordinator.registry.lookup(scope).unwrap().owned.len()
    }

    /// 测试专用故障：把一个 target 直接注入 Scope 的本地位置。
    fn inject_target(&mut self, scope: &ScopeId, position: &RefId, target: RefTarget) {
        self.coordinator
            .registry
            .lookup_mut(scope)
            .unwrap()
            .refs
            .insert(position.clone(), target);
    }

    /// 测试专用故障：撤销一个 Scope 的 owned 责任记录（entry 仍在容器中）。
    fn drop_ownership(&mut self, scope: &ScopeId, id: &DataId) {
        assert!(
            self.coordinator
                .registry
                .lookup_mut(scope)
                .unwrap()
                .owned
                .remove(id)
        );
    }

    /// 测试专用故障：把责任记录加到一个 Scope 上，制造重复 owner。
    fn add_ownership(&mut self, scope: &ScopeId, id: &DataId) {
        assert!(
            self.coordinator
                .registry
                .lookup_mut(scope)
                .unwrap()
                .owned
                .insert(id.clone())
        );
    }
}

#[test]
fn b01_execution_identity_is_shared_and_foreign_scopes_are_rejected_first() {
    let mut first = Fixture::new();
    let mut second = Fixture::new();

    // 两个 Execution 的 RootScope 序号相同，各自的第一个 child 序号也相同。
    assert_eq!(first.root().seq(), second.root().seq());
    let child = first.coordinator.create_child(&first.root()).unwrap();
    let second_child = second.coordinator.create_child(&second.root()).unwrap();
    assert_eq!(child.seq(), second_child.seq());

    // 来源校验先于查表：不会误命中第二份 registry 的同序号记录。
    assert!(matches!(
        second.coordinator.state(&first.root()),
        Err(ScopeError::ForeignExecution { .. })
    ));
    assert!(matches!(
        second.coordinator.create_child(&first.root()),
        Err(ScopeError::ForeignExecution { .. })
    ));

    // 只有 Active parent 能建立 child；关闭后的身份不复用。
    first.finalize_none(&child).unwrap();
    assert_eq!(first.coordinator.state(&child).unwrap(), ScopeState::Closed);
    assert!(matches!(
        first.coordinator.create_child(&child),
        Err(ScopeError::ScopeClosed { .. })
    ));
    let next = first.coordinator.create_child(&first.root()).unwrap();
    assert_ne!(next.seq(), child.seq());

    // foreign DataId 的拒绝发生在存储校验，且诊断是"来源不符"而不是"不存在"。
    let foreign = second
        .coordinator
        .register_owned(&second.root(), &second.ref_id(), Other(9))
        .unwrap();
    let position = first.ref_id();
    first
        .coordinator
        .registry
        .lookup_mut(&first.root())
        .unwrap()
        .refs
        .insert(position.clone(), RefTarget::Data(foreign));
    assert!(matches!(
        first.coordinator.resolve::<Other>(&first.root(), &position),
        Err(ScopeError::Storage {
            source: InternalError::ForeignExecution { .. }
        })
    ));
}

#[test]
fn b02_definition_ref_ids_are_reusable_across_independent_scopes() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let first_child = fixture.coordinator.create_child(&root).unwrap();
    let second_child = fixture.coordinator.create_child(&root).unwrap();

    // 同一 Definition 逻辑位置在两个 child 实例中分别绑定到不同 DataId。
    let shared_position = fixture.ref_id();
    let first_id = fixture
        .coordinator
        .register_owned(&first_child, &shared_position, fixture.tracked(1))
        .unwrap();
    let second_id = fixture
        .coordinator
        .register_owned(&second_child, &shared_position, fixture.tracked(2))
        .unwrap();

    assert_ne!(first_id, second_id);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&first_child, &shared_position)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&second_child, &shared_position)
            .unwrap()
            .num,
        2
    );
    // 两个实例的本地 refs 互不覆写，各自登记一个位置。
    assert_eq!(
        fixture
            .coordinator
            .registry
            .lookup(&first_child)
            .unwrap()
            .refs
            .len(),
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .registry
            .lookup(&second_child)
            .unwrap()
            .refs
            .len(),
        1
    );
}

#[test]
fn b03_local_binding_is_single_assignment_and_aliasable() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let position = fixture.ref_id();
    let first_alias = fixture.ref_id();
    let second_alias = fixture.ref_id();

    let d1 = fixture
        .coordinator
        .register_owned(&root, &position, fixture.tracked(1))
        .unwrap();

    // 重绑同一位置（即使目标相同）在修改前拒绝，原绑定可读。
    assert!(matches!(
        fixture
            .coordinator
            .register_owned(&root, &position, fixture.tracked(2)),
        Err(ScopeError::RefAlreadyBound { .. })
    ));
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &position)
            .unwrap()
            .num,
        1
    );

    // 不同位置 alias 同一 DataId：两个位置都可读，责任仍只有一份。
    let child = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[
                ImportSlot::new::<Tracked>(&position, &first_alias),
                ImportSlot::new::<Tracked>(&position, &second_alias),
            ],
        )
        .unwrap();
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &first_alias)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &second_alias)
            .unwrap()
            .num,
        1
    );
    assert_eq!(fixture.owner(&d1).unwrap(), root);
}

#[test]
fn b04_import_grants_visibility_without_ownership() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let parent_position = fixture.ref_id();
    let child_position = fixture.ref_id();
    let sibling_position = fixture.ref_id();

    let d1 = fixture
        .coordinator
        .register_owned(&root, &parent_position, fixture.tracked(1))
        .unwrap();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let sibling = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .register_owned(&sibling, &sibling_position, fixture.tracked(2))
        .unwrap();

    // 未导入时 child 不能通过位置读取；不搜索 ancestor.refs，也看不到 sibling 的位置。
    assert!(matches!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &parent_position),
        Err(ScopeError::RefNotBound { .. })
    ));
    assert!(matches!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &sibling_position),
        Err(ScopeError::RefNotBound { .. })
    ));

    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Tracked>(
                &parent_position,
                &child_position,
            )],
        )
        .unwrap();

    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &child_position)
            .unwrap()
            .num,
        1
    );
    // 导入不增加 owner：责任仍在 root。
    assert_eq!(fixture.owner(&d1).unwrap(), root);
}

#[test]
fn b05_batch_import_failure_leaves_no_partial_binding() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let caller_position = fixture.ref_id();
    let good_target = fixture.ref_id();
    let bad_target = fixture.ref_id();

    fixture
        .coordinator
        .register_owned(&root, &caller_position, fixture.tracked(1))
        .unwrap();

    // 第二项类型不符：第一项也不能留下绑定。
    let error = fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[
                ImportSlot::new::<Tracked>(&caller_position, &good_target),
                ImportSlot::new::<Other>(&caller_position, &bad_target),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, ScopeError::TypeMismatch { .. }));
    assert!(matches!(
        fixture.coordinator.resolve::<Tracked>(&child, &good_target),
        Err(ScopeError::RefNotBound { .. })
    ));
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&child)
            .unwrap()
            .refs
            .is_empty()
    );

    // 第二项重复目标位置同样整组拒绝。
    assert!(matches!(
        fixture.coordinator.import_batch(
            &child,
            &root,
            &[
                ImportSlot::new::<Tracked>(&caller_position, &good_target),
                ImportSlot::new::<Tracked>(&caller_position, &good_target),
            ],
        ),
        Err(ScopeError::DuplicatePosition { .. })
    ));
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&child)
            .unwrap()
            .refs
            .is_empty()
    );
}

#[test]
fn b06_registration_rejects_before_insert_and_survives_exhaustion() {
    // 用近上限的全新 Execution 身份同时建立 registry 与 Container。
    let execution = ExecutionIdentity::with_starts(u64::MAX - 1, 0);
    let mut coordinator = ScopeCoordinator::new(execution);
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let root = coordinator.root();

    let first = ids.allocate().unwrap();
    let second = ids.allocate().unwrap();
    let id = coordinator
        .register_owned(
            &root,
            &first,
            Tracked {
                num: 1,
                drops: Arc::clone(&drops),
            },
        )
        .unwrap();
    assert_eq!(id.seq(), u64::MAX - 1);

    // 序号耗尽：插入失败不留 refs／owned，也不影响原值与责任。
    assert!(matches!(
        coordinator.register_owned(
            &root,
            &second,
            Tracked {
                num: 2,
                drops: Arc::clone(&drops)
            }
        ),
        Err(ScopeError::Storage {
            source: InternalError::IdSpaceExhausted { .. }
        })
    ));
    {
        let record = coordinator.registry.lookup(&root).unwrap();
        assert_eq!(record.owned.len(), 1);
        assert_eq!(record.refs.len(), 1);
    }
    assert_eq!(
        coordinator.resolve::<Tracked>(&root, &first).unwrap().num,
        1
    );
    // 失败插入的输入值随拒绝一同 drop，不计入已登记值。
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    // 非 Active Scope 在插入前拒绝。
    coordinator.registry.lookup_mut(&root).unwrap().state = ScopeState::Finalizing;
    assert!(matches!(
        coordinator.register_owned(
            &root,
            &ids.allocate().unwrap(),
            Tracked {
                num: 3,
                drops: Arc::clone(&drops)
            }
        ),
        Err(ScopeError::ScopeNotActive { .. })
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn b07_p02_normal_export_commits_binding_and_responsibility() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let parent_position = fixture.ref_id();
    let imported = fixture.ref_id();
    let child_output = fixture.ref_id();
    let caller_position = fixture.ref_id();
    let temporary = fixture.ref_id();

    let d1 = fixture
        .coordinator
        .register_owned(&root, &parent_position, fixture.tracked(1))
        .unwrap();
    let child = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Tracked>(&parent_position, &imported)],
        )
        .unwrap();
    let d2 = fixture
        .coordinator
        .register_owned(&child, &child_output, fixture.tracked(2))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &temporary, fixture.tracked(3))
        .unwrap();

    fixture
        .finalize_one::<Tracked>(&child, &child_output, &caller_position)
        .unwrap();

    // 只有未导出的临时值被清理。
    assert_eq!(fixture.drops(), 1);
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    // caller 可读 D1／D2；责任转移完成。
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &parent_position)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_position)
            .unwrap()
            .num,
        2
    );
    assert_eq!(fixture.owner(&d1).unwrap(), root);
    assert_eq!(fixture.owner(&d2).unwrap(), root);
    // child 的本地引用随 Closed 失效。
    assert!(matches!(
        fixture.coordinator.resolve::<Tracked>(&child, &imported),
        Err(ScopeError::ScopeClosed { .. })
    ));

    // parent 退出时清理自己负责的 D1／D2 各一次。
    fixture.finalize_none(&root).unwrap();
    assert_eq!(fixture.drops(), 3);
}

#[test]
fn b08_exported_imported_alias_keeps_the_original_owner() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let root_position = fixture.ref_id();
    let middle_position = fixture.ref_id();
    let caller_position = fixture.ref_id();

    let d1 = fixture
        .coordinator
        .register_owned(&root, &root_position, fixture.tracked(1))
        .unwrap();
    let middle = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .import_batch(
            &middle,
            &root,
            &[ImportSlot::new::<Tracked>(&root_position, &middle_position)],
        )
        .unwrap();

    // 中间 Scope 原样再输出 imported Data。
    fixture
        .finalize_one::<Tracked>(&middle, &middle_position, &caller_position)
        .unwrap();

    assert_eq!(fixture.drops(), 0);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_position)
            .unwrap()
            .num,
        1
    );
    // 原 owner 不变，也没有第二个 owner。
    assert_eq!(fixture.owner(&d1).unwrap(), root);
}

#[test]
fn b09_alias_outputs_transfer_responsibility_once() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child_first = fixture.ref_id();
    let child_second = fixture.ref_id();
    let caller_first = fixture.ref_id();
    let caller_second = fixture.ref_id();

    let child = fixture.coordinator.create_child(&root).unwrap();
    let d2 = fixture
        .coordinator
        .register_owned(&child, &child_first, fixture.tracked(1))
        .unwrap();
    // 两个不同输出位置指向同一 child-owned DataId。
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(child_second.clone(), RefTarget::Data(d2.clone()));

    fixture
        .finalize_many(
            &child,
            &[child_first.clone(), child_second.clone()],
            vec![
                ExportSlot::new::<Tracked>(&child_first, &caller_first),
                ExportSlot::new::<Tracked>(&child_second, &caller_second),
            ],
        )
        .unwrap();

    assert_eq!(fixture.drops(), 0);
    assert_eq!(fixture.owner(&d2).unwrap(), root);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_first)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_second)
            .unwrap()
            .num,
        1
    );

    // 责任只转移一次：parent 清理只 drop 一次。
    fixture.finalize_none(&root).unwrap();
    assert_eq!(fixture.drops(), 1);
}

#[test]
fn b10_heterogeneous_export_rejects_before_any_commit() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let tracked_position = fixture.ref_id();
    let other_position = fixture.ref_id();
    let caller_tracked = fixture.ref_id();
    let caller_other = fixture.ref_id();

    let child = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .register_owned(&child, &tracked_position, fixture.tracked(1))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &other_position, Other(7))
        .unwrap();

    // 异构两输出正常导出。
    fixture
        .finalize_many(
            &child,
            &[tracked_position.clone(), other_position.clone()],
            vec![
                ExportSlot::new::<Tracked>(&tracked_position, &caller_tracked),
                ExportSlot::new::<Other>(&other_position, &caller_other),
            ],
        )
        .unwrap();
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_tracked)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Other>(&root, &caller_other)
            .unwrap()
            .0,
        7
    );

    // 第二项类型不符：合法第一项不得提前提交。
    let child = fixture.coordinator.create_child(&root).unwrap();
    let first_output = fixture.ref_id();
    let second_output = fixture.ref_id();
    let first_caller = fixture.ref_id();
    let second_caller = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&child, &first_output, fixture.tracked(2))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &second_output, fixture.tracked(3))
        .unwrap();

    let error = fixture
        .coordinator
        .prepare_export(
            &child,
            &root,
            &[first_output.clone(), second_output.clone()],
            &[
                ExportSlot::new::<Tracked>(&first_output, &first_caller),
                ExportSlot::new::<Other>(&second_output, &second_caller),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, ScopeError::TypeMismatch { .. }));
    assert!(
        !fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .contains_key(&first_caller)
    );
    assert!(matches!(
        fixture.coordinator.resolve::<Tracked>(&root, &first_caller),
        Err(ScopeError::RefNotBound { .. })
    ));
    assert_eq!(fixture.drops(), 0);
}

#[test]
fn b11_caller_position_conflicts_and_illegal_relations_are_rejected() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let sibling = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let other_output = fixture.ref_id();
    let bound_caller = fixture.ref_id();

    fixture
        .coordinator
        .register_owned(&child, &output, fixture.tracked(1))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &other_output, fixture.tracked(3))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&root, &bound_caller, fixture.tracked(2))
        .unwrap();

    // caller 位置已绑定。
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Tracked>(&output, &bound_caller)],
        ),
        Err(ScopeError::RefAlreadyBound { .. })
    ));

    // 本批重复 caller 位置：第二个 slot 才会撞上重复。
    let fresh_caller = fixture.ref_id();
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            &[output.clone(), other_output.clone()],
            &[
                ExportSlot::new::<Tracked>(&output, &fresh_caller),
                ExportSlot::new::<Tracked>(&other_output, &fresh_caller),
            ],
        ),
        Err(ScopeError::DuplicatePosition { .. })
    ));

    // 非直接 parent。
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &sibling,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::NotDirectParent { .. })
    ));

    // caller 非 Active。
    fixture
        .coordinator
        .registry
        .lookup_mut(&root)
        .unwrap()
        .state = ScopeState::Finalizing;
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::ScopeNotActive { .. })
    ));
}

#[test]
fn b12_target_and_owner_faults_are_rejected_before_commit() {
    // 失效 target。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let dead = fixture
        .coordinator
        .register_owned(&child, &output, fixture.tracked(1))
        .unwrap();
    fixture.coordinator.container.destroy(&dead).unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .owned
        .remove(&dead);
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::TargetNotAlive { .. })
    ));

    // foreign target。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let mut other = Fixture::new();
    let foreign = other
        .coordinator
        .register_owned(&other.root(), &other.ref_id(), Other(1))
        .unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(output.clone(), RefTarget::Data(foreign));
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::Storage {
            source: InternalError::ForeignExecution { .. }
        })
    ));

    // 无 owner。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let orphan = fixture
        .coordinator
        .container
        .insert_owned(Other(2))
        .unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(output.clone(), RefTarget::Data(orphan));
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::NoOwner { .. })
    ));

    // sibling owner。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let exporter = fixture.coordinator.create_child(&root).unwrap();
    let sibling = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let sibling_position = fixture.ref_id();
    let sibling_data = fixture
        .coordinator
        .register_owned(&sibling, &sibling_position, Other(3))
        .unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&exporter)
        .unwrap()
        .refs
        .insert(output.clone(), RefTarget::Data(sibling_data));
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &exporter,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::IllegalOwner { .. })
    ));

    // 重复 owner。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let exporter = fixture.coordinator.create_child(&root).unwrap();
    let sibling = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let sibling_position = fixture.ref_id();
    let shared = fixture
        .coordinator
        .register_owned(&sibling, &sibling_position, Other(4))
        .unwrap();
    {
        let record = fixture.coordinator.registry.lookup_mut(&exporter).unwrap();
        record.owned.insert(shared.clone());
        record.refs.insert(output.clone(), RefTarget::Data(shared));
    }
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &exporter,
            &root,
            std::slice::from_ref(&output),
            &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
        ),
        Err(ScopeError::DuplicateOwner { .. })
    ));
}

#[test]
fn b13_precommit_snapshot_and_formal_failure_exit() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let root_position = fixture.ref_id();
    let imported = fixture.ref_id();
    let first_output = fixture.ref_id();
    let second_output = fixture.ref_id();
    let first_caller = fixture.ref_id();
    let second_caller = fixture.ref_id();

    // ancestor-owned 值显式导入 child：失败退出不得动它。
    fixture
        .coordinator
        .register_owned(&root, &root_position, fixture.tracked(9))
        .unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Tracked>(&root_position, &imported)],
        )
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &first_output, fixture.tracked(1))
        .unwrap();
    let d2 = fixture
        .coordinator
        .register_owned(&child, &second_output, fixture.tracked(2))
        .unwrap();

    // 私有预检路径：失败瞬间 caller 无新绑定，D2 仍由 child 负责。
    let error = fixture
        .coordinator
        .prepare_export(
            &child,
            &root,
            &[first_output.clone(), second_output.clone()],
            &[
                ExportSlot::new::<Tracked>(&first_output, &first_caller),
                ExportSlot::new::<Other>(&second_output, &second_caller),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, ScopeError::TypeMismatch { .. }));
    assert!(
        !fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .contains_key(&first_caller)
    );
    assert_eq!(fixture.owner(&d2).unwrap(), child);
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Active
    );
    assert_eq!(fixture.drops(), 0);

    // 正式失败出口：清理 child 自身数据并返回原诊断，ancestor 值保留。
    let error = fixture
        .finalize_many(
            &child,
            &[first_output.clone(), second_output.clone()],
            vec![
                ExportSlot::new::<Tracked>(&first_output, &first_caller),
                ExportSlot::new::<Other>(&second_output, &second_caller),
            ],
        )
        .unwrap_err();
    assert!(matches!(error, ScopeError::TypeMismatch { .. }));
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(fixture.drops(), 2);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &root_position)
            .unwrap()
            .num,
        9
    );
    // 同 §7：RefId 的 Eq／Hash 只用来源地址与序号，键语义不随分配变化。
    #[allow(clippy::mutable_key_type)]
    fn caller_bindings(fixture: &Fixture, root: &ScopeId, first: &RefId, second: &RefId) -> bool {
        let refs = &fixture.coordinator.registry.lookup(root).unwrap().refs;
        !refs.contains_key(first) && !refs.contains_key(second)
    }
    assert!(caller_bindings(
        &fixture,
        &root,
        &first_caller,
        &second_caller
    ));
}
#[test]
fn b14_finalizing_and_closed_states_are_distinguishable() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let position = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&child, &position, fixture.tracked(1))
        .unwrap();

    // Finalizing 禁止新 child／导入／登记／业务 resolve，但允许内部输出预检。
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .state = ScopeState::Finalizing;
    let unrelated = fixture.ref_id();
    assert!(matches!(
        fixture.coordinator.create_child(&child),
        Err(ScopeError::ScopeNotActive { .. })
    ));
    assert!(matches!(
        fixture
            .coordinator
            .register_owned(&child, &unrelated, fixture.tracked(2)),
        Err(ScopeError::ScopeNotActive { .. })
    ));
    assert!(matches!(
        fixture.coordinator.resolve::<Tracked>(&child, &position),
        Err(ScopeError::ScopeNotActive { .. })
    ));
    assert!(matches!(
        fixture.coordinator.import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Tracked>(&unrelated, &unrelated)],
        ),
        Err(ScopeError::ScopeNotActive { .. })
    ));
    // 内部输出校验仍可用。
    fixture
        .coordinator
        .prepare_export(
            &child,
            &root,
            std::slice::from_ref(&position),
            &[ExportSlot::new::<Tracked>(&position, &fixture.ref_id())],
        )
        .unwrap();

    // 从 Finalizing 完成退出清理。
    let mut consumed: Vec<ExportSlot> = Vec::new();
    fixture
        .coordinator
        .complete_exit(&child, &[], &mut consumed)
        .unwrap();
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    // 已关闭身份 → Closed；同次 Execution 已分配但未登记的身份 → 不存在。
    assert!(matches!(
        fixture.coordinator.resolve::<Tracked>(&child, &position),
        Err(ScopeError::ScopeClosed { .. })
    ));
    let unregistered = {
        let ids = ScopeIdAllocator::new(fixture.coordinator.registry.execution.clone());
        ids.allocate().unwrap();
        ids.allocate().unwrap()
    };
    assert!(matches!(
        fixture.coordinator.state(&unregistered),
        Err(ScopeError::ScopeNotFound { .. })
    ));
    // 关闭后的 tombstone 不再保留 refs／owned／活跃子关系。
    let record = fixture.coordinator.registry.lookup(&child).unwrap();
    assert!(record.refs.is_empty() && record.owned.is_empty() && record.children.is_empty());
}

#[test]
fn b15_empty_output_set_is_a_normal_exit() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    fixture
        .coordinator
        .register_owned(&child, &fixture.ref_id(), fixture.tracked(1))
        .unwrap();

    fixture.finalize_none(&child).unwrap();

    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(fixture.drops(), 1);
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .is_empty()
    );
}

#[test]
fn b16_active_descendants_block_normal_exit() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let grandchild = fixture.coordinator.create_child(&child).unwrap();

    assert!(matches!(
        fixture.finalize_none(&child),
        Err(ScopeError::ActiveDescendants { .. })
    ));
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Active
    );
    assert_eq!(
        fixture.coordinator.state(&grandchild).unwrap(),
        ScopeState::Active
    );

    fixture.finalize_none(&grandchild).unwrap();
    fixture.finalize_none(&child).unwrap();
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
}

#[test]
fn b17_multi_level_failure_cleanup_is_post_order() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let root_position = fixture.ref_id();
    let parent = fixture.coordinator.create_child(&root).unwrap();
    let imported_in_parent = fixture.ref_id();
    let parent_position = fixture.ref_id();
    let child = fixture.coordinator.create_child(&parent).unwrap();
    let imported_in_child = fixture.ref_id();
    let child_position = fixture.ref_id();

    fixture
        .coordinator
        .register_owned(&root, &root_position, fixture.tracked(1))
        .unwrap();
    fixture
        .coordinator
        .import_batch(
            &parent,
            &root,
            &[ImportSlot::new::<Tracked>(
                &root_position,
                &imported_in_parent,
            )],
        )
        .unwrap();
    fixture
        .coordinator
        .register_owned(&parent, &parent_position, fixture.tracked(2))
        .unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &parent,
            &[ImportSlot::new::<Tracked>(
                &imported_in_parent,
                &imported_in_child,
            )],
        )
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &child_position, fixture.tracked(3))
        .unwrap();

    fixture.coordinator.abort(&parent).unwrap();

    // 三层中的 parent 与 child 全部关闭，未导出的自有数据各 drop 一次。
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(
        fixture.coordinator.state(&parent).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(fixture.drops(), 2);
    assert!(matches!(
        fixture
            .coordinator
            .resolve::<Tracked>(&child, &imported_in_child),
        Err(ScopeError::ScopeClosed { .. })
    ));
    // 仍存活的 ancestor 值未被清理,root 也未脱离管理。
    assert_eq!(
        fixture.coordinator.state(&root).unwrap(),
        ScopeState::Active
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &root_position)
            .unwrap()
            .num,
        1
    );
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .children
            .is_empty()
    );
}
#[test]
fn b18_received_values_are_disposed_by_the_receiving_scope() {
    // 两种退出路径都观测"接收方处置转交值、仍存活的 ancestor 值保留"。
    for use_abort in [false, true] {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let root_position = fixture.ref_id();
        let parent = fixture.coordinator.create_child(&root).unwrap();
        let imported_in_parent = fixture.ref_id();
        let child = fixture.coordinator.create_child(&parent).unwrap();
        let imported_in_child = fixture.ref_id();
        let child_output = fixture.ref_id();
        let parent_position = fixture.ref_id();

        fixture
            .coordinator
            .register_owned(&root, &root_position, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &parent,
                &root,
                &[ImportSlot::new::<Tracked>(
                    &root_position,
                    &imported_in_parent,
                )],
            )
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &parent,
                &[ImportSlot::new::<Tracked>(
                    &imported_in_parent,
                    &imported_in_child,
                )],
            )
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &child_output, fixture.tracked(2))
            .unwrap();

        // child 把自有值转交 parent 后关闭；接收方与 ancestor 均存活时未发生 Drop。
        fixture
            .finalize_one::<Tracked>(&child, &child_output, &parent_position)
            .unwrap();
        assert_eq!(fixture.drops(), 0);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&parent, &parent_position)
                .unwrap()
                .num,
            2
        );

        // parent 正常退出或失败清理：只处置转交来的值一次，ancestor 值保留。
        if use_abort {
            fixture.coordinator.abort(&parent).unwrap();
        } else {
            fixture.finalize_none(&parent).unwrap();
        }
        assert_eq!(fixture.drops(), 1);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &root_position)
                .unwrap()
                .num,
            1
        );
        assert!(
            !fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .is_empty()
        );

        // Container 析构只清理 root 自己的值，不再重复 Drop。
        let drops = Arc::clone(&fixture.drops);
        let coordinator = fixture.coordinator;
        drop(coordinator);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn b19_shortening_the_borrow_allows_exit_and_cleanup() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let position = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&root, &position, fixture.tracked(1))
        .unwrap();

    // 缩短借用后：正常退出入口可运行。
    {
        let borrowed = fixture
            .coordinator
            .resolve::<Tracked>(&root, &position)
            .unwrap();
        assert_eq!(borrowed.num, 1);
    }
    fixture.finalize_none(&root).unwrap();
    assert_eq!(fixture.drops(), 1);

    // 缩短借用后：失败清理入口可运行。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let position = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&root, &position, fixture.tracked(2))
        .unwrap();
    {
        let borrowed = fixture
            .coordinator
            .resolve::<Tracked>(&root, &position)
            .unwrap();
        assert_eq!(borrowed.num, 2);
    }
    fixture.coordinator.abort(&root).unwrap();
    assert_eq!(fixture.drops(), 1);
}

#[test]
fn r13_failure_and_abort_must_not_destroy_ancestor_values() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let root_position = fixture.ref_id();
    let imported = fixture.ref_id();
    let caller_position = fixture.ref_id();

    let ancestor = fixture
        .coordinator
        .register_owned(&root, &root_position, fixture.tracked(1))
        .unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Tracked>(&root_position, &imported)],
        )
        .unwrap();
    // 故障构造：child 重复认领 imported ancestor 值,破坏唯一责任。
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .owned
        .insert(ancestor.clone());

    // 正式失败入口：保留原导出诊断,且不授予销毁权。
    let error = fixture
        .finalize_one::<Tracked>(&child, &imported, &caller_position)
        .unwrap_err();
    assert!(matches!(error, ScopeError::DuplicateOwner { .. }));
    assert_eq!(fixture.drops(), 0);
    assert!(fixture.coordinator.container.validate(&ancestor).is_ok());
    // caller refs 不变：没有新增输出绑定。
    {
        let root_record = fixture.coordinator.registry.lookup(&root).unwrap();
        assert!(root_record.refs.contains_key(&root_position));
        assert!(!root_record.refs.contains_key(&caller_position));
    }
    // 破坏状态不被静默清理：child 未冒充 Closed,其 owned 仍保留注入项。
    {
        let child_record = fixture.coordinator.registry.lookup(&child).unwrap();
        assert_ne!(child_record.state, ScopeState::Closed);
        assert!(child_record.owned.contains(&ancestor));
    }

    // 破坏状态不冒充正常 Closed：后续退出尝试以责任诊断报出，仍不销毁任何值。
    assert!(matches!(
        fixture.coordinator.abort(&child),
        Err(ScopeError::DuplicateOwner { .. }) | Err(ScopeError::Invariant { .. })
    ));
    assert_eq!(fixture.drops(), 0);
    assert!(fixture.coordinator.container.validate(&ancestor).is_ok());

    // 恢复被破坏的责任后,ancestor 值仍完整可读（证明未被销毁）。
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .owned
        .remove(&ancestor);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &root_position)
            .unwrap()
            .num,
        1
    );
}

#[test]
fn r14_resolve_requires_a_legal_responsibility_chain() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let sibling = fixture.coordinator.create_child(&root).unwrap();
    let own_position = fixture.ref_id();
    let root_position = fixture.ref_id();
    let imported = fixture.ref_id();
    let forged = fixture.ref_id();
    let orphan_position = fixture.ref_id();
    let duplicated_position = fixture.ref_id();
    let dead_position = fixture.ref_id();

    // 合法本地登记与合法导入都可读。
    fixture
        .coordinator
        .register_owned(&child, &own_position, Other(1))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&root, &root_position, Other(2))
        .unwrap();
    fixture
        .coordinator
        .import_batch(
            &child,
            &root,
            &[ImportSlot::new::<Other>(&root_position, &imported)],
        )
        .unwrap();
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Other>(&child, &own_position)
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Other>(&child, &imported)
            .unwrap()
            .0,
        2
    );

    // sibling-owned：注入 child 本地位置后必须拒绝。
    let sibling_position = fixture.ref_id();
    let sibling_id = fixture
        .coordinator
        .register_owned(&sibling, &sibling_position, Other(3))
        .unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(forged.clone(), RefTarget::Data(sibling_id.clone()));
    assert!(matches!(
        fixture.coordinator.resolve::<Other>(&child, &forged),
        Err(ScopeError::IllegalOwner { .. })
    ));

    // 无 owner。
    let orphan = fixture
        .coordinator
        .container
        .insert_owned(Other(4))
        .unwrap();
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(orphan_position.clone(), RefTarget::Data(orphan));
    assert!(matches!(
        fixture
            .coordinator
            .resolve::<Other>(&child, &orphan_position),
        Err(ScopeError::NoOwner { .. })
    ));

    // 重复 owner。
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .owned
        .insert(sibling_id.clone());
    fixture
        .coordinator
        .registry
        .lookup_mut(&child)
        .unwrap()
        .refs
        .insert(duplicated_position.clone(), RefTarget::Data(sibling_id));
    assert!(matches!(
        fixture
            .coordinator
            .resolve::<Other>(&child, &duplicated_position),
        Err(ScopeError::DuplicateOwner { .. })
    ));

    // 失效 target 仍按既有顺序先报 TargetNotAlive。
    let dead = fixture
        .coordinator
        .register_owned(&child, &dead_position, Other(5))
        .unwrap();
    fixture.coordinator.container.destroy(&dead).unwrap();
    assert!(matches!(
        fixture.coordinator.resolve::<Other>(&child, &dead_position),
        Err(ScopeError::TargetNotAlive { .. })
    ));
}

#[test]
fn r15_output_position_and_count_contract_is_checked_against_declaration() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let first = fixture.ref_id();
    let second = fixture.ref_id();
    let first_caller = fixture.ref_id();
    let second_caller = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&child, &first, fixture.tracked(1))
        .unwrap();
    fixture
        .coordinator
        .register_owned(&child, &second, fixture.tracked(2))
        .unwrap();
    let declared = vec![first.clone(), second.clone()];

    // 声明两项却只提交一项（合法缩短列表）：数量契约在任何绑定与转移前拒绝。
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            &declared,
            &[ExportSlot::new::<Tracked>(&first, &first_caller)],
        ),
        Err(ScopeError::OutputCountMismatch {
            declared: 2,
            supplied: 1,
            ..
        })
    ));

    // 空提交同样不能通过非空声明。
    assert!(matches!(
        fixture
            .coordinator
            .prepare_export(&child, &root, &declared, &[]),
        Err(ScopeError::OutputCountMismatch { supplied: 0, .. })
    ));

    // 数量相同但位置顺序与声明不符。
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            &declared,
            &[
                ExportSlot::new::<Tracked>(&second, &first_caller),
                ExportSlot::new::<Tracked>(&first, &second_caller),
            ],
        ),
        Err(ScopeError::OutputPositionMismatch { .. })
    ));

    // 声明本身重复位置也拒绝。
    assert!(matches!(
        fixture.coordinator.prepare_export(
            &child,
            &root,
            &[first.clone(), first.clone()],
            &[ExportSlot::new::<Tracked>(&first, &first_caller)],
        ),
        Err(ScopeError::DuplicatePosition { .. })
    ));

    // 以上均未产生任何 caller 绑定、责任转移或 Drop。
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .is_empty()
    );
    assert_eq!(fixture.drops(), 0);
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Active
    );

    // 正式入口同样拒绝缩短声明，并走失败退出清理。
    let mut shortened = vec![ExportSlot::new::<Tracked>(&first, &first_caller)];
    assert!(matches!(
        fixture
            .coordinator
            .finalize(&child, &declared, &mut shortened),
        Err(ScopeError::OutputCountMismatch { .. })
    ));
    assert_eq!(fixture.drops(), 2);
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .is_empty()
    );
}

#[test]
fn r16_invocation_slots_are_consumed_and_cannot_be_reused() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let caller_position = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&child, &output, fixture.tracked(1))
        .unwrap();

    let declared = vec![output.clone()];
    let mut slots = vec![ExportSlot::new::<Tracked>(&output, &caller_position)];

    // 第一次提交：本次 Invocation 的 slot 被消费为空，声明列表未被消耗。
    fixture
        .coordinator
        .finalize(&child, &declared, &mut slots)
        .unwrap();
    assert!(slots.is_empty(), "invocation slots must be consumed");
    assert_eq!(declared.len(), 1, "declaration metadata stays reusable");
    assert!(
        fixture
            .coordinator
            .registry
            .lookup(&root)
            .unwrap()
            .refs
            .contains_key(&caller_position)
    );

    // 复用同一份已消费列表提交另一个 Invocation：不再被接受。
    let second = fixture.coordinator.create_child(&root).unwrap();
    let second_output = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&second, &second_output, fixture.tracked(2))
        .unwrap();
    let mut recycled = slots;
    assert!(matches!(
        fixture
            .coordinator
            .finalize(&second, &[second_output], &mut recycled),
        Err(ScopeError::OutputCountMismatch {
            declared: 1,
            supplied: 0,
            ..
        })
    ));
    // 崩溃的第二次提交走失败退出：只清理它自己的数据，第一次的输出不受影响。
    assert_eq!(
        fixture.coordinator.state(&second).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(fixture.drops(), 1);
    assert_eq!(
        fixture
            .coordinator
            .resolve::<Tracked>(&root, &caller_position)
            .unwrap()
            .num,
        1
    );
}

#[test]
fn r18_cleanup_diagnostic_is_not_hidden_by_the_export_error() {
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let temporary = fixture.ref_id();
    let caller_position = fixture.ref_id();

    fixture
        .coordinator
        .register_owned(&child, &output, Other(42))
        .unwrap();
    let stale = fixture
        .coordinator
        .register_owned(&child, &temporary, fixture.tracked(9))
        .unwrap();
    // 故障构造：临时 entry 失效，但 owned 责任记录仍在（该值已随 destroy 析构）。
    fixture.coordinator.container.destroy(&stale).unwrap();
    let drops_after_fault = fixture.drops();

    // 组合故障：Export 类型错误 + 清理前提破坏。
    // 必须报告清理诊断（Invariant），不能只让调用方看到 TypeMismatch。
    let mut slots = vec![ExportSlot::new::<Tracked>(&output, &caller_position)];
    let error = fixture
        .coordinator
        .finalize(&child, std::slice::from_ref(&output), &mut slots)
        .unwrap_err();
    assert!(
        matches!(error, ScopeError::Invariant { .. }),
        "cleanup failure was hidden: {error:?}"
    );
    // 未冒充正常 Closed，也没有越权销毁（失败清理未再析构任何值）。
    assert_ne!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    assert_eq!(fixture.drops(), drops_after_fault);

    // 对照：普通 Export 类型错误且清理前提完好时，仍返回原诊断并完成清理。
    let mut fixture = Fixture::new();
    let root = fixture.root();
    let child = fixture.coordinator.create_child(&root).unwrap();
    let output = fixture.ref_id();
    let caller_position = fixture.ref_id();
    fixture
        .coordinator
        .register_owned(&child, &output, fixture.tracked(42))
        .unwrap();
    let mut slots = vec![ExportSlot::new::<Other>(&output, &caller_position)];
    let error = fixture
        .coordinator
        .finalize(&child, std::slice::from_ref(&output), &mut slots)
        .unwrap_err();
    assert!(matches!(error, ScopeError::TypeMismatch { .. }));
    assert_eq!(
        fixture.coordinator.state(&child).unwrap(),
        ScopeState::Closed
    );
    // 失败清理完成：child 自有的 Tracked 值析构一次。
    assert_eq!(fixture.drops(), 1);
}

// ---- V21-03：控制状态登记、导入与 Promote（C01～C11） ----

/// C01：控制状态登记、句柄约束与归属／存在诊断。
#[test]
fn c01_control_state_registration_and_handles() {
    let mut f = Fixture::new();
    let root = f.root();
    let local = f.ref_id();
    f.coordinator
        .register_owned(&root, &local, f.tracked(1))
        .unwrap();

    // 从合法本地引用初始化：不分配 Definition RefId，也不重绑任何本地位置。
    let before = f.ids.allocate().unwrap().seq();
    let state = f.state::<Tracked>(&root, &local);
    let after = f.ids.allocate().unwrap().seq();
    assert_eq!(
        after - before,
        1,
        "register_state must not allocate or rebind Definition RefIds"
    );
    assert_eq!(state.owner(), &root);
    assert_eq!(state.seq(), 0);
    assert_eq!(f.refs_len(&root), 1);
    assert_eq!(f.uninit_state::<Tracked>(&root).seq(), 1);

    // 未初始化状态不能作为最终输出绑定。
    let out = f.ref_id();
    let uninit = f.uninit_state::<Tracked>(&root);
    assert!(matches!(
        f.coordinator.bind_state_output(&root, &uninit, &out),
        Err(ScopeError::StateUninitialized { .. })
    ));

    // 错误 owner：状态属于 root，被要求用于另一个控制器。
    let other = f.child(&root);
    let other_pos = f.ref_id();
    assert!(matches!(
        f.coordinator.bind_state_output(&other, &state, &other_pos),
        Err(ScopeError::StateWrongOwner { .. })
    ));

    // 来源不是本地引用时，登记直接拒绝。
    assert!(matches!(
        f.coordinator.register_state::<Tracked>(&root, &f.ref_id()),
        Err(ScopeError::RefNotBound { .. })
    ));

    // foreign 句柄：另一次 Execution 的状态不能被本协调组件解析。
    let mut foreign = Fixture::new();
    let foreign_root = foreign.root();
    let foreign_local = foreign.ref_id();
    foreign
        .coordinator
        .register_owned(&foreign_root, &foreign_local, foreign.tracked(7))
        .unwrap();
    let foreign_state = foreign.state::<Tracked>(&foreign_root, &foreign_local);
    assert!(matches!(
        f.coordinator.bind_state_output(&root, &foreign_state, &out),
        Err(ScopeError::StateForeignExecution { .. })
    ));
    assert!(matches!(
        f.coordinator.import_batch_with_states(
            &other,
            &root,
            &[],
            &[StateImportSlot::new::<Tracked>(&foreign_state, &other_pos)],
        ),
        Err(ScopeError::StateForeignExecution { .. })
    ));

    // 同 Execution 内从未登记的序号：StateNotRegistered（不授予任何 target）。
    let phantom = ControlStateId {
        owner: root.clone(),
        seq: 42,
    };
    assert!(matches!(
        f.coordinator.bind_state_output(&root, &phantom, &out),
        Err(ScopeError::StateNotRegistered { .. })
    ));

    // 控制器关闭后：旧句柄只得到 Closed，不复活。
    f.coordinator.abort(&other).unwrap();
    let declared: Vec<RefId> = Vec::new();
    let mut slots: Vec<ExportSlot> = Vec::new();
    f.coordinator
        .finalize(&root, &declared, &mut slots)
        .unwrap();
    assert!(matches!(
        f.coordinator.bind_state_output(&root, &state, &out),
        Err(ScopeError::ScopeClosed { .. })
    ));
    assert!(matches!(
        f.coordinator.promote(&other, &other_pos, &state),
        Err(ScopeError::ScopeClosed { .. })
    ));
    assert_eq!(f.drops(), 1, "root exit destroyed its own value once");
}

/// C02：状态来源与本地来源混合的整组 Import，失败不留部分绑定。
#[test]
fn c02_state_import_is_group_atomic_and_mixed() {
    let mut f = Fixture::new();
    let root = f.root();
    let d1_pos = f.ref_id();
    let d2_pos = f.ref_id();
    let d1 = f
        .coordinator
        .register_owned(&root, &d1_pos, f.tracked(1))
        .unwrap();
    f.coordinator
        .register_owned(&root, &d2_pos, f.tracked(2))
        .unwrap();

    let controller = f.child(&root);
    let c1 = f.ref_id();
    let c2 = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[
                ImportSlot::new::<Tracked>(&d1_pos, &c1),
                ImportSlot::new::<Tracked>(&d2_pos, &c2),
            ],
        )
        .unwrap();
    let state = f.state::<Tracked>(&controller, &c1);

    // 混合批次：本地引用与状态来源同组导入并全部成功。
    let round = f.child(&controller);
    let local_target = f.ref_id();
    let state_target = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &round,
            &controller,
            &[ImportSlot::new::<Tracked>(&c2, &local_target)],
            &[StateImportSlot::new::<Tracked>(&state, &state_target)],
        )
        .unwrap_or_else(|error| panic!("mixed import rejected: {error}"));
    assert_eq!(f.refs_len(&round), 2);
    assert_eq!(
        f.owner(&d1).unwrap(),
        root,
        "state import never changes owner"
    );
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&round, &state_target)
            .unwrap()
            .num,
        1
    );
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&round, &local_target)
            .unwrap()
            .num,
        2
    );

    // 后项失败（声明类型与状态登记类型不符）时，前项也不得留下部分绑定。
    let round2 = f.child(&controller);
    let ok_target = f.ref_id();
    let bad_target = f.ref_id();
    assert!(matches!(
        f.coordinator.import_batch_with_states(
            &round2,
            &controller,
            &[ImportSlot::new::<Tracked>(&c2, &ok_target)],
            &[StateImportSlot::new::<Other>(&state, &bad_target)],
        ),
        Err(ScopeError::StateTypeMismatch { .. })
    ));
    assert_eq!(f.refs_len(&round2), 0, "no partial import bindings");
    assert_eq!(f.owner(&d1).unwrap(), root);
    assert_eq!(f.owned_len(&round2), 0);

    // 未初始化的状态不能作为导入来源。
    let uninit = f.uninit_state::<Tracked>(&controller);
    let target = f.ref_id();
    assert!(matches!(
        f.coordinator.import_batch_with_states(
            &round2,
            &controller,
            &[],
            &[StateImportSlot::new::<Tracked>(&uninit, &target)]
        ),
        Err(ScopeError::StateUninitialized { .. })
    ));
    assert_eq!(f.refs_len(&round2), 0);

    // 状态句柄的控制器与 caller 不一致：不得导入。
    let other_controller = f.child(&root);
    let round3 = f.child(&other_controller);
    let target3 = f.ref_id();
    assert!(matches!(
        f.coordinator.import_batch_with_states(
            &round3,
            &other_controller,
            &[],
            &[StateImportSlot::new::<Tracked>(&state, &target3)]
        ),
        Err(ScopeError::StateWrongOwner { .. })
    ));
    assert_eq!(f.refs_len(&round3), 0);
}

/// C03：两轮新 owned 值的 Promote：责任转移、无父 Ref 绑定、逐轮导入。
#[test]
fn c03_two_rounds_of_new_owned_promote() {
    let mut f = Fixture::new();
    let root = f.root();
    let seed_pos = f.ref_id();
    let d1 = f
        .coordinator
        .register_owned(&root, &seed_pos, f.tracked(1))
        .unwrap();

    // 控制器显式导入 Root 输入，再从自己的本地引用初始化控制状态。
    let controller = f.child(&root);
    let ctrl_seed = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
        )
        .unwrap();
    let state = f.state::<Tracked>(&controller, &ctrl_seed);
    let refs_before = f.refs_len(&controller);

    let round1 = f.child(&controller);
    let r1_in = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &round1,
            &controller,
            &[],
            &[StateImportSlot::new::<Tracked>(&state, &r1_in)],
        )
        .unwrap();
    let r1_out = f.ref_id();
    let d2 = f
        .coordinator
        .register_owned(&round1, &r1_out, f.tracked(2))
        .unwrap();
    f.coordinator.promote(&round1, &r1_out, &state).unwrap();

    assert_eq!(f.scope_state(&round1), ScopeState::Closed);
    assert_eq!(
        f.owner(&d2).unwrap(),
        controller,
        "source-owned value moved to the controller"
    );
    assert_eq!(f.owner(&d1).unwrap(), root, "seed keeps its owner");
    assert_eq!(
        f.refs_len(&controller),
        refs_before,
        "promote binds no parent ref"
    );
    assert_eq!(f.drops(), 0);

    let round2 = f.child(&controller);
    let r2_in = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &round2,
            &controller,
            &[],
            &[StateImportSlot::new::<Tracked>(&state, &r2_in)],
        )
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&round2, &r2_in)
            .unwrap()
            .num,
        2,
        "round2 sees the promoted state"
    );
    let r2_out = f.ref_id();
    let d3 = f
        .coordinator
        .register_owned(&round2, &r2_out, f.tracked(3))
        .unwrap();
    f.coordinator.promote(&round2, &r2_out, &state).unwrap();
    assert_eq!(f.owner(&d3).unwrap(), controller);
    assert_eq!(f.drops(), 0, "replaced state waits for controlled recycle");
    assert!(f.alive(&d2));

    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(
        f.drops(),
        1,
        "controller-owned replaced state drops exactly once"
    );
    assert!(!f.alive(&d2));
    assert!(f.alive(&d1), "imported seed keeps its owner");
    assert!(f.alive(&d3));
}

/// C04：来源原样输出 imported 目标时只更新状态，不转移 owner。
#[test]
fn c04_imported_selected_result_only_updates_state() {
    let mut f = Fixture::new();
    let root = f.root();
    let seed_pos = f.ref_id();
    let d1 = f
        .coordinator
        .register_owned(&root, &seed_pos, f.tracked(5))
        .unwrap();
    let controller = f.child(&root);
    let ctrl_seed = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
        )
        .unwrap();
    let state = f.state::<Tracked>(&controller, &ctrl_seed);

    let round = f.child(&controller);
    let in_pos = f.ref_id();
    let out_pos = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &round,
            &controller,
            &[],
            &[
                StateImportSlot::new::<Tracked>(&state, &in_pos),
                StateImportSlot::new::<Tracked>(&state, &out_pos),
            ],
        )
        .unwrap();
    f.coordinator.promote(&round, &out_pos, &state).unwrap();

    assert_eq!(
        f.owner(&d1).unwrap(),
        root,
        "imported target keeps its owner"
    );
    assert_eq!(f.drops(), 0);
    assert!(f.readable_as::<Tracked>(&d1));
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&root, &seed_pos)
            .unwrap()
            .num,
        5
    );
    assert_eq!(f.scope_state(&round), ScopeState::Closed);
}

/// C07：旧状态回收受存活引用与其它控制状态约束。
#[test]
fn c07_recycle_respects_live_references_and_other_states() {
    let mut f = Fixture::new();
    let root = f.root();

    // (a) 尚存活 descendant 的有效 alias 阻止回收。
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);
    let round1 = f.child(&controller);
    let out1 = f.ref_id();
    f.coordinator
        .register_owned(&round1, &out1, f.tracked(1))
        .unwrap();
    f.coordinator.promote(&round1, &out1, &state).unwrap();

    let keeper = f.child(&controller);
    let keeper_pos = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &keeper,
            &controller,
            &[],
            &[StateImportSlot::new::<Tracked>(&state, &keeper_pos)],
        )
        .unwrap();

    let round2 = f.child(&controller);
    let out2 = f.ref_id();
    let d2 = f
        .coordinator
        .register_owned(&round2, &out2, f.tracked(2))
        .unwrap();
    f.coordinator.promote(&round2, &out2, &state).unwrap();

    // 旧值此时既被 keeper 引用，也被替换前登记为 pending。
    assert_eq!(f.owner(&d2).unwrap(), controller);
    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(f.drops(), 0, "live descendant alias blocks recycling");
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&keeper, &keeper_pos)
            .unwrap()
            .num,
        1
    );

    f.coordinator.abort(&keeper).unwrap();
    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(
        f.drops(),
        1,
        "old state drops exactly once after the alias disappears"
    );
    assert!(f.alive(&d2));

    // (b) 另一个控制状态保留旧值时同样不回收。
    let holder_a = f.uninit_state::<Tracked>(&controller);
    let holder_b = f.uninit_state::<Tracked>(&controller);
    let round3 = f.child(&controller);
    let out3 = f.ref_id();
    let d3 = f
        .coordinator
        .register_owned(&round3, &out3, f.tracked(3))
        .unwrap();
    f.coordinator.promote(&round3, &out3, &holder_a).unwrap();

    let round4 = f.child(&controller);
    let in4 = f.ref_id();
    let out4 = f.ref_id();
    f.coordinator
        .import_batch_with_states(
            &round4,
            &controller,
            &[],
            &[
                StateImportSlot::new::<Tracked>(&holder_a, &in4),
                StateImportSlot::new::<Tracked>(&holder_a, &out4),
            ],
        )
        .unwrap();
    f.coordinator.promote(&round4, &out4, &holder_b).unwrap();

    let round5 = f.child(&controller);
    let out5 = f.ref_id();
    f.coordinator
        .register_owned(&round5, &out5, f.tracked(5))
        .unwrap();
    f.coordinator.promote(&round5, &out5, &holder_a).unwrap();

    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(
        f.drops(),
        1,
        "another control state still holds the old value"
    );
    assert!(f.alive(&d3));

    let round6 = f.child(&controller);
    let out6 = f.ref_id();
    f.coordinator
        .register_owned(&round6, &out6, f.tracked(6))
        .unwrap();
    f.coordinator.promote(&round6, &out6, &holder_b).unwrap();
    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(f.drops(), 2, "recycled once all control states moved on");
    assert!(!f.alive(&d3));
}

/// C08：仅最终输出位置绑定一次；多轮 Promote 不绑定父 Ref。
#[test]
fn c08_only_final_output_ref_binding() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);
    let refs_before = f.refs_len(&controller);

    let mut last: Option<DataId> = None;
    for num in [1u32, 2] {
        let round = f.child(&controller);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&round, &out, f.tracked(num))
            .unwrap();
        f.coordinator.promote(&round, &out, &state).unwrap();
        last = Some(id);
    }
    assert_eq!(
        f.refs_len(&controller),
        refs_before,
        "no parent Ref binding during rounds"
    );

    let final_pos = f.ref_id();
    f.coordinator
        .bind_state_output(&controller, &state, &final_pos)
        .unwrap();
    assert_eq!(f.refs_len(&controller), refs_before + 1);
    // 同一位置不能再次绑定（不存在解绑／重绑接口）。
    assert!(matches!(
        f.coordinator
            .bind_state_output(&controller, &state, &final_pos),
        Err(ScopeError::RefAlreadyBound { .. })
    ));

    let root_out = f.ref_id();
    f.finalize_one::<Tracked>(&controller, &final_pos, &root_out)
        .unwrap();
    assert_eq!(f.scope_state(&controller), ScopeState::Closed);
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&root, &root_out)
            .unwrap()
            .num,
        2
    );
    let exported = last.expect("two rounds promoted a value");
    assert_eq!(
        f.owner(&exported).unwrap(),
        root,
        "unique owner after export"
    );
    assert_eq!(f.owned_len(&controller), 0);
    // 旧 state 句柄随控制器关闭失效。
    assert!(matches!(
        f.coordinator
            .bind_state_output(&controller, &state, &f.ref_id()),
        Err(ScopeError::ScopeClosed { .. })
    ));
}

/// C09：Promote 预检拒绝：状态、责任与两侧内容都不变。
#[test]
fn c09_promote_precheck_rejections() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let seed_pos = f.ref_id();
    f.coordinator
        .register_owned(&controller, &seed_pos, f.tracked(1))
        .unwrap();
    let state = f.state::<Tracked>(&controller, &seed_pos);

    // (i) 类型不符
    let round = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&round, &out, Other(7))
        .unwrap();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::TypeMismatch { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);
    assert_eq!(f.drops(), 0);

    // (ii) 未绑定的输出位置
    let round = f.child(&controller);
    let out = f.ref_id();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::RefNotBound { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);

    // (iii) 已失效 target：控制器另一个值导入 Round 后 entry 被销毁。
    let dead_pos = f.ref_id();
    let dead_src = f
        .coordinator
        .register_owned(&controller, &dead_pos, f.tracked(3))
        .unwrap();
    let round = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .import_batch(
            &round,
            &controller,
            &[ImportSlot::new::<Tracked>(&dead_pos, &out)],
        )
        .unwrap();
    f.coordinator.container.destroy(&dead_src).unwrap();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::TargetNotAlive { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);

    // (iv) sibling-owned：注入一个由兄弟 Scope 负责的 target。
    let sibling = f.child(&controller);
    let sib_pos = f.ref_id();
    let sib_id = f
        .coordinator
        .register_owned(&sibling, &sib_pos, f.tracked(4))
        .unwrap();
    let round = f.child(&controller);
    let out = f.ref_id();
    f.inject_target(&round, &out, RefTarget::Data(sib_id.clone()));
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);
    assert!(
        f.alive(&sib_id),
        "rejected promote must not destroy a sibling value"
    );

    // (v) 错误状态句柄：状态属于另一个控制器。
    let other = f.child(&root);
    let other_state = f.uninit_state::<Tracked>(&other);
    let round = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&round, &out, f.tracked(5))
        .unwrap();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &other_state),
        Err(ScopeError::StateWrongOwner { .. })
    ));

    // (vi) 非直接父控制器：句柄属于 root，来源是孙 Scope。
    let root_state = f.uninit_state::<Tracked>(&root);
    let round = f.child(&controller);
    let out = f.ref_id();
    let extra = f
        .coordinator
        .register_owned(&round, &out, f.tracked(6))
        .unwrap();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &root_state),
        Err(ScopeError::StateWrongOwner { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);
    assert!(
        !f.alive(&extra),
        "failure exit cleans the source's own values"
    );
    assert!(matches!(
        f.coordinator
            .bind_state_output(&root, &root_state, &f.ref_id()),
        Err(ScopeError::StateUninitialized { .. })
    ));

    // 所有拒绝都没有改变状态与责任：仍可绑定并读到 seed 值。
    let probe = f.ref_id();
    f.coordinator
        .bind_state_output(&controller, &state, &probe)
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&controller, &probe)
            .unwrap()
            .num,
        1
    );
    assert!(f.alive(&sib_id));
}

/// C10：正式失败退出保留 ancestor，并优先传播清理诊断。
#[test]
fn c10_promote_failure_exit_preserves_ancestor_and_cleanup_diagnostic() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let seed_pos = f.ref_id();
    f.coordinator
        .register_owned(&controller, &seed_pos, f.tracked(1))
        .unwrap();
    let state = f.state::<Tracked>(&controller, &seed_pos);

    let anc_pos = f.ref_id();
    let ancestor = f
        .coordinator
        .register_owned(&root, &anc_pos, f.tracked(50))
        .unwrap();
    let ctrl_anc = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
        )
        .unwrap();

    // 普通预检失败：Round Closed、自有临时值清理，ancestor 与状态保留。
    let round = f.child(&controller);
    let r_in = f.ref_id();
    f.coordinator
        .import_batch(
            &round,
            &controller,
            &[ImportSlot::new::<Tracked>(&ctrl_anc, &r_in)],
        )
        .unwrap();
    let temp = f.ref_id();
    let out = f.ref_id();
    f.coordinator
        .register_owned(&round, &temp, f.tracked(60))
        .unwrap();
    f.coordinator
        .register_owned(&round, &out, Other(61))
        .unwrap();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::TypeMismatch { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Closed);
    assert_eq!(f.drops(), 1, "round temporary cleaned exactly once");
    assert!(f.alive(&ancestor));
    assert_eq!(f.owner(&ancestor).unwrap(), root);
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&root, &anc_pos)
            .unwrap()
            .num,
        50
    );
    let probe = f.ref_id();
    f.coordinator
        .bind_state_output(&controller, &state, &probe)
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&controller, &probe)
            .unwrap()
            .num,
        1
    );

    // 清理前提破坏：预检无法完成 → 传播清理诊断，不误删、不冒充 Closed。
    let round = f.child(&controller);
    let out = f.ref_id();
    let owned_id = f
        .coordinator
        .register_owned(&round, &out, f.tracked(70))
        .unwrap();
    let broken = f.ref_id();
    let broken_id = f
        .coordinator
        .register_owned(&round, &broken, f.tracked(71))
        .unwrap();
    f.coordinator.container.destroy(&broken_id).unwrap();
    let drops_before = f.drops();
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::Invariant { .. })
    ));
    assert_ne!(f.scope_state(&round), ScopeState::Closed);
    assert_eq!(
        f.drops(),
        drops_before,
        "broken cleanup grants no destruction right"
    );
    assert!(f.alive(&owned_id));
    assert!(f.alive(&ancestor));
}

/// C11：Promote／Consume 的来源存活边界。
#[test]
fn c11_source_liveness_boundary() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);
    let collector = f.collector::<Tracked>(&controller);

    // Closed 来源：拒绝。
    let closed = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&closed, &out, f.tracked(1))
        .unwrap();
    f.coordinator.abort(&closed).unwrap();
    assert!(matches!(
        f.coordinator.promote(&closed, &out, &state),
        Err(ScopeError::ScopeClosed { .. })
    ));
    assert!(matches!(
        f.coordinator.consume_item(&closed, &out, &collector),
        Err(ScopeError::ScopeClosed { .. })
    ));

    // 带活跃 descendant：在冻结／mutation 前拒绝。
    let round = f.child(&controller);
    let out = f.ref_id();
    let id = f
        .coordinator
        .register_owned(&round, &out, f.tracked(2))
        .unwrap();
    let inner = f.child(&round);
    assert!(matches!(
        f.coordinator.promote(&round, &out, &state),
        Err(ScopeError::ActiveDescendants { .. })
    ));
    assert_eq!(
        f.scope_state(&round),
        ScopeState::Active,
        "not frozen before rejection"
    );
    assert_eq!(f.scope_state(&inner), ScopeState::Active);
    assert_eq!(f.owner(&id).unwrap(), round);
    assert!(matches!(
        f.coordinator.consume_item(&round, &out, &collector),
        Err(ScopeError::ActiveDescendants { .. })
    ));
    assert_eq!(f.scope_state(&round), ScopeState::Active);
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert!(matches!(
        f.coordinator
            .finish_collector(&controller, &collector, &f.ref_id()),
        Err(ScopeError::ActiveDescendants { .. })
    ));
    assert_eq!(f.owned_len(&controller), 0);
}

/// C12：collector 值只在 Container 建构区；未完成 collector 没有普通 DataId。
#[test]
fn c12_collector_physical_storage_is_container_only() {
    fn scope_shape(record: &Scope) {
        let _: &ScopeId = &record.id;
        let _: &Option<ScopeId> = &record.parent;
        let _: &Vec<ScopeId> = &record.children;
        let _: &ScopeState = &record.state;
        let _: &HashMap<RefId, RefTarget> = &record.refs;
        let _: &HashSet<DataId> = &record.owned;
        let _: &u64 = &record.state_next;
    }
    fn registry_shape(registry: &ScopeRegistry) {
        let _: &HashMap<u64, Scope> = &registry.scopes;
        let _: &HashMap<(u64, u64), ControlState> = &registry.states;
        let _: &HashMap<u64, CollectorRecord> = &registry.collectors;
    }
    fn collector_record_shape(record: &CollectorRecord) {
        let _: &CollectorId = &record.id;
        let _: &ScopeId = &record.owner;
        let _: &TypeId = &record.element_type;
        let _: &&'static str = &record.element_name;
    }

    let mut f = Fixture::new();
    let controller = f.child(&f.root());
    let item = f.child(&controller);
    let out = f.ref_id();
    let item_id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    let collector = f.collector::<Tracked>(&controller);
    assert_eq!(item_id.seq(), 0);

    f.coordinator.consume_item(&item, &out, &collector).unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert!(!f.alive(&item_id), "consumed item identity is invalid");

    let final_pos = f.ref_id();
    let finished = f
        .coordinator
        .finish_collector(&controller, &collector, &final_pos)
        .unwrap();
    assert_eq!(
        finished.seq(),
        1,
        "the collection DataId is allocated only at completion"
    );
    assert_eq!(
        f.coordinator
            .resolve::<Vec<Tracked>>(&controller, &final_pos)
            .unwrap()
            .len(),
        1
    );

    scope_shape(f.coordinator.registry.lookup(&controller).unwrap());
    registry_shape(&f.coordinator.registry);
    collector_record_shape(&CollectorRecord {
        id: collector,
        owner: controller.clone(),
        element_type: TypeId::of::<Tracked>(),
        element_name: std::any::type_name::<Tracked>(),
    });
}

/// C13：collector 身份、责任与完成／清理后的句柄。
#[test]
fn c13_collector_identity_and_responsibility() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);

    let tracked_c = f.collector::<Tracked>(&controller);
    let other_c = f.collector::<Other>(&controller);
    assert_ne!(tracked_c, other_c);
    let (name, owner) = f.coordinator.collector_metadata(&tracked_c).unwrap();
    assert_eq!(name, std::any::type_name::<Tracked>());
    assert_eq!(owner, controller);

    // foreign 句柄。
    let mut foreign = Fixture::new();
    let foreign_root = foreign.root();
    let foreign_c = foreign.collector::<Tracked>(&foreign_root);
    assert!(matches!(
        f.coordinator.collector_len(&foreign_c),
        Err(ScopeError::CollectorForeignExecution { .. })
    ));

    // 错误责任 Scope：消费端与完成端的控制器都不是 collector 的 owner。
    let elsewhere = f.child(&root);
    let pos = f.ref_id();
    f.coordinator
        .register_owned(&elsewhere, &pos, f.tracked(1))
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&elsewhere, &pos, &tracked_c),
        Err(ScopeError::CollectorNotOwnedBy { .. })
    ));
    assert_eq!(
        f.scope_state(&elsewhere),
        ScopeState::Closed,
        "failed consume closes its own source only"
    );
    let solo = f.child(&root);
    let wrong_owner = f
        .coordinator
        .finish_collector(&solo, &tracked_c, &f.ref_id())
        .unwrap_err();
    assert!(
        matches!(wrong_owner, ScopeError::CollectorNotOwnedBy { .. }),
        "unexpected diagnostic: {wrong_owner:?}"
    );
    assert_eq!(f.scope_state(&solo), ScopeState::Active);

    // 显式空 collector 完成为新的空 Vec 普通 Data。
    let empty_pos = f.ref_id();
    f.coordinator
        .finish_collector(&controller, &other_c, &empty_pos)
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Vec<Other>>(&controller, &empty_pos)
            .unwrap()
            .len(),
        0
    );

    // 已完成句柄：不可再追加或重复完成。
    assert!(matches!(
        f.coordinator.collector_len(&other_c),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
    assert!(matches!(
        f.coordinator
            .finish_collector(&controller, &other_c, &f.ref_id()),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
    let item = f.child(&controller);
    let item_out = f.ref_id();
    f.coordinator
        .register_owned(&item, &item_out, f.tracked(2))
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item, &item_out, &other_c),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));

    // 清理后句柄：随控制器退出撤销。
    f.coordinator.abort(&controller).unwrap();
    assert!(matches!(
        f.coordinator.collector_len(&tracked_c),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
}

/// C14：ItemScope 自有输出直接 Consume，无逐项 EachScope 中转。
#[test]
fn c14_item_owned_direct_consume() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);
    let item = f.child(&each);
    let out = f.ref_id();
    let item_id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(20))
        .unwrap();

    f.coordinator.consume_item(&item, &out, &collector).unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 1);
    assert_eq!(f.drops(), 0, "moved element is not dropped");
    assert!(!f.alive(&item_id));
    assert!(!f.readable_as::<Tracked>(&item_id));
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
    assert_eq!(f.owned_len(&item), 0);
    assert_eq!(f.refs_len(&each), 0);
    assert_eq!(f.owned_len(&each), 0, "no per-item transfer into EachScope");
}

/// C15：imported／parent-owned／sibling-owned 输出在取值前拒绝。
#[test]
fn c15_imported_and_parent_owned_outputs_rejected_before_moving() {
    let mut f = Fixture::new();
    let root = f.root();
    let shared_pos = f.ref_id();
    let shared = f
        .coordinator
        .register_owned(&root, &shared_pos, f.tracked(1))
        .unwrap();
    let each = f.child(&root);
    let each_pos = f.ref_id();
    f.coordinator
        .import_batch(
            &each,
            &root,
            &[ImportSlot::new::<Tracked>(&shared_pos, &each_pos)],
        )
        .unwrap();
    let collector = f.collector::<Tracked>(&each);

    // (a) 原样重新暴露 shared ancestor 的完整 Data。
    let item = f.child(&each);
    let alias = f.ref_id();
    f.coordinator
        .import_batch(
            &item,
            &each,
            &[ImportSlot::new::<Tracked>(&each_pos, &alias)],
        )
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item, &alias, &collector),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.alive(&shared));
    assert_eq!(f.owner(&shared).unwrap(), root);

    // (b) parent(Each) 自己 owned 的 Data。
    let own_pos = f.ref_id();
    let own = f
        .coordinator
        .register_owned(&each, &own_pos, f.tracked(2))
        .unwrap();
    let item2 = f.child(&each);
    let alias2 = f.ref_id();
    f.coordinator
        .import_batch(
            &item2,
            &each,
            &[ImportSlot::new::<Tracked>(&own_pos, &alias2)],
        )
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item2, &alias2, &collector),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.alive(&own));

    // (c) sibling-owned：单有 target 存活不授予消费权。
    let item3 = f.child(&each);
    let temp = f.ref_id();
    let temp_id = f
        .coordinator
        .register_owned(&item3, &temp, f.tracked(3))
        .unwrap();
    let item4 = f.child(&each);
    let injected = f.ref_id();
    f.inject_target(&item4, &injected, RefTarget::Data(temp_id.clone()));
    assert!(matches!(
        f.coordinator.consume_item(&item4, &injected, &collector),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.alive(&temp_id));
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
}

/// C16：无 owner／重复 owner 的责任破坏诊断，且在移动前拒绝。
#[test]
fn c16_missing_and_duplicate_owner_rejections() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);

    // 无 owner：责任记录被撤销，target 仍存活。
    let item = f.child(&each);
    let out = f.ref_id();
    let id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    f.drop_ownership(&item, &id);
    assert!(matches!(
        f.coordinator.consume_item(&item, &out, &collector),
        Err(ScopeError::NoOwner { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.alive(&id), "broken ownership grants no destruction right");

    // 重复 owner：预检发现责任破坏；清理前提校验同样失败，因此优先传播清理诊断。
    let item = f.child(&each);
    let out = f.ref_id();
    let id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(2))
        .unwrap();
    f.add_ownership(&each, &id);
    let error = f
        .coordinator
        .consume_item(&item, &out, &collector)
        .unwrap_err();
    assert!(
        matches!(
            error,
            ScopeError::DuplicateOwner { .. } | ScopeError::Invariant { .. }
        ),
        "duplicate ownership must surface as a responsibility/cleanup diagnostic, got {error:?}"
    );
    assert_eq!(
        f.scope_state(&item),
        ScopeState::Finalizing,
        "broken cleanup must not disguise a damaged scope as Closed"
    );
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert!(f.alive(&id));
}

/// C17：Consume 的类型与 target 检查先于移动。
#[test]
fn c17_consume_type_and_target_checks() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);

    // (i) 元素类型不符：collector 元素为 Other，item 输出 Tracked。
    let collector = f.collector::<Other>(&each);
    let item = f.child(&each);
    let out = f.ref_id();
    let id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item, &out, &collector),
        Err(ScopeError::TypeMismatch { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    // 拒绝后走正式失败退出：来源自身清理，元素没有进入 collector。
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
    assert!(!f.alive(&id));

    // (ii) 失效 target：imported 值被销毁，来源自身责任集合仍完好。
    let collector = f.collector::<Tracked>(&each);
    let src_pos = f.ref_id();
    let src = f
        .coordinator
        .register_owned(&each, &src_pos, f.tracked(2))
        .unwrap();
    let item = f.child(&each);
    let alias = f.ref_id();
    f.coordinator
        .import_batch(
            &item,
            &each,
            &[ImportSlot::new::<Tracked>(&src_pos, &alias)],
        )
        .unwrap();
    f.coordinator.container.destroy(&src).unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item, &alias, &collector),
        Err(ScopeError::TargetNotAlive { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);

    // (iii) foreign DataId：归属先于类型与移动判定。
    let mut foreign = Fixture::new();
    let foreign_root = foreign.root();
    let foreign_pos = foreign.ref_id();
    let foreign_id = foreign
        .coordinator
        .register_owned(&foreign_root, &foreign_pos, foreign.tracked(3))
        .unwrap();
    let item = f.child(&each);
    let injected = f.ref_id();
    f.inject_target(&item, &injected, RefTarget::Data(foreign_id.clone()));
    let error = f
        .coordinator
        .consume_item(&item, &injected, &collector)
        .unwrap_err();
    assert!(
        matches!(error, ScopeError::Storage { .. }),
        "foreign DataId must be reported as a storage ownership failure, got {error:?}"
    );
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
}

/// C18：descendant 先正常 Export 到 ItemScope，再按同一 Consume 路径收集。
#[test]
fn c18_descendant_output_via_export_then_consume() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);

    let item = f.child(&each);
    let descendant = f.child(&item);
    let d_out = f.ref_id();
    let item_out = f.ref_id();
    f.coordinator
        .register_owned(&descendant, &d_out, f.tracked(30))
        .unwrap();
    f.finalize_one::<Tracked>(&descendant, &d_out, &item_out)
        .unwrap();
    assert_eq!(f.scope_state(&descendant), ScopeState::Closed);
    assert_eq!(
        f.owned_len(&item),
        1,
        "exported value is owned by ItemScope"
    );

    f.coordinator
        .consume_item(&item, &item_out, &collector)
        .unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
    assert_eq!(f.refs_len(&each), 0, "no per-item EachScope Ref binding");
    assert_eq!(f.drops(), 0);
}

/// C19：多个 Item 复用同一输出位置与同一 collector；顺序与另一个 collector 不受影响。
#[test]
fn c19_multiple_items_share_one_output_ref_and_two_collectors() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);
    let other_collector = f.collector::<Other>(&each);
    let own_pos = f.ref_id();
    let own_id = f
        .coordinator
        .register_owned(&each, &own_pos, Other(99))
        .unwrap();

    // 同一个 body 输出定义位置按 item 重复使用。
    let body_out = f.ref_id();
    for num in [1u32, 2] {
        let item = f.child(&each);
        f.coordinator
            .register_owned(&item, &body_out, f.tracked(num))
            .unwrap();
        f.coordinator
            .consume_item(&item, &body_out, &collector)
            .unwrap();
    }
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 2);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 2);
    assert_eq!(f.owned_len(&each), 1, "only the pre-existing own value");

    let final_pos = f.ref_id();
    let _vec_id = f
        .coordinator
        .finish_collector(&each, &collector, &final_pos)
        .unwrap();
    let collected = f
        .coordinator
        .resolve::<Vec<Tracked>>(&each, &final_pos)
        .unwrap();
    assert_eq!(
        collected.iter().map(|item| item.num).collect::<Vec<_>>(),
        vec![1, 2],
        "collection order follows item order"
    );
    assert_eq!(f.refs_len(&each), 2, "no per-item growth");
    assert!(
        f.alive(&own_id),
        "the other collector's owner value is untouched"
    );
    assert_eq!(f.coordinator.collector_len(&other_collector).unwrap(), 0);
    assert_eq!(f.drops(), 0);
}

/// C20：部分结果与失败 Item 的清理；无正常集合输出、无 detached Scope。
#[test]
fn c20_partial_results_failure_cleanup() {
    let mut f = Fixture::new();
    let root = f.root();
    let rules_pos = f.ref_id();
    let rules = f
        .coordinator
        .register_owned(&root, &rules_pos, f.tracked(100))
        .unwrap();
    let each = f.child(&root);
    let each_rules = f.ref_id();
    f.coordinator
        .import_batch(
            &each,
            &root,
            &[ImportSlot::new::<Tracked>(&rules_pos, &each_rules)],
        )
        .unwrap();
    let collector = f.collector::<Tracked>(&each);

    let item1 = f.child(&each);
    let out1 = f.ref_id();
    f.coordinator
        .register_owned(&item1, &out1, f.tracked(1))
        .unwrap();
    f.coordinator
        .consume_item(&item1, &out1, &collector)
        .unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);

    // Item2 重新暴露 imported Rules：预检拒绝；失败退出只清理自身。
    let item2 = f.child(&each);
    let temp = f.ref_id();
    f.coordinator
        .register_owned(&item2, &temp, f.tracked(2))
        .unwrap();
    let alias = f.ref_id();
    f.coordinator
        .import_batch(
            &item2,
            &each,
            &[ImportSlot::new::<Tracked>(&each_rules, &alias)],
        )
        .unwrap();
    assert!(matches!(
        f.coordinator.consume_item(&item2, &alias, &collector),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert_eq!(f.drops(), 1, "item2 temporary cleaned exactly once");
    assert!(f.alive(&rules));

    f.coordinator.abort(&each).unwrap();
    assert_eq!(f.drops(), 2, "partial collector result dropped once");
    assert!(f.alive(&rules), "root imported value survives");
    assert_eq!(f.owner(&rules).unwrap(), root);
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&root, &rules_pos)
            .unwrap()
            .num,
        100
    );
    assert_eq!(f.refs_len(&each), 0, "no normal Vec output was produced");
    for scope in [&each, &item1, &item2] {
        assert_eq!(
            f.scope_state(scope),
            ScopeState::Closed,
            "no scope of the aborted subtree is left detached"
        );
    }
}

/// C21：完成与最终 Export；空 collector；活跃 descendant 与位置冲突的拒绝。
#[test]
fn c21_finish_and_final_export() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);

    let body_out = f.ref_id();
    for num in [1u32, 2] {
        let item = f.child(&each);
        f.coordinator
            .register_owned(&item, &body_out, f.tracked(num))
            .unwrap();
        f.coordinator
            .consume_item(&item, &body_out, &collector)
            .unwrap();
    }

    // 活跃 descendant：完成前拒绝，且无半成品。
    let live_item = f.child(&each);
    assert!(matches!(
        f.coordinator
            .finish_collector(&each, &collector, &f.ref_id()),
        Err(ScopeError::ActiveDescendants { .. })
    ));
    assert_eq!(f.owned_len(&each), 0);
    assert_eq!(f.refs_len(&each), 0);
    f.coordinator.abort(&live_item).unwrap();

    let final_pos = f.ref_id();
    let vec_id = f
        .coordinator
        .finish_collector(&each, &collector, &final_pos)
        .unwrap();
    assert_eq!(f.owner(&vec_id).unwrap(), each);
    assert!(matches!(
        f.coordinator
            .finish_collector(&each, &collector, &f.ref_id()),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));

    // 输出位置冲突：完成前拒绝，collector 保持未完成。
    let empty_collector = f.collector::<Tracked>(&each);
    assert!(matches!(
        f.coordinator
            .finish_collector(&each, &empty_collector, &final_pos),
        Err(ScopeError::RefAlreadyBound { .. })
    ));
    assert_eq!(f.coordinator.collector_len(&empty_collector).unwrap(), 0);

    // 显式空 collector 形成新的空 Vec 普通 Data。
    let empty_pos = f.ref_id();
    let empty_id = f
        .coordinator
        .finish_collector(&each, &empty_collector, &empty_pos)
        .unwrap();
    assert_ne!(empty_id, vec_id);
    assert_eq!(
        f.coordinator
            .resolve::<Vec<Tracked>>(&each, &empty_pos)
            .unwrap()
            .len(),
        0
    );

    // 最终 Export；controller 退出不销毁 caller 接收值。
    let root_out = f.ref_id();
    f.finalize_one::<Vec<Tracked>>(&each, &final_pos, &root_out)
        .unwrap();
    assert_eq!(f.scope_state(&each), ScopeState::Closed);
    assert_eq!(f.owner(&vec_id).unwrap(), root);
    assert_eq!(
        f.coordinator
            .resolve::<Vec<Tracked>>(&root, &root_out)
            .unwrap()
            .iter()
            .map(|item| item.num)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(f.drops(), 0);
    assert!(
        !f.alive(&empty_id),
        "unexported empty Vec is cleaned by controller exit"
    );
}

/// C22：完成时 DataId 耗尽：collector 保持未完成，随后 abort 清理部分结果。
#[test]
fn c22_data_id_exhaustion_at_finish() {
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut c = ScopeCoordinator::new(ExecutionIdentity::with_starts(u64::MAX - 2, 0));
    let root = c.root();

    let rules_pos = ids.allocate().unwrap();
    let rules = c
        .register_owned(
            &root,
            &rules_pos,
            Tracked {
                num: 1,
                drops: Arc::clone(&drops),
            },
        )
        .unwrap();
    let each = c.create_child(&root).unwrap();
    let each_rules = ids.allocate().unwrap();
    c.import_batch(
        &each,
        &root,
        &[ImportSlot::new::<Tracked>(&rules_pos, &each_rules)],
    )
    .unwrap();
    let collector = c.begin_collector::<Tracked>(&each).unwrap();

    let item = c.create_child(&each).unwrap();
    let out = ids.allocate().unwrap();
    let item_id = c
        .register_owned(
            &item,
            &out,
            Tracked {
                num: 2,
                drops: Arc::clone(&drops),
            },
        )
        .unwrap();
    assert_eq!(item_id.seq(), u64::MAX - 1);
    c.consume_item(&item, &out, &collector).unwrap();
    assert_eq!(c.collector_len(&collector).unwrap(), 1);

    let refs_before = c.registry.lookup(&each).unwrap().refs.len();
    let owned_before = c.registry.lookup(&each).unwrap().owned.len();
    let final_pos = ids.allocate().unwrap();
    let error = c
        .finish_collector(&each, &collector, &final_pos)
        .unwrap_err();
    assert!(
        matches!(
            error,
            ScopeError::Storage {
                source: InternalError::IdSpaceExhausted { .. }
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        c.collector_len(&collector).unwrap(),
        1,
        "collector stays unfinished"
    );
    let record = c.registry.lookup(&each).unwrap();
    assert_eq!(
        record.refs.len(),
        refs_before,
        "no output ref is bound on exhaustion"
    );
    assert_eq!(
        record.owned.len(),
        owned_before,
        "no ordinary entry on exhaustion"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    c.abort(&each).unwrap();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "partial result cleaned once"
    );
    assert!(
        c.container.validate(&rules).is_ok(),
        "ancestor value survives"
    );
    assert_eq!(c.resolve::<Tracked>(&root, &rules_pos).unwrap().num, 1);
}

/// C23 正例：缩短 borrow 后 Promote／Consume 正常完成（负例见 tests/ui/c23_*.rs）。
#[test]
fn c23_shortening_the_borrow_allows_promote_and_consume() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);

    let round = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&round, &out, f.tracked(1))
        .unwrap();
    {
        let borrowed = f.coordinator.resolve::<Tracked>(&round, &out).unwrap();
        assert_eq!(borrowed.num, 1);
    }
    f.coordinator.promote(&round, &out, &state).unwrap();

    let each = f.child(&root);
    let collector = f.collector::<Tracked>(&each);
    let item = f.child(&each);
    let item_out = f.ref_id();
    f.coordinator
        .register_owned(&item, &item_out, f.tracked(2))
        .unwrap();
    {
        let borrowed = f.coordinator.resolve::<Tracked>(&item, &item_out).unwrap();
        assert_eq!(borrowed.num, 2);
    }
    f.coordinator
        .consume_item(&item, &item_out, &collector)
        .unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
}

/// C24：三类操作的提交前拒绝快照；清理校验不晚于 commit。
#[test]
fn c24_pre_commit_rejection_snapshots() {
    let mut f = Fixture::new();
    let root = f.root();
    let seed_pos = f.ref_id();
    f.coordinator
        .register_owned(&root, &seed_pos, f.tracked(9))
        .unwrap();
    let controller = f.child(&root);
    let ctrl_seed = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
        )
        .unwrap();
    let state = f.state::<Tracked>(&controller, &ctrl_seed);
    let collector = f.collector::<Tracked>(&controller);

    let seed_item = f.child(&controller);
    let seed_item_out = f.ref_id();
    f.coordinator
        .register_owned(&seed_item, &seed_item_out, f.tracked(2))
        .unwrap();
    f.coordinator
        .consume_item(&seed_item, &seed_item_out, &collector)
        .unwrap();

    let snapshot = |f: &Fixture| {
        (
            f.drops(),
            f.refs_len(&controller),
            f.owned_len(&controller),
            f.coordinator.collector_len(&collector).unwrap(),
            f.coordinator.collector_moves(&collector).unwrap(),
            f.scope_state(&controller),
        )
    };

    // Promote：类型不符的 Round 输出来源。
    let round = f.child(&controller);
    let round_out = f.ref_id();
    f.coordinator
        .register_owned(&round, &round_out, Other(3))
        .unwrap();
    let before = snapshot(&f);
    assert!(f.coordinator.promote(&round, &round_out, &state).is_err());
    assert_eq!(
        before,
        snapshot(&f),
        "promote rejection leaves no partial commit"
    );

    // Consume：元素类型不符的 item 输出。
    let item = f.child(&controller);
    let item_out = f.ref_id();
    f.coordinator
        .register_owned(&item, &item_out, Other(4))
        .unwrap();
    let before = snapshot(&f);
    assert!(
        f.coordinator
            .consume_item(&item, &item_out, &collector)
            .is_err()
    );
    assert_eq!(
        before,
        snapshot(&f),
        "consume rejection leaves no partial commit"
    );

    // 完成：输出位置已被占用。
    let before = snapshot(&f);
    assert!(matches!(
        f.coordinator
            .finish_collector(&controller, &collector, &ctrl_seed),
        Err(ScopeError::RefAlreadyBound { .. })
    ));
    assert_eq!(
        before,
        snapshot(&f),
        "finish rejection leaves no partial commit"
    );
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
}

/// C25：控制器退出／abort 清理未完成 collector、pending 与控制状态。
#[test]
fn c25_controller_exit_cleans_collector_pending_and_states() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);
    let collector = f.collector::<Tracked>(&controller);

    let item = f.child(&controller);
    let item_out = f.ref_id();
    f.coordinator
        .register_owned(&item, &item_out, f.tracked(1))
        .unwrap();
    f.coordinator
        .consume_item(&item, &item_out, &collector)
        .unwrap();

    let round = f.child(&controller);
    let out = f.ref_id();
    let first = f
        .coordinator
        .register_owned(&round, &out, f.tracked(2))
        .unwrap();
    f.coordinator.promote(&round, &out, &state).unwrap();
    let final_pos = f.ref_id();
    f.coordinator
        .bind_state_output(&controller, &state, &final_pos)
        .unwrap();

    let round2 = f.child(&controller);
    let out2 = f.ref_id();
    f.coordinator
        .register_owned(&round2, &out2, f.tracked(3))
        .unwrap();
    f.coordinator.promote(&round2, &out2, &state).unwrap();
    f.coordinator.recycle_pending(&controller).unwrap();
    assert_eq!(
        f.drops(),
        0,
        "controller-local alias keeps the old state pending"
    );

    f.coordinator.abort(&controller).unwrap();
    assert_eq!(
        f.drops(),
        3,
        "collector result + pending old state + current state"
    );
    assert!(!f.alive(&first));
    assert_eq!(f.scope_state(&controller), ScopeState::Closed);
    assert!(matches!(
        f.coordinator.collector_len(&collector),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
    assert!(matches!(
        f.coordinator
            .bind_state_output(&controller, &state, &f.ref_id()),
        Err(ScopeError::ScopeClosed { .. })
    ));

    // Container 析构不产生额外 Drop。
    let drops = Arc::clone(&f.drops);
    let coordinator = f.coordinator;
    drop(coordinator);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

/// C26：非 Clone 业务值走完 Promote／Consume／完成／Export，内部面仍是借用式。
#[test]
fn c26_non_clone_pipeline_and_internal_surface() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);
    let collector = f.collector::<Tracked>(&controller);

    for num in [1u32, 2] {
        let round = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&round, &out, f.tracked(num))
            .unwrap();
        f.coordinator.promote(&round, &out, &state).unwrap();

        let item = f.child(&controller);
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&item, &item_out, f.tracked(num))
            .unwrap();
        f.coordinator
            .consume_item(&item, &item_out, &collector)
            .unwrap();
    }
    let final_pos = f.ref_id();
    f.coordinator
        .finish_collector(&controller, &collector, &final_pos)
        .unwrap();
    let root_out = f.ref_id();
    f.finalize_one::<Vec<Tracked>>(&controller, &final_pos, &root_out)
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Vec<Tracked>>(&root, &root_out)
            .unwrap()
            .iter()
            .map(|item| item.num)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    // 内部读取面仍是"返回借用"，没有按值取数的公开入口。
    type BorrowFn<'a> =
        fn(&'a ScopeCoordinator, &ScopeId, &RefId) -> Result<&'a Tracked, ScopeError>;
    let _resolve: BorrowFn<'_> = ScopeCoordinator::resolve::<Tracked>;
}

/// C27：CollectorId 与控制状态序号耗尽；清理后不复用、旧句柄不复活。
#[test]
fn c27_collector_and_state_sequence_exhaustion() {
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Arc::new(AtomicUsize::new(0));

    // CollectorId 近上限：耗尽后不回绕。
    let mut c = ScopeCoordinator::new(ExecutionIdentity::with_starts_and_collector_start(
        0,
        0,
        u64::MAX - 1,
    ));
    let root = c.root();
    let controller = c.create_child(&root).unwrap();
    let first = c.begin_collector::<Tracked>(&controller).unwrap();
    assert_eq!(first.seq(), u64::MAX - 1);
    let error = c.begin_collector::<Tracked>(&controller).unwrap_err();
    assert!(
        matches!(
            error,
            ScopeError::Storage {
                source: InternalError::IdSpaceExhausted {
                    kind: IdKind::Collector
                }
            }
        ),
        "got {error:?}"
    );
    assert_eq!(c.collector_metadata(&first).unwrap().1, controller);

    // 控制状态近上限：耗尽后不回绕；关闭后旧句柄只得到 Closed。
    let mut c2 = ScopeCoordinator::new(ExecutionIdentity::new());
    let root2 = c2.root();
    let scope2 = c2.create_child(&root2).unwrap();
    let pos2 = ids.allocate().unwrap();
    c2.register_owned(
        &scope2,
        &pos2,
        Tracked {
            num: 1,
            drops: Arc::clone(&drops),
        },
    )
    .unwrap();
    c2.with_state_start(&scope2, u64::MAX - 1).unwrap();
    let last = c2.register_state::<Tracked>(&scope2, &pos2).unwrap();
    assert_eq!(last.seq(), u64::MAX - 1);
    let error = c2.register_state::<Tracked>(&scope2, &pos2).unwrap_err();
    assert!(
        matches!(
            error,
            ScopeError::Storage {
                source: InternalError::IdSpaceExhausted {
                    kind: IdKind::ControlState
                }
            }
        ),
        "got {error:?}"
    );
    c2.abort(&scope2).unwrap();
    assert!(matches!(
        c2.bind_state_output(&scope2, &last, &ids.allocate().unwrap()),
        Err(ScopeError::ScopeClosed { .. })
    ));

    // 清理后新身份不复用：同一 Execution 内序号继续前进。
    let mut c3 = ScopeCoordinator::new(ExecutionIdentity::new());
    let root3 = c3.root();
    let scope3 = c3.create_child(&root3).unwrap();
    let a = c3.begin_collector::<Tracked>(&scope3).unwrap();
    c3.abort(&scope3).unwrap();
    let scope4 = c3.create_child(&root3).unwrap();
    let b = c3.begin_collector::<Tracked>(&scope4).unwrap();
    assert_eq!(
        (a.seq(), b.seq()),
        (0, 1),
        "collector identities are never reused"
    );
    assert_ne!(a, b);
    assert!(matches!(
        c3.collector_len(&a),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
}

// ---- R7～R9：清理前提与登记／建构值一致性的补证样本 ----

/// R7：Promote 的清理前提包含来源未完成 collector；提交前拒绝。
#[test]
fn r7_promote_cleanup_preflight_covers_unfinished_collectors() {
    let mut f = Fixture::new();
    let root = f.root();
    let controller = f.child(&root);
    let state = f.uninit_state::<Tracked>(&controller);

    // ancestor：root 的值经控制器导入来源 Round，用于观测误删。
    let anc_pos = f.ref_id();
    let ancestor = f
        .coordinator
        .register_owned(&root, &anc_pos, f.tracked(50))
        .unwrap();
    let ctrl_anc = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
        )
        .unwrap();
    let round = f.child(&controller);
    let round_anc = f.ref_id();
    f.coordinator
        .import_batch(
            &round,
            &controller,
            &[ImportSlot::new::<Tracked>(&ctrl_anc, &round_anc)],
        )
        .unwrap();
    let out = f.ref_id();
    let value = f
        .coordinator
        .register_owned(&round, &out, f.tracked(1))
        .unwrap();
    let broken = f.collector::<Tracked>(&round);
    // test-only 故障：删除物理建构值，保留 registry 登记。
    f.coordinator.container.destroy_collector(&broken).unwrap();

    let before = (
        f.drops(),
        f.refs_len(&controller),
        f.owned_len(&controller),
        f.refs_len(&round),
        f.owned_len(&round),
    );
    let error = f.coordinator.promote(&round, &out, &state).unwrap_err();
    assert!(
        matches!(error, ScopeError::Invariant { .. }),
        "got {error:?}"
    );

    // 提交前拒绝：父状态仍未初始化、两侧 refs／owned 与 Drop 都不变、值仍归来源。
    assert!(matches!(
        f.coordinator
            .bind_state_output(&controller, &state, &f.ref_id()),
        Err(ScopeError::StateUninitialized { .. })
    ));
    assert_eq!(
        before,
        (
            f.drops(),
            f.refs_len(&controller),
            f.owned_len(&controller),
            f.refs_len(&round),
            f.owned_len(&round)
        ),
        "collector precondition failure leaves no partial commit"
    );
    assert_eq!(f.owner(&value).unwrap(), round);
    assert!(f.alive(&value));
    assert!(f.alive(&ancestor), "ancestor value is not destroyed");
    assert_eq!(f.owner(&ancestor).unwrap(), root);
    assert_eq!(
        f.coordinator
            .resolve::<Tracked>(&root, &anc_pos)
            .unwrap()
            .num,
        50
    );
    // 清理自身也失败：来源不冒充 Closed，且故障 collector 的登记不被提前撤销。
    assert_ne!(f.scope_state(&round), ScopeState::Closed);
    assert!(
        f.coordinator.collector_metadata(&broken).is_ok(),
        "registration is revoked only after the building value is actually destroyed"
    );
}

/// R8：Consume 在提交前发现 registry 与物理 builder 的元素类型不一致。
#[test]
fn r8_consume_rejects_registry_builder_type_mismatch_before_moving() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.collector::<Other>(&each);
    // test-only 故障：只改 registry 登记类型，物理建构值仍是 Other。
    {
        let record = f
            .coordinator
            .registry
            .collectors
            .get_mut(&collector.seq())
            .unwrap();
        record.element_type = TypeId::of::<Tracked>();
        record.element_name = std::any::type_name::<Tracked>();
    }

    let item = f.child(&each);
    let out = f.ref_id();
    let item_id = f
        .coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    let error = f
        .coordinator
        .consume_item(&item, &out, &collector)
        .unwrap_err();
    assert!(
        matches!(error, ScopeError::Invariant { .. }),
        "got {error:?}"
    );
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);

    // 预检失败仍走正式来源失败清理路径：来源自身清理完成 → Closed。
    assert!(!f.alive(&item_id));
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
}

/// R9／C25：正常退出清理未完成但有元素的 collector。
#[test]
fn c25_normal_exit_cleans_unfinished_collector_with_elements() {
    let mut f = Fixture::new();
    let root = f.root();
    let anc_pos = f.ref_id();
    let ancestor = f
        .coordinator
        .register_owned(&root, &anc_pos, f.tracked(50))
        .unwrap();
    let controller = f.child(&root);
    let ctrl_anc = f.ref_id();
    f.coordinator
        .import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
        )
        .unwrap();
    let collector = f.collector::<Tracked>(&controller);

    let item = f.child(&controller);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    f.coordinator.consume_item(&item, &out, &collector).unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);

    // 正常退出：Container 仍存活时部分结果恰好 drop 一次。
    let declared: Vec<RefId> = Vec::new();
    let mut slots: Vec<ExportSlot> = Vec::new();
    f.coordinator
        .finalize(&controller, &declared, &mut slots)
        .unwrap();
    assert_eq!(f.drops(), 1);
    assert_eq!(f.scope_state(&controller), ScopeState::Closed);
    assert!(matches!(
        f.coordinator.collector_len(&collector),
        Err(ScopeError::CollectorNotRegistered { .. })
    ));
    assert!(f.alive(&ancestor), "ancestor value survives");
    assert_eq!(f.owner(&ancestor).unwrap(), root);

    // ancestor 随其 owner 退出销毁：每个值恰好一次。
    let declared: Vec<RefId> = Vec::new();
    let mut slots: Vec<ExportSlot> = Vec::new();
    f.coordinator
        .finalize(&root, &declared, &mut slots)
        .unwrap();
    assert_eq!(f.drops(), 2);

    // Container 析构不产生额外 Drop。
    let drops = Arc::clone(&f.drops);
    let coordinator = f.coordinator;
    drop(coordinator);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// R7 收口：登记键与记录身份必须一致；清理不按未经验证的身份删除。
#[test]
fn r7_cleanup_rejects_collector_identity_binding_mismatch() {
    let mut f = Fixture::new();
    let root = f.root();

    // Root 的 C0：合法 Consume 一项，形成祖先的部分结果。
    let c0 = f.collector::<Tracked>(&root);
    let item = f.child(&root);
    let out = f.ref_id();
    f.coordinator
        .register_owned(&item, &out, f.tracked(1))
        .unwrap();
    f.coordinator.consume_item(&item, &out, &c0).unwrap();
    assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 1);

    // 子 Scope 的 C1：test-only 只把记录的 id 改成 C0，键与 owner 保留。
    let child = f.child(&root);
    let c1 = f.collector::<Tracked>(&child);
    {
        let record = f
            .coordinator
            .registry
            .collectors
            .get_mut(&c1.seq())
            .unwrap();
        record.id = c0.clone();
    }

    let error = f.coordinator.abort(&child).unwrap_err();
    assert!(
        matches!(error, ScopeError::Invariant { .. }),
        "got {error:?}"
    );

    // 祖先的建构值、Drop、登记与来源状态保持原状。
    assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 1);
    assert_eq!(f.drops(), 0);
    assert!(f.coordinator.collector_metadata(&c0).is_ok());
    assert!(
        f.coordinator.registry.collectors.contains_key(&c1.seq()),
        "the mismatched registration is not revoked by another identity"
    );
    assert_ne!(f.scope_state(&child), ScopeState::Closed);
    // 错配句柄不会重定向到 C0；合法句柄仍指向自己的登记。
    assert!(matches!(
        f.coordinator.collector_len(&c1),
        Err(ScopeError::Invariant { .. })
    ));
    assert_eq!(f.coordinator.collector_metadata(&c0).unwrap().1, root);
}

// ---- 成组 Consume（WE-T05 Core 侧；§5.2 全组预检／§5.3 提交与关闭） ----

use crate::core::test_support::ConsumeGroupSnapshotPhase as GroupPhase;

/// 建立 root→each→item 的真实 Scope 链，返回 (each, item)。
fn group_chain(f: &mut Fixture) -> (ScopeId, ScopeId) {
    let root = f.root();
    let each = f.child(&root);
    let item = f.child(&each);
    (each, item)
}

fn group_snapshots(phase: GroupPhase) -> Vec<crate::core::test_support::ConsumeGroupSnapshot> {
    crate::core::test_support::take_consume_group()
        .into_iter()
        .filter(|snapshot| snapshot.phase == phase)
        .collect()
}

#[test]
fn g01_group_consume_moves_every_slot_and_closes_item_once() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let c1 = f.collector::<Other>(&each);
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let id1 = f.coordinator.register_owned(&item, &p1, Other(2)).unwrap();

    let outcome = f.coordinator.consume_item_group_report(
        &item,
        &[p0.clone(), p1.clone()],
        &[c0.clone(), c1.clone()],
    );
    assert!(
        matches!(outcome, super::ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );

    // 每槽恰移动一次；Item 只关闭一次；旧 DataId 失效；owned／refs 撤销。
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 1);
    assert_eq!(f.coordinator.collector_moves(&c1).unwrap(), 1);
    assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 1);
    assert_eq!(f.coordinator.collector_len(&c1).unwrap(), 1);
    assert!(!f.alive(&id0));
    assert!(!f.alive(&id1));
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
    assert_eq!(f.owned_len(&item), 0);
    assert_eq!(f.refs_len(&item), 0);
    assert_eq!(
        f.scope_state(&each),
        ScopeState::Active,
        "collector owner remains alive after group consume"
    );

    let snapshots = crate::core::test_support::take_consume_group();
    assert_eq!(snapshots.len(), 2, "Before + AfterCommit");
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.observation_error.is_none()),
        "no observation failure"
    );
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == GroupPhase::Before)
        .expect("Before");
    let after = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == GroupPhase::AfterCommit)
        .expect("AfterCommit");
    assert!(
        before
            .slots
            .iter()
            .all(|slot| slot.collector_moves == Some(0))
    );
    assert!(before.slots.iter().all(|slot| slot.alive));
    assert_eq!(before.item_owned.len(), 2);
    assert!(
        after
            .slots
            .iter()
            .all(|slot| slot.collector_moves == Some(1))
    );
    assert!(after.slots.iter().all(|slot| !slot.alive));
    assert!(after.item_owned.is_empty());
    assert_eq!(after.item_state, Some(ScopeState::Finalizing));
    assert_eq!(
        after.item_refs.len(),
        2,
        "close 之前 refs 仍在（只在 close 一次时清空）"
    );
}

#[test]
fn g02_later_slot_reject_leaves_first_slot_unmoved() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let c1 = f.collector::<Tracked>(&each); // 目的元素是 Tracked，而 p1 是 Other
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let _id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let _id1 = f.coordinator.register_owned(&item, &p1, Other(2)).unwrap();

    let outcome = f.coordinator.consume_item_group_report(
        &item,
        &[p0.clone(), p1.clone()],
        &[c0.clone(), c1.clone()],
    );
    match outcome {
        super::ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::TypeMismatch { position, .. } if position == &p1),
                "got {primary:?}"
            );
            assert!(
                cleanup_failure.is_none(),
                "clean item cleans up: {cleanup_failure:?}"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }

    // 拒绝时点（cleanup 之前）：首槽零移动、两值仍存活、owned 未撤销。
    let rejected = group_snapshots(GroupPhase::AfterReject);
    assert_eq!(rejected.len(), 1);
    let rejected = &rejected[0];
    assert!(rejected.observation_error.is_none());
    assert!(
        rejected
            .slots
            .iter()
            .all(|slot| slot.collector_moves == Some(0))
    );
    assert!(rejected.slots.iter().all(|slot| slot.alive));
    assert_eq!(rejected.item_owned.len(), 2);
    assert_eq!(rejected.item_state, Some(ScopeState::Finalizing));

    // 拒绝收口后：collector 依旧完整、零移动；Item 已按失败路径关闭。
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 0);
    assert_eq!(f.coordinator.collector_moves(&c1).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 0);
    assert_eq!(f.coordinator.collector_len(&c1).unwrap(), 0);
    assert_eq!(f.scope_state(&item), ScopeState::Closed);
    assert_eq!(f.scope_state(&each), ScopeState::Active);
}

#[test]
fn g03_duplicate_destination_collector_is_typed_reject_before_freeze() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let id1 = f
        .coordinator
        .register_owned(&item, &p1, f.tracked(2))
        .unwrap();

    let outcome =
        f.coordinator
            .consume_item_group_report(&item, &[p0, p1], &[c0.clone(), c0.clone()]);
    match outcome {
        super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::DuplicateCollectorDestination { collector } if collector == &c0),
            "got {primary:?}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }
    // 纯结构错误：冻结前完成，Item 保持 Active／零副作用。
    assert_eq!(f.scope_state(&item), ScopeState::Active);
    assert!(f.alive(&id0) && f.alive(&id1));
    assert_eq!(f.owned_len(&item), 2);
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 0);
    assert_eq!(crate::core::test_support::take_consume_group().len(), 0);
}

#[test]
fn g05_empty_group_and_length_mismatch_are_rejected_before_freeze() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let c1 = f.collector::<Tracked>(&each);
    let p0 = f.ref_id();
    let id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();

    match f.coordinator.consume_item_group_report(&item, &[], &[]) {
        super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::Invariant { violated }
                    if *violated == "consume group must declare at least one output slot"),
            "got {primary:?}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }
    match f.coordinator.consume_item_group_report(
        &item,
        std::slice::from_ref(&p0),
        &[c0.clone(), c1.clone()],
    ) {
        super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::Invariant { violated }
                    if *violated == "consume group selected and destination collector counts must match"),
            "got {primary:?}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }
    assert_eq!(f.scope_state(&item), ScopeState::Active);
    assert!(f.alive(&id0));
    assert_eq!(f.owned_len(&item), 1);
    assert_eq!(crate::core::test_support::take_consume_group().len(), 0);
}

#[test]
fn g06_live_descendant_blocks_group_consume_before_freeze() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let p0 = f.ref_id();
    let id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let _grand = f.child(&item);

    match f.coordinator.consume_item_group_report(
        &item,
        std::slice::from_ref(&p0),
        std::slice::from_ref(&c0),
    ) {
        super::ConsumeGroupOutcome::Rejected { primary, .. } => {
            assert!(
                matches!(&primary, ScopeError::ActiveDescendants { scope } if scope == &item),
                "got {primary:?}"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    assert_eq!(f.scope_state(&item), ScopeState::Active);
    assert!(f.alive(&id0));
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 0);
}

#[test]
fn g07_cleanup_precondition_fault_reports_primary_and_cleanup_failure() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let p0 = f.ref_id();
    let extra = f.ref_id();
    let id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let _extra = f
        .coordinator
        .register_owned(&item, &extra, f.tracked(9))
        .unwrap();
    crate::core::test_support::install_consume_group_fault(
        crate::core::test_support::ConsumeGroupFault::FailCleanup,
    );

    match f.coordinator.consume_item_group_report(
        &item,
        std::slice::from_ref(&p0),
        std::slice::from_ref(&c0),
    ) {
        super::ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::Invariant { violated }
                        if *violated == "owned entry must exist until its scope closes"),
                "got {primary:?}"
            );
            assert!(
                matches!(&cleanup_failure, Some(ScopeError::Invariant { .. })),
                "cleanup 前提不足需独立报告: {cleanup_failure:?}"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    // 首槽零移动；Item 未关闭（清理前提不足，不得宣称 Closed）。
    let rejected = group_snapshots(GroupPhase::AfterReject);
    assert_eq!(rejected.len(), 1);
    assert!(
        rejected[0]
            .slots
            .iter()
            .all(|slot| slot.collector_moves == Some(0))
    );
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 0);
    assert_ne!(f.scope_state(&item), ScopeState::Closed);
    assert_ne!(f.scope_state(&item), ScopeState::Active);
    assert!(f.alive(&id0));
}

#[test]
fn g09_commit_fault_after_first_slot_is_observable_and_not_rolled_back() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let (each, item) = group_chain(&mut f);
    let c0 = f.collector::<Tracked>(&each);
    let c1 = f.collector::<Tracked>(&each);
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let _id0 = f
        .coordinator
        .register_owned(&item, &p0, f.tracked(1))
        .unwrap();
    let _id1 = f
        .coordinator
        .register_owned(&item, &p1, f.tracked(2))
        .unwrap();
    crate::core::test_support::install_consume_group_fault(
        crate::core::test_support::ConsumeGroupFault::FailCommitAt(1),
    );
    let outcome = f.coordinator.consume_item_group_report(
        &item,
        &[p0.clone(), p1.clone()],
        &[c0.clone(), c1.clone()],
    );
    // 生产提交段无可恢复失败；此注入仅证明不变量破坏不触发 rollback／假成功。
    assert!(matches!(
        outcome,
        super::ConsumeGroupOutcome::Rejected {
            primary: ScopeError::Invariant { .. },
            cleanup_failure: None
        }
    ));
    assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 1);
    assert_eq!(f.coordinator.collector_moves(&c1).unwrap(), 0);
}
