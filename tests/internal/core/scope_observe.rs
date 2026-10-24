//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::scope::*;

#[cfg(test)]
impl TargetSnapshot {
    pub(super) fn of(target: &RefTarget) -> Self {
        match target {
            RefTarget::Data(id) => Self::Data(id.clone()),
            RefTarget::CollectionItem {
                collection,
                index,
                lifetime_cap,
                access,
            } => Self::CollectionItem {
                collection: collection
                    .root_data()
                    .expect("an item collection source always resolves to a real root Data")
                    .clone(),
                index: *index,
                lifetime_cap: lifetime_cap.clone(),
                collection_type: access.collection_type(),
                element_type: access.element_type(),
            },
        }
    }
}

#[cfg(test)]
impl ScopeCoordinator {
    pub(crate) fn inject_consume_group_cleanup_fault(
        &mut self,
        item: &ScopeId,
        selected: &[RefId],
    ) {
        if crate::core::test_support::take_consume_group_fault_if(|fault| {
            matches!(
                fault,
                crate::core::test_support::ConsumeGroupFault::FailCleanup
            )
        })
        .is_none()
        {
            return;
        }
        let Ok(record) = self.registry.lookup(item) else {
            return;
        };
        let mut selected_ids: Vec<DataId> = Vec::new();
        for position in selected {
            if let Some(id) = record
                .refs
                .get(position)
                .and_then(|target| target.data_id())
            {
                selected_ids.push(id.clone());
            }
        }
        let candidate = record
            .owned
            .iter()
            .filter(|id| !selected_ids.iter().any(|selected| selected == *id))
            .min_by_key(|id| id.seq())
            .cloned()
            .or_else(|| selected_ids.first().cloned());
        if let Some(id) = candidate {
            self.container.destroy(&id).ok();
        }
    }
    pub(crate) fn record_round_collect_probe(
        &self,
        phase: crate::core::test_support::RoundCollectSnapshotPhase,
        operation: crate::core::test_support::RoundCollectOperation,
        source: &ScopeId,
        selected: Option<&RefId>,
        state: Option<&ControlStateId>,
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let source_state = self.registry.lookup(source).ok().map(|record| record.state);
        let (source_refs, source_owned) = match self.snapshot_targets_probe(source) {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("source snapshot: {err:?}"));
                (None, None)
            }
        };
        let controller = state.map(|state| state.owner().clone()).or_else(|| {
            self.registry
                .lookup(source)
                .ok()
                .and_then(|record| record.parent.clone())
        });
        let (controller_refs, controller_owned) = match controller.as_ref() {
            Some(controller) => match self.snapshot_targets_probe(controller) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("controller snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let selected_target_value = selected.and_then(|selected| {
            self.registry
                .lookup(source)
                .ok()
                .and_then(|record| record.refs.get(selected).cloned())
        });
        let selected_target = selected_target_value.as_ref().map(TargetSnapshot::of);
        let selected_collection_owner = match selected_target.as_ref() {
            Some(TargetSnapshot::CollectionItem { collection, .. }) => {
                self.owner_of(collection).ok()
            }
            _ => None,
        };
        let (selected_data, selected_owner, selected_alive) = match selected {
            Some(selected) => match self
                .registry
                .lookup(source)
                .ok()
                .and_then(|record| record.refs.get(selected).cloned())
            {
                Some(RefTarget::Data(id)) => (
                    Some(id.clone()),
                    self.owner_of(&id).ok(),
                    self.container.borrow_any(&id).is_ok(),
                ),
                Some(RefTarget::CollectionItem { .. }) => (None, None, false),
                None => {
                    note(format!(
                        "selected position {:?} is not bound",
                        selected.seq()
                    ));
                    (None, None, false)
                }
            },
            None => (None, None, false),
        };
        let (state_target, state_target_owner, state_target_alive, state_pending) = match state {
            Some(state) => match self.lookup_state(state) {
                Ok(record) => {
                    let target_value = record.target.clone();
                    let owner = target_value.as_ref().and_then(|target| match target {
                        RefTarget::Data(id) => self.owner_of(id).ok(),
                        RefTarget::CollectionItem { .. } => None,
                    });
                    let alive = target_value.as_ref().is_some_and(|target| match target {
                        RefTarget::Data(id) => self.container.borrow_any(id).is_ok(),
                        RefTarget::CollectionItem { .. } => false,
                    });
                    (
                        target_value.map(|target| TargetSnapshot::of(&target)),
                        owner,
                        alive,
                        Some(record.pending.clone()),
                    )
                }
                Err(err) => {
                    note(format!("state lookup: {err:?}"));
                    (None, None, false, None)
                }
            },
            None => (None, None, false, None),
        };
        crate::core::test_support::record_round_collect_pre_cleanup(
            crate::core::test_support::RoundCollectPreCleanupSnapshot {
                phase,
                operation,
                source: Some(source.clone()),
                source_state,
                source_refs,
                source_owned,
                controller,
                controller_refs,
                controller_owned,
                selected_data,
                selected_target,
                selected_collection_owner,
                selected_owner,
                selected_alive,
                state_target,
                state_target_owner,
                state_target_alive,
                state_pending,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
    }
    pub(crate) fn record_round_collect_group_probe(
        &self,
        phase: crate::core::test_support::RoundCollectSnapshotPhase,
        source: &ScopeId,
        selected: &[RefId],
        states: &[ControlStateId],
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let (source_refs, source_owned) = match self.snapshot_targets_probe(source) {
            Ok((refs, owned)) => (refs, owned),
            Err(err) => {
                note(format!("source snapshot: {err:?}"));
                (Vec::new(), Vec::new())
            }
        };
        let controller = states
            .first()
            .map(|state| state.owner().clone())
            .or_else(|| {
                self.registry
                    .lookup(source)
                    .ok()
                    .and_then(|record| record.parent.clone())
            });
        let (controller_refs, controller_owned) = match controller.as_ref() {
            Some(controller) => match self.snapshot_targets_probe(controller) {
                Ok((refs, owned)) => (refs, owned),
                Err(err) => {
                    note(format!("controller snapshot: {err:?}"));
                    (Vec::new(), Vec::new())
                }
            },
            None => (Vec::new(), Vec::new()),
        };
        let mut slots = Vec::with_capacity(selected.len());
        for position in selected {
            let target_value = self
                .registry
                .lookup(source)
                .ok()
                .and_then(|record| record.refs.get(position).cloned());
            let target = target_value.as_ref().map(TargetSnapshot::of);
            let slot = match &target_value {
                Some(RefTarget::Data(id)) => crate::core::test_support::GroupSlotSnapshot {
                    position: position.clone(),
                    target,
                    owner: self.owner_of(id).ok(),
                    alive: self.container.borrow_any(id).is_ok(),
                },
                Some(RefTarget::CollectionItem { .. }) => {
                    crate::core::test_support::GroupSlotSnapshot {
                        position: position.clone(),
                        target,
                        owner: None,
                        alive: false,
                    }
                }
                None => {
                    note(format!(
                        "selected position {:?} is not bound",
                        position.seq()
                    ));
                    crate::core::test_support::GroupSlotSnapshot {
                        position: position.clone(),
                        target: None,
                        owner: None,
                        alive: false,
                    }
                }
            };
            slots.push(slot);
        }
        let mut state_snapshots = Vec::with_capacity(states.len());
        for state in states {
            let snapshot = match self.lookup_state(state) {
                Ok(record) => {
                    let target = record.target.clone();
                    let alive = target.as_ref().is_some_and(|target| match target {
                        RefTarget::Data(id) => self.container.borrow_any(id).is_ok(),
                        RefTarget::CollectionItem { .. } => false,
                    });
                    crate::core::test_support::GroupStateSnapshot {
                        owner: record.owner.clone(),
                        seq: state.seq(),
                        target: target.map(|target| TargetSnapshot::of(&target)),
                        alive,
                        pending: record.pending.clone(),
                    }
                }
                Err(err) => {
                    note(format!("state lookup: {err:?}"));
                    crate::core::test_support::GroupStateSnapshot {
                        owner: state.owner().clone(),
                        seq: state.seq(),
                        target: None,
                        alive: false,
                        pending: Vec::new(),
                    }
                }
            };
            state_snapshots.push(snapshot);
        }
        crate::core::test_support::record_round_collect_group(
            crate::core::test_support::RoundCollectGroupSnapshot {
                phase,
                source: source.clone(),
                selected: slots,
                states: state_snapshots,
                source_refs,
                source_owned,
                controller_refs,
                controller_owned,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
    }
    pub(crate) fn record_consume_group_probe(
        &self,
        phase: crate::core::test_support::ConsumeGroupSnapshotPhase,
        item: &ScopeId,
        selected: &[RefId],
        collectors: &[CollectorId],
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let item_state = self.registry.lookup(item).ok().map(|record| record.state);
        let (item_refs, item_owned) = match self.snapshot_targets_probe(item) {
            Ok((refs, owned)) => (refs, owned),
            Err(err) => {
                note(format!("item snapshot: {err:?}"));
                (Vec::new(), Vec::new())
            }
        };
        let each = self
            .registry
            .lookup(item)
            .ok()
            .and_then(|record| record.parent.clone());
        let (each_refs, each_owned) = match each.as_ref() {
            Some(each) => match self.snapshot_targets_probe(each) {
                Ok((refs, owned)) => (refs, owned),
                Err(err) => {
                    note(format!("each snapshot: {err:?}"));
                    (Vec::new(), Vec::new())
                }
            },
            None => (Vec::new(), Vec::new()),
        };
        let mut slots = Vec::with_capacity(selected.len().max(collectors.len()));
        for (index, position) in selected.iter().enumerate() {
            let target_value = self
                .registry
                .lookup(item)
                .ok()
                .and_then(|record| record.refs.get(position).cloned());
            let target = target_value.as_ref().map(TargetSnapshot::of);
            let (data_id, owner, alive) = match &target_value {
                Some(RefTarget::Data(id)) => (
                    Some(id.clone()),
                    self.owner_of(id).ok(),
                    self.container.borrow_any(id).is_ok(),
                ),
                _ => (None, None, false),
            };
            let (collector, collector_element, collector_owner, collector_moves) =
                match collectors.get(index) {
                    Some(collector) => match self.lookup_collector(collector) {
                        Ok(record) => (
                            Some(collector.clone()),
                            Some(record.element_name),
                            Some(record.owner.clone()),
                            self.container.collector_moves(collector).ok(),
                        ),
                        Err(err) => {
                            note(format!("collector lookup {index}: {err:?}"));
                            (
                                Some(collector.clone()),
                                None,
                                None,
                                self.container.collector_moves(collector).ok(),
                            )
                        }
                    },
                    None => (None, None, None, None),
                };
            slots.push(crate::core::test_support::ConsumeGroupSlotSnapshot {
                position: position.clone(),
                target,
                data_id,
                owner,
                alive,
                collector,
                collector_element,
                collector_owner,
                collector_moves,
            });
        }
        crate::core::test_support::record_consume_group(
            crate::core::test_support::ConsumeGroupSnapshot {
                phase,
                item: item.clone(),
                item_state,
                item_refs,
                item_owned,
                each,
                each_refs,
                each_owned,
                slots,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
    }
    pub(crate) fn record_pre_cleanup_probe(
        &self,
        phase: crate::core::test_support::ConsumeSnapshotPhase,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let item_state = self.snapshot_targets_probe(item);
        let (item_refs, item_owned) = match item_state {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("item snapshot: {err:?}"));
                (None, None)
            }
        };
        let parent = self
            .registry
            .lookup(item)
            .ok()
            .and_then(|record| record.parent.clone());
        let (parent_refs, parent_owned) = match parent.as_ref() {
            Some(parent) => match self.snapshot_targets_probe(parent) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("parent snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let selected_target = self
            .registry
            .lookup(item)
            .ok()
            .and_then(|record| record.refs.get(selected).cloned());
        let (selected_data, selected_owner, selected_alive) = match selected_target {
            Some(RefTarget::Data(id)) => (
                Some(id.clone()),
                self.owner_of(&id).ok(),
                self.container.borrow_any(&id).is_ok(),
            ),
            _ => (None, None, false),
        };
        let (collector_element, collector_owner, collector_moves) =
            match self.lookup_collector(collector) {
                Ok(record) => (
                    Some(record.element_name),
                    Some(record.owner.clone()),
                    self.container.collector_moves(collector).ok(),
                ),
                Err(err) => {
                    note(format!("collector lookup: {err:?}"));
                    (None, None, None)
                }
            };
        crate::core::test_support::record_consume_pre_cleanup(
            crate::core::test_support::ConsumePreCleanupSnapshot {
                phase,
                item_refs,
                item_owned,
                parent_refs,
                parent_owned,
                selected_data,
                selected_owner,
                selected_alive,
                collector_element,
                collector_owner,
                collector_moves,
                observation_error: error,
            },
        );
    }
    pub(crate) fn record_export_probe(
        &self,
        phase: crate::core::test_support::ExportSnapshotPhase,
        child: &ScopeId,
        declared: &[RefId],
        outputs: &[ExportSlot],
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let caller = self
            .registry
            .lookup(child)
            .ok()
            .and_then(|r| r.parent.clone());
        let (child_refs, child_owned) = match self.snapshot_targets_probe(child) {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("child snapshot: {err:?}"));
                (None, None)
            }
        };
        let (caller_refs, caller_owned) = match caller.as_ref() {
            Some(caller) => match self.snapshot_targets_probe(caller) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("caller snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let slots: Vec<(RefId, RefId, &'static str)> = outputs
            .iter()
            .map(|slot| (slot.child.clone(), slot.caller.clone(), slot.expected_name))
            .collect();
        let conflict = outputs
            .iter()
            .find(|slot| {
                caller
                    .as_ref()
                    .and_then(|caller| self.registry.lookup(caller).ok())
                    .is_some_and(|record| record.refs.contains_key(&slot.caller))
            })
            .map(|slot| slot.caller.clone());
        let conflict_target = match conflict.as_ref() {
            Some(position) => caller
                .as_ref()
                .and_then(|caller| self.registry.lookup(caller).ok())
                .and_then(|record| record.refs.get(position).cloned())
                .map(|target| TargetSnapshot::of(&target)),
            None => None,
        };
        let (conflict_owner, conflict_alive) = match conflict_target.as_ref() {
            Some(TargetSnapshot::Data(id)) => (
                self.owner_of(id).ok(),
                self.container.borrow_any(id).is_ok(),
            ),
            _ => (None, false),
        };
        let caller_state = caller
            .as_ref()
            .and_then(|caller| self.registry.lookup(caller).ok())
            .map(|record| record.state);
        crate::core::test_support::record_export_pre_cleanup(
            crate::core::test_support::ExportPreCleanupSnapshot {
                phase,
                child: Some(child.clone()),
                caller,
                child_refs,
                child_owned,
                caller_refs,
                caller_owned,
                slots,
                conflict_target,
                conflict_owner,
                conflict_alive,
                caller_state,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
        let _ = declared;
    }
    /// cfg(test) 窄故障：把控制状态 target 的声明类型改为指定元数据。
    pub(crate) fn corrupt_state_type_probe(
        &mut self,
        state: &ControlStateId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        let key = (state.owner().seq(), state.seq());
        let record =
            self.registry
                .states
                .get_mut(&key)
                .ok_or_else(|| ScopeError::StateNotRegistered {
                    state: state.clone(),
                })?;
        record.expected = expected;
        record.expected_name = expected_name;
        Ok(())
    }
    /// cfg(test) 窄故障：把一个**本地位置**上的 item target 的 cap 换成指定 Scope。
    pub(crate) fn corrupt_item_cap_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        cap: ScopeId,
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup_mut(scope)?;
        match record.refs.get_mut(position) {
            Some(RefTarget::CollectionItem { lifetime_cap, .. }) => {
                *lifetime_cap = cap;
                Ok(())
            }
            _ => Err(ScopeError::Invariant {
                violated: "item cap fault requires a collection item target",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn refs_len_probe(&self, scope: &ScopeId) -> Result<usize, ScopeError> {
        Ok(self.registry.lookup(scope)?.refs.len())
    }

    #[cfg(test)]
    pub(crate) fn destroy_probe(&mut self, id: &DataId) {
        self.container
            .destroy(id)
            .expect("fixture only destroys live entries");
    }

    #[cfg(test)]
    pub(crate) fn inject_target_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        target: RefTarget,
    ) {
        self.registry
            .lookup_mut(scope)
            .expect("fixture scope exists")
            .refs
            .insert(position.clone(), target);
    }

    #[cfg(test)]
    pub(crate) fn relocate_owned_probe(&mut self, id: &DataId, to: &ScopeId) {
        for record in self.registry.scopes.values_mut() {
            record.owned.remove(id);
        }
        self.registry
            .lookup_mut(to)
            .expect("fixture target scope exists")
            .owned
            .insert(id.clone());
    }

    #[cfg(test)]
    pub(crate) fn replace_item_cap_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        cap: &ScopeId,
    ) {
        if let Some(record) = self.registry.scopes.get_mut(&scope.seq())
            && let Some(RefTarget::CollectionItem { lifetime_cap, .. }) =
                record.refs.get_mut(position)
        {
            *lifetime_cap = cap.clone();
        }
    }

    #[cfg(test)]
    pub(crate) fn owned_len_probe(&self, scope: &ScopeId) -> Result<usize, ScopeError> {
        Ok(self.registry.lookup(scope)?.owned.len())
    }

    #[cfg(test)]
    pub(crate) fn alive_probe(&self, id: &DataId) -> bool {
        self.container.validate(id).is_ok()
    }

    #[cfg(test)]
    pub(crate) fn owner_probe(&self, id: &DataId) -> Result<ScopeId, ScopeError> {
        self.owner_of(id)
    }

    #[cfg(test)]
    pub(crate) fn container_probe(&self) -> *const () {
        std::ptr::from_ref(&self.container).cast::<()>()
    }

    #[cfg(test)]
    pub(crate) fn coordinator_probe(&self) -> *const () {
        std::ptr::from_ref(self).cast::<()>()
    }

    #[cfg(test)]
    pub(crate) fn identity_probe(&self) -> *const () {
        Arc::as_ptr(&self.registry.execution).cast::<()>()
    }

    #[cfg(test)]
    pub(crate) fn collector_moves_probe(
        &self,
        collector: &CollectorId,
    ) -> Result<usize, ScopeError> {
        self.container
            .collector_moves(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    #[cfg(test)]
    pub(crate) fn snapshot_targets_probe(
        &self,
        scope: &ScopeId,
    ) -> Result<TargetSnapshotPair, ScopeError> {
        let record = self.registry.lookup(scope)?;
        let mut refs: Vec<(RefId, TargetSnapshot)> = record
            .refs
            .iter()
            .map(|(position, target)| (position.clone(), TargetSnapshot::of(target)))
            .collect();
        refs.sort_by_key(|(position, _)| position.seq());
        let mut owned: Vec<DataId> = record.owned.iter().cloned().collect();
        owned.sort_by_key(DataId::seq);
        Ok((refs, owned))
    }

    #[cfg(test)]
    pub(crate) fn collector_moves(&self, collector: &CollectorId) -> Result<usize, ScopeError> {
        let _ = self.lookup_collector(collector)?;
        self.container
            .collector_moves(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    #[cfg(test)]
    pub(crate) fn next_data_id_probe(&self) -> Option<u64> {
        self.container.next_data_id_probe()
    }

    #[cfg(test)]
    pub(crate) fn target_data_id_probe(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<Option<DataId>, ScopeError> {
        let record = self.registry.lookup(scope)?;
        Ok(record
            .refs
            .get(position)
            .and_then(|target| target.data_id().cloned()))
    }

    #[cfg(test)]
    pub(crate) fn pending_probe(&self, controller: &ScopeId) -> Option<Vec<DataId>> {
        let mut pending: Vec<DataId> = Vec::new();
        for state in self.registry.states.values() {
            if state.owner == *controller {
                pending.extend(state.pending.iter().cloned());
            }
        }
        Some(pending)
    }

    #[cfg(test)]
    pub(crate) fn with_state_start(
        &mut self,
        controller: &ScopeId,
        start: u64,
    ) -> Result<(), ScopeError> {
        self.registry.lookup_mut(controller)?.state_next = start;
        Ok(())
    }

    #[cfg(test)]
    /// cfg(test) 完整只读观察（成组 Consume 提交后）：逐槽位置与 collector 快照。
    pub(super) fn record_consume_group_after_commit(&self, plan: &PreparedConsumeGroup) {
        let selected: Vec<RefId> = plan
            .slots
            .iter()
            .map(|slot| slot.position.clone())
            .collect();
        let collectors: Vec<CollectorId> = plan
            .slots
            .iter()
            .map(|slot| slot.collector.clone())
            .collect();
        self.record_consume_group_probe(
            crate::core::test_support::ConsumeGroupSnapshotPhase::AfterCommit,
            &plan.item,
            &selected,
            &collectors,
        );
    }

    #[cfg(test)]
    pub(super) fn record_promote_group_state_probe(&self, plans: &[PreparedPromote]) {
        let Some(first) = plans.first() else {
            return;
        };
        let controller = first.controller.clone();
        let source = first.source.clone();
        let mut states = Vec::with_capacity(plans.len());
        for plan in plans {
            let (target, pending) = self
                .registry
                .states
                .get(&plan.state_key)
                .map(|state| {
                    (
                        state.target.as_ref().map(TargetSnapshot::of),
                        state.pending.clone(),
                    )
                })
                .unwrap_or((None, Vec::new()));
            states.push((plan.controller.clone(), plan.state_key.1, target, pending));
        }
        let mut transferred: Vec<DataId> = Vec::new();
        for plan in plans {
            if plan.transferred
                && let Some(id) = plan.target.data_id()
                && !transferred.iter().any(|seen| seen == id)
            {
                transferred.push(id.clone());
            }
        }
        let mut controller_owned: Vec<DataId> = self
            .registry
            .lookup(&controller)
            .map(|record| record.owned.iter().cloned().collect())
            .unwrap_or_default();
        controller_owned.sort_by_key(DataId::seq);
        crate::core::test_support::record_promote_group_state(
            crate::core::test_support::PromoteGroupStateRecord {
                controller,
                source,
                states,
                transferred,
                controller_owned,
            },
        );
    }
}

#[cfg(test)]
pub(crate) type TargetSnapshotPair = (Vec<(RefId, TargetSnapshot)>, Vec<DataId>);

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetSnapshot {
    /// 完整 Data 实例。
    Data(DataId),
    /// 集合元素目标（含来源集合、下标、cap 与声明类型）。
    CollectionItem {
        /// 来源集合：整条来源链最外层的真实根 Data（嵌套 item-backed 集合亦然）。
        collection: DataId,
        /// 元素下标。
        index: usize,
        /// item 的 lifetime cap。
        lifetime_cap: ScopeId,
        /// 声明的集合类型。
        collection_type: TypeId,
        /// 声明的元素类型。
        element_type: TypeId,
    },
}

impl CollectionRoot {
    #[cfg(test)]
    pub(super) fn root_data(&self) -> Option<&DataId> {
        match self {
            Self::Data(id) => Some(id),
            Self::Item(target) => match target.as_ref() {
                RefTarget::Data(id) => Some(id),
                RefTarget::CollectionItem { collection, .. } => collection.root_data(),
            },
        }
    }
}

impl PreparedRootExtraction {
    /// 计划中按声明顺序的物理实例（只读观测；不授予 take 权）。
    #[cfg(test)]
    pub(crate) fn taken_ids(&self) -> Vec<DataId> {
        self.selections.iter().map(|(_, id)| id.clone()).collect()
    }
}

/// cfg(test) 故障注入：整组 Consume 的预检拒绝点（第 `index` 槽）。
pub(super) fn consume_group_reject_preflight_at(index: usize) -> bool {
    crate::core::test_support::take_consume_group_fault_if(|fault| {
        matches!(
            fault,
            crate::core::test_support::ConsumeGroupFault::RejectPreflightAt(at) if *at == index
        )
    })
    .is_some()
}

/// cfg(test) 故障注入：整组 Consume 的提交失败点（第 `index` 槽）。
pub(super) fn consume_group_fail_commit_at(index: usize) -> bool {
    crate::core::test_support::take_consume_group_fault_if(|fault| {
        matches!(
            fault,
            crate::core::test_support::ConsumeGroupFault::FailCommitAt(at) if *at == index
        )
    })
    .is_some()
}
