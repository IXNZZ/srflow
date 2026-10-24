use std::cell::Cell;

thread_local! {
    static CONTEXTS: Cell<usize> = const { Cell::new(0) };
    static COORDINATORS: Cell<usize> = const { Cell::new(0) };
    static CONTAINERS: Cell<usize> = const { Cell::new(0) };
    static GUARD_CLEANUPS: Cell<usize> = const { Cell::new(0) };
}

thread_local! {
    static EVENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// 记录一次由 guard 退出触发的清理（错误或取消）。
pub(crate) fn count_guard_cleanup() {
    GUARD_CLEANUPS.with(|count| count.set(count.get() + 1));
}

/// guard 退出清理的累计次数。
pub(crate) fn guard_cleanups() -> usize {
    GUARD_CLEANUPS.with(Cell::get)
}

pub(crate) fn count_context() {
    CONTEXTS.with(|count| count.set(count.get() + 1));
}

/// 在 `ScopeCoordinator::new` 的实际创建处记录协调组件。
pub(crate) fn count_coordinator() {
    COORDINATORS.with(|count| count.set(count.get() + 1));
}

/// 在 `DataContainer::with_identity` 的实际创建处记录 Container。
pub(crate) fn count_container() {
    CONTAINERS.with(|count| count.set(count.get() + 1));
}

/// 事件日志：按发生顺序记录析构／清理事件，用于验证先后关系。
pub(crate) fn record_event(event: &str) {
    EVENTS.with(|events| events.borrow_mut().push(event.to_string()));
}

/// 取走当前线程的事件序列。
pub(crate) fn take_events() -> Vec<String> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

/// 当前线程的计数快照。
///
/// 计数是**线程局部**的：每个测试在自己的线程上驱动同一条顺序执行路径，因此
/// 并发测试不会互相干扰；它不是跨执行的持久身份。
pub(crate) fn snapshot() -> (usize, usize, usize) {
    (
        CONTEXTS.with(Cell::get),
        COORDINATORS.with(Cell::get),
        CONTAINERS.with(Cell::get),
    )
}
