//! Choose 的定义期上下文 `Choice`（SPEC §9、Engine §6.7）。
//!
//! 每个 case／otherwise 的定义闭包在构建期同步执行一次，登记一个真实分支 Body；运行期
//! 只执行选中的那个分支（见 `boundary::invoke_choose`）。匹配键 `K` 只是 Definition
//! 配置：它不登记为业务输入，也不获得 DataId／Scope owner，Definition 不保存实际选中
//! 索引或业务结果。

use crate::core::engine::data::Data;
use crate::core::engine::definition::{Builder, ChooseBranch, ChooseCase};
use crate::core::engine::error::BuildErrorDetail;
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::{BodyId, PositionHandle};
use crate::core::engine::shape::{PositionDesc, RefShape, ShapeOps};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

/// 值匹配分支的定义上下文；由 [`Flow::choose`](crate::Flow::choose) 建立并在同一次
/// 同步定义调用内消费。字段私有：分支只能经 `case`／`otherwise` 登记，没有公开构造入口。
pub struct Choice<'n, K, O> {
    builder: Rc<RefCell<Builder<'n>>>,
    body: BodyId,
    step: usize,
    outputs: Vec<PositionHandle>,
    cases: Vec<ChooseCase<K>>,
    otherwise: Option<ChooseBranch>,
    marker: PhantomData<fn() -> O>,
}

impl<'n, K, O> Choice<'n, K, O> {
    pub(crate) fn new(
        builder: Rc<RefCell<Builder<'n>>>,
        body: BodyId,
        step: usize,
        outputs: Vec<PositionHandle>,
    ) -> Self {
        Self {
            builder,
            body,
            step,
            outputs,
            cases: Vec::new(),
            otherwise: None,
            marker: PhantomData,
        }
    }

    /// 取出已登记的分支配置；由 `Flow::choose` 在闭包返回后收口使用。
    pub(crate) fn into_parts(self) -> (Vec<ChooseCase<K>>, Option<ChooseBranch>) {
        (self.cases, self.otherwise)
    }
}

#[allow(private_bounds)]
impl<'n, K, O> Choice<'n, K, O>
where
    K: Data + PartialEq,
    O: RefShape + ShapeOps,
{
    /// 登记一个“匹配值 → 分支”配置；键按 `K` 的 `PartialEq` 与既有 case 判重。
    ///
    /// 重复值记录一条分支索引诊断并保持原配置；构建出口拒绝该 Definition，任何业务
    /// Node 都不会开始执行。
    #[allow(private_bounds)]
    pub fn case<F>(&mut self, key: K, define: F)
    where
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        if let Some(first) = self.cases.iter().position(|case| case.key == key) {
            self.builder
                .borrow_mut()
                .record_config_error(BuildErrorDetail::DuplicateCase {
                    body: self.body,
                    step: Some(self.step),
                    first_slot: first as u16,
                    second_slot: self.cases.len() as u16,
                });
            return;
        }
        let branch = self.build_branch(define);
        self.cases.push(ChooseCase { key, branch });
    }

    /// 登记无匹配时执行的分支；重复登记被构建拒绝（保留首次登记的分支）。
    #[allow(private_bounds)]
    pub fn otherwise<F>(&mut self, define: F)
    where
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        if self.otherwise.is_some() {
            self.builder
                .borrow_mut()
                .record_config_error(BuildErrorDetail::DuplicateOtherwise {
                    body: self.body,
                    step: Some(self.step),
                });
            return;
        }
        let branch = self.build_branch(define);
        self.otherwise = Some(branch);
    }

    /// 同步构建一个分支 Body：执行定义闭包、声明其输出、装配槽位出口。
    fn build_branch<F>(&mut self, define: F) -> ChooseBranch
    where
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        let builder = Rc::clone(&self.builder);
        let child = builder.borrow_mut().new_body(Some(self.body));
        let out = {
            let mut sub = Flow::open(Rc::clone(&builder), child);
            define(&mut sub)
        };
        let declared: Vec<PositionDesc> = out.positions();
        let branch = (self.cases.len() + usize::from(self.otherwise.is_some())) as u16;
        builder.borrow_mut().register_choose_branch(
            child,
            self.step,
            branch,
            declared,
            &self.outputs,
        )
    }
}
