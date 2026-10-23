//! Choose 的定义期上下文 `Choice`（SPEC §8.2、Engine §6.7）。
//!
//! 每个 case／otherwise 的定义闭包在构建期同步执行一次，登记一个真实分支 Body；分支的
//! 输出类型来自该 Body 内的 `output`／`output_with` 声明，不由闭包返回值给出。运行期
//! 只执行选中的那个分支（见 `boundary::invoke_choose`）。匹配键 `K` 只是 Definition
//! 配置：它不登记为业务输入，也不获得 DataId／Scope owner，Definition 不保存实际选中
//! 索引或业务结果。

use crate::core::engine::data::Data;
use crate::core::engine::definition::{BodyRole, Builder};
use crate::core::engine::error::BuildErrorDetail;
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::{BodyId, PositionHandle};
use std::cell::RefCell;
use std::rc::Rc;

/// 值匹配分支的定义上下文；由 [`Flow::choose`](crate::Flow::choose) 建立并在同一次
/// 同步定义调用内消费。字段私有：分支只能经 `case`／`otherwise` 登记，没有公开构造入口。
///
/// 类型只带父 Body 的借用期与匹配键 `K`：不预持有父输出槽，也不携带分支输出类型；
/// 各分支的共同契约在闭包返回后由 Builder 统一读取、比较与发布。
pub struct Choice<'n, K> {
    builder: Rc<RefCell<Builder<'n>>>,
    body: BodyId,
    step: usize,
    cases: Vec<(K, BodyId)>,
    otherwise: Option<BodyId>,
}

impl<'n, K> Choice<'n, K> {
    pub(crate) fn new(builder: Rc<RefCell<Builder<'n>>>, body: BodyId, step: usize) -> Self {
        Self {
            builder,
            body,
            step,
            cases: Vec::new(),
            otherwise: None,
        }
    }

    /// 取出已登记的分支配置（匹配值 → 分支 Body）；由 `Flow` 在闭包返回后收口使用。
    pub(crate) fn into_parts(self) -> (Vec<(K, BodyId)>, Option<BodyId>) {
        (self.cases, self.otherwise)
    }
}

impl<'n, K> Choice<'n, K>
where
    K: Data + PartialEq,
{
    /// 登记一个“匹配值 → 分支”配置；键按 `K` 的 `PartialEq` 与既有 case 判重。
    ///
    /// 重复值记录一条分支索引诊断并保持原配置；构建出口拒绝该 Definition，任何业务
    /// Node 都不会开始执行。分支 Body 是普通 unit Body：输出在该 Body 内声明。
    pub fn case<F>(&mut self, key: K, define: F)
    where
        F: FnOnce(&mut Flow<'n>),
    {
        if let Some(first) = self.cases.iter().position(|(existing, _)| *existing == key) {
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
        self.cases.push((key, branch));
    }

    /// 登记无匹配时执行的分支；重复登记被构建拒绝（保留首次登记的分支）。
    pub fn otherwise<F>(&mut self, define: F)
    where
        F: FnOnce(&mut Flow<'n>),
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

    /// 同步构建一个分支 Body（Ordinary child）：执行 unit Body，不在此补声明。
    ///
    /// 分支输出声明属于该 Body 自身；父输出槽在全部分支构建完成后才按共同契约分配，
    /// 因此分支构建期间看不到尚未成立的父输出。
    fn build_branch<F>(&mut self, define: F) -> BodyId
    where
        F: FnOnce(&mut Flow<'n>),
    {
        let child = self
            .builder
            .borrow_mut()
            .new_body(Some(self.body), BodyRole::Ordinary);
        let mut sub = Flow::open(Rc::clone(&self.builder), child);
        define(&mut sub);
        child
    }
}

/// 供结构检查使用的空占位：Choose 不再预持有父输出句柄。
#[allow(dead_code)] // 由定向测试与结构检查使用
pub(crate) type ChoiceParentOutputs = Vec<PositionHandle>;
