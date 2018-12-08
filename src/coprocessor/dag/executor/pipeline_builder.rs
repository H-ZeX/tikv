// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};

use storage::{Snapshot, SnapshotStore};

use super::{BatchIndexScanExecutor, BatchTableScanExecutor};
use super::{Executor, ExecutorContext};
use super::{
    HashAggExecutor, IndexScanExecutor, LimitExecutor, SelectionExecutor, StreamAggExecutor,
    TableScanExecutor, TopNExecutor,
};
use coprocessor::dag::expr::EvalConfig;
use coprocessor::*;

pub struct ExecutorPipelineBuilder;

impl ExecutorPipelineBuilder {
    /// Given a list of executor descriptors and returns whether all executor descriptors can
    /// be used to build batch executors.
    pub fn can_build_batch(exec_descriptors: &[executor::Executor]) -> bool {
        use cop_datatype::EvalType;
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    for column in descriptor.get_columns() {
                        let eval_type = EvalType::try_from(column.tp());
                        match eval_type {
                            Err(_) => {
                                info!("Coprocessor request cannot be batched because column eval type {:?} is not supported", eval_type);
                                return false;
                            }
                            // Currently decimal or JSON field is not supported
                            Ok(EvalType::Decimal) | Ok(EvalType::Json) => {
                                info!("Coprocessor request cannot be batched because column eval type {:?} is not supported", eval_type);
                                return false;
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    info!(
                        "Coprocessor request cannot be batched because {:?} is not supported",
                        ed.get_tp()
                    );
                    return false;
                }
            }
        }
        true
    }

    // Note: `S` is `'static` because we have trait objects `Executor`.
    pub fn build_batch<S: Snapshot + 'static>(
        executor_descriptors: Vec<executor::Executor>,
        store: SnapshotStore<S>,
        ranges: Vec<KeyRange>,
        _eval_ctx: Arc<EvalConfig>,
    ) -> Result<Box<Executor + Send>> {
        let mut executor_descriptors = executor_descriptors.into_iter();
        let mut first_ed = executor_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("No executors")))?;

        let executor_context;
        let executor;

        match first_ed.get_tp() {
            ExecType::TypeTableScan => {
                let mut descriptor = first_ed.take_tbl_scan();
                executor_context = ExecutorContext::new(descriptor.take_columns().into_vec());
                executor = BatchTableScanExecutor::new(
                    store,
                    executor_context,
                    ranges,
                    descriptor.get_desc(),
                )?.into_boxed();
            }
            ExecType::TypeIndexScan => {
                let mut descriptor = first_ed.take_idx_scan();
                executor_context = ExecutorContext::new(descriptor.take_columns().into_vec());
                executor = BatchIndexScanExecutor::new(
                    store,
                    executor_context,
                    ranges,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                )?.into_boxed();
            }
            _ => {
                return Err(Error::Other(box_err!(
                    "Unexpected first executor {:?}",
                    first_ed.get_tp()
                )))
            }
        }

        Ok(executor)
    }

    pub fn build_normal<S: Snapshot + 'static>(
        execs: Vec<executor::Executor>,
        store: SnapshotStore<S>,
        ranges: Vec<KeyRange>,
        ctx: Arc<EvalConfig>,
        collect: bool,
    ) -> Result<Box<Executor + Send>> {
        let mut execs = execs.into_iter();
        let first = execs
            .next()
            .ok_or_else(|| Error::Other(box_err!("has no executor")))?;
        let mut src = Self::build_normal_first_executor(first, store, ranges, collect)?;
        for mut exec in execs {
            let curr: Box<Executor + Send> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!("got too much *scan exec, should be only one"))
                }
                ExecType::TypeSelection => Box::new(SelectionExecutor::new(
                    exec.take_selection(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeAggregation => Box::new(HashAggExecutor::new(
                    exec.take_aggregation(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeStreamAgg => Box::new(StreamAggExecutor::new(
                    Arc::clone(&ctx),
                    src,
                    exec.take_aggregation(),
                )?),
                ExecType::TypeTopN => {
                    Box::new(TopNExecutor::new(exec.take_topN(), Arc::clone(&ctx), src)?)
                }
                ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
            };
            src = curr;
        }
        Ok(src)
    }

    fn build_normal_first_executor<S: Snapshot + 'static>(
        mut first: executor::Executor,
        store: SnapshotStore<S>,
        ranges: Vec<KeyRange>,
        collect: bool,
    ) -> Result<Box<Executor + Send>> {
        match first.get_tp() {
            ExecType::TypeTableScan => {
                let ex = Box::new(TableScanExecutor::new(
                    first.take_tbl_scan(),
                    ranges,
                    store,
                    collect,
                )?);
                Ok(ex)
            }
            ExecType::TypeIndexScan => {
                let unique = first.get_idx_scan().get_unique();
                let ex = Box::new(IndexScanExecutor::new(
                    first.take_idx_scan(),
                    ranges,
                    store,
                    unique,
                    collect,
                )?);
                Ok(ex)
            }
            _ => Err(box_err!(
                "first exec type should be *Scan, but get {:?}",
                first.get_tp()
            )),
        }
    }
}
