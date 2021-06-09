//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t innerTableId = plan_->GetInnerTableOid();
  inner_table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(innerTableId);
}

void NestIndexJoinExecutor::Init() {}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  plan_->OuterTableSchema();
  // where is the outer table info ?
  IndexInfo *inner_index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_meta_data_->name_);
  for (; child_executor_->Next(&last_outer_tuple_, &last_outer_rid_);) {
    Tuple index_key = last_outer_tuple_.KeyFromTuple(*plan_->OuterTableSchema(), inner_index_info->key_schema_,
                                                     inner_index_info->index_->GetKeyAttrs());
    std::vector<RID> results;
    inner_index_info->index_->ScanKey(index_key, &results, exec_ctx_->GetTransaction());
    if (results.empty()) {
      continue;
    }
    // in this proj index has no duplicate key
    assert(results.size() == 1);
    // construct a tuple by rid
    Tuple innerTuple;
    bool ok = inner_table_meta_data_->table_->GetTuple(results[0], &innerTuple, exec_ctx_->GetTransaction());
    if (ok) {
      // pack the result tuple and return
      // TODO(rewindding) write a function to do this,
      // TODO(rewindding) make sure no duplicate column problem
      std::vector<Value> values;
      for (uint32_t i = 0; i < plan_->OuterTableSchema()->GetColumnCount(); ++i) {
        auto value = last_outer_tuple_.GetValue(plan_->OuterTableSchema(), i);
        values.push_back(value);
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema()->GetColumnCount(); ++i) {
        auto value = innerTuple.GetValue(plan_->InnerTableSchema(), i);
        values.push_back(value);
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
