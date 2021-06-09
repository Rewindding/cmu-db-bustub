//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)  // What is child_excutor?
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  table_oid_t tableOid = plan_->TableOid();
  TableMetadata *tableMetadata = exec_ctx_->GetCatalog()->GetTable(tableOid);
  Tuple newTuple = GenerateUpdatedTuple(*tuple);
  bool updated = tableMetadata->table_->UpdateTuple(newTuple, *rid, exec_ctx_->GetTransaction());
  if (!updated) {
    return updated;
  }
  // update all indexes
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(tableMetadata->name_);
  for (IndexInfo *index : indexes) {
    // how to check if this index need not to update,e.g the key does not change,key attibutes
    // index->index_->GetKeyAttrs(); if key attributes x in plan_->GetUpdateAttr(), the index should be updated
    bool shouldUpdate = false;
    for (auto keyAttrs : index->index_->GetKeyAttrs()) {
      if (plan_->GetUpdateAttr()->count(keyAttrs) != 0U) {
        shouldUpdate = true;
        break;
      }
    }
    if (!shouldUpdate) {
      continue;
    }
    // 这里delete entry是基于tuple的，假如有重复的key，如何能确保删除掉这个tuple对应的那条记录吗？
    index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    index->index_->InsertEntry(newTuple, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub
