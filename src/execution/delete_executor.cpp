//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  table_oid_t tableOid = plan_->TableOid();
  TableMetadata *tableMetadata = exec_ctx_->GetCatalog()->GetTable(tableOid);
  tableMetadata->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
  // delete entry from all relative index
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(tableMetadata->name_);
  for (IndexInfo *index : indexes) {
    Tuple indexKey = tuple->KeyFromTuple(tableMetadata->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(indexKey, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
