//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() { iterator.reset(nullptr); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  table_oid_t table_id = plan_->GetTableOid();
  TableMetadata *tableMetadata = exec_ctx_->GetCatalog()->GetTable(table_id);
  if (iterator == nullptr) {
    iterator = std::make_unique<TableIterator>(tableMetadata->table_->Begin(exec_ctx_->GetTransaction()));
  }
  TableIterator end = tableMetadata->table_->End();
  auto predicate = plan_->GetPredicate();
  while (iterator->operator!=(end)) {
    *tuple = iterator.operator*().operator*();
    *rid = iterator.operator*()->GetRid();
    (*iterator).operator++();
    if (predicate == nullptr || predicate->Evaluate(tuple, &tableMetadata->schema_).GetAs<bool>()) {
      return true;
    }
  }
  return !iterator->operator==(end);
}

}  // namespace bustub
