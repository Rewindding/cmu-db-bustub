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
#include <execution/expressions/column_value_expression.h>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  //

  table_oid_t table_id = plan_->GetTableOid();
  table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto tableSchema = table_meta_data_->schema_;
  auto outputSchema = plan_->OutputSchema();
  for (const Column &column : outputSchema->GetColumns()) {
    auto colIdx = dynamic_cast<const ColumnValueExpression *>(column.GetExpr())->GetColIdx();
    output_tuple_key_attrs_.emplace_back(colIdx);
  }
}

void SeqScanExecutor::Init() { iterator.reset(nullptr); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iterator == nullptr) {
    iterator = std::make_unique<TableIterator>(table_meta_data_->table_->Begin(exec_ctx_->GetTransaction()));
  }
  TableIterator end = table_meta_data_->table_->End();
  auto predicate = plan_->GetPredicate();
  while (iterator->operator!=(end)) {
    *rid = iterator.operator*()->GetRid();
    // lock rid之前有可能已经获取过了，这个应该收到lock manager里面处理，如果该txn已经获取了对应lock，直接return
    // true是没问题的？类似于recursive mutex
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED: {
        // 根本不用lock?
        break;
      }
      case IsolationLevel::REPEATABLE_READ: {
        // two phase locking?
        exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), *rid);
        break;
      }
      case IsolationLevel::READ_COMMITTED: {
        // rlock过后return前就unlock？
        exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), *rid);
        break;
      }
    }
    Tuple t = iterator.operator*().operator*();
    // form the output schema
    *tuple = t.KeyFromTuple(table_meta_data_->schema_, *plan_->OutputSchema(), output_tuple_key_attrs_);
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::REPEATABLE_READ: {
        // two phase locking?
        // release lock when txn commit
        break;
      }
      case IsolationLevel::READ_COMMITTED: {
        // rlock过后return前就unlock？
        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), *rid);
        break;
      }
    }
    (*iterator).operator++();
    if (predicate == nullptr || predicate->Evaluate(&t, &table_meta_data_->schema_).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
