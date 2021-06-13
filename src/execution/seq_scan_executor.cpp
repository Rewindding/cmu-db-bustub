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
    // form the output schema
    Tuple t = iterator.operator*().operator*();

    *tuple = t.KeyFromTuple(table_meta_data_->schema_, *plan_->OutputSchema(), output_tuple_key_attrs_);
    *rid = iterator.operator*()->GetRid();
    (*iterator).operator++();
    if (predicate == nullptr || predicate->Evaluate(&t, &table_meta_data_->schema_).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
