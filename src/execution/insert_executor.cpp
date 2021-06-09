//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <execution/executor_factory.h>
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), raw_value_index_(0), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {}

// 这里传入tuple指针的意义是？
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // insert & update indexes
  table_oid_t tableOid = plan_->TableOid();
  TableMetadata *tableMetadata = exec_ctx_->GetCatalog()->GetTable(tableOid);

  // raw value insert or child node insert?
  if (plan_->IsRawInsert()) {
    if (raw_value_index_ >= plan_->RawValues().size()) {
      return false;
    }
    std::vector<Value> value = plan_->RawValuesAt(raw_value_index_);
    raw_value_index_++;
    // how to use value to generate a tuple?
    *tuple = Tuple(value, &tableMetadata->schema_);
  } else {
    if (!child_executor_->Next(tuple, rid)) {  // no tuple to insert
      return false;
    }
  }
  bool inserted = tableMetadata->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  if (!inserted) {
    return false;
  }
  // update all index relative to the key
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(tableMetadata->name_);
  for (IndexInfo *index : indexes) {
    // 这里的tuple要根据index schema转换
    Tuple indexKey = tuple->KeyFromTuple(tableMetadata->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(indexKey, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
