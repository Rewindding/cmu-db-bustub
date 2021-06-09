//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_iterator_(nullptr) {}

void IndexScanExecutor::Init() { index_iterator_.reset(nullptr); }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  index_oid_t indexOid = plan_->GetIndexOid();
  IndexInfo *indexInfo = exec_ctx_->GetCatalog()->GetIndex(indexOid);
  // how to retrive every rid from index??
  // if the index type is not sure? how to handle then?
  BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *index =
      static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(indexInfo->index_.get());
  if (index_iterator_ == nullptr) {
    index_iterator_ =
        std::make_unique<IndexIterator<GenericKey<8>, RID, GenericComparator<8>>>(index->GetBeginIterator());
  }
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> endIterator = index->GetEndIterator();
  TableMetadata *tableMetadata = exec_ctx_->GetCatalog()->GetTable(indexInfo->table_name_);
  while (index_iterator_.operator*() != endIterator) {
    *rid = index_iterator_.operator*().operator*().second;
    index_iterator_.operator*().operator++();
    rid->GetPageId();
    bool ok;
    bool predicated = false;
    ok = tableMetadata->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    if (ok) {
      predicated = plan_->GetPredicate()->Evaluate(tuple, &tableMetadata->schema_).GetAs<bool>();
    }
    if (predicated) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
