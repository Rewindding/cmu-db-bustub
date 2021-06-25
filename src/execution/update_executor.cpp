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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t tableOid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(tableOid);
}

void UpdateExecutor::Init() {}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid) &&
      exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
    } else {
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
    }
  }
  Tuple newTuple = GenerateUpdatedTuple(*tuple);
  bool updated = table_info_->table_->UpdateTuple(newTuple, *rid, exec_ctx_->GetTransaction());
  if (!updated) {
    return updated;
  }
  if (exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid) &&
      exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), *rid);
  }
  // update all indexes
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (IndexInfo *index : indexes) {
    // how to check if this index need not to update,e.g the key does not change,key attibutes
    // index->index_->GetKeyAttrs(); if key attributes x in plan_->GetUpdateAttr(), the index should be updated
    // 这里delete entry是基于tuple的，假如有重复的key，如何能确保删除掉这个tuple对应的那条记录吗？
    Tuple oldIndexKey = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    Tuple newIndexKey = newTuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    auto iwr = IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, newIndexKey, index->index_oid_,
                                exec_ctx_->GetCatalog());
    iwr.old_tuple_ = oldIndexKey;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(iwr);
    // 是否应该先获取要更新的key对应的rid，看是否与待更新的tuple rid对应？
    index->index_->DeleteEntry(oldIndexKey, *rid, exec_ctx_->GetTransaction());
    index->index_->InsertEntry(newIndexKey, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub
