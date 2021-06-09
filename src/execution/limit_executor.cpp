//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), skipped_tuple_(0) {}

void LimitExecutor::Init() {}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  size_t cnt = plan_->GetLimit() * plan_->GetOffset();
  while (skipped_tuple_ < cnt) {
    bool hasNext = child_executor_->Next(tuple, rid);
    if (!hasNext) {
      return false;
    }
    skipped_tuple_++;
  }
  return child_executor_->Next(tuple, rid);
}

}  // namespace bustub
