//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      outer_next_(false) {}

void NestedLoopJoinExecutor::Init() {}

// stupid nested loop join
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *leftSchema = plan_->GetLeftPlan()->OutputSchema();
  const Schema *rightSchema = plan_->GetRightPlan()->OutputSchema();

  for (; outer_next_ || left_executor_->Next(&last_outer_tuple_, &last_outer_rid_);) {
    outer_next_ = true;
    Tuple rightTuple;
    RID rightRid;

    for (; right_executor_->Next(&rightTuple, &rightRid);) {
      bool match =
          plan_->Predicate()->EvaluateJoin(&last_outer_tuple_, leftSchema, &rightTuple, rightSchema).GetAs<bool>();
      if (match) {
        // how to generate the result tuple?
        // last_outer_tuple_.GetValue()
        // how to get value vector from tuple?
        std::vector<Value> values;
        for (uint32_t i = 0; i < leftSchema->GetColumnCount(); ++i) {
          auto value = last_outer_tuple_.GetValue(leftSchema, i);
          values.push_back(value);
        }
        for (uint32_t i = 0; i < rightSchema->GetColumnCount(); ++i) {
          auto value = rightTuple.GetValue(rightSchema, i);
          values.push_back(value);
        }
        // plan_->OutputSchema()->
        *tuple = Tuple(values, plan_->OutputSchema());
        return true;
      }
    }
    // 有些executor 没有实现init方法，怎么重复遍历他们的tuple？
    // 所有的executor都实现init方法?
    // 第一次遍历的时候应该把tuple暂存起来？
    right_executor_->Init();
    outer_next_ = false;
  }
  return false;
}

}  // namespace bustub
