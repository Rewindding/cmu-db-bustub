//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <execution/expressions/column_value_expression.h>
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(nullptr) {
  uint32_t valueIdx = 0;
  // 问题:如何找到input schema 和output schema之间的映射关系？
  for (const AbstractExpression *groupBy : plan_->GetGroupBys()) {
    const ColumnValueExpression *columnExp = dynamic_cast<const ColumnValueExpression *>(groupBy);
    auto colIdx = columnExp->GetColIdx();
    col_to_value_map_[colIdx] = valueIdx++;
  }

  for (auto agg : plan_->GetAggregates()) {
    const ColumnValueExpression *columnExp = dynamic_cast<const ColumnValueExpression *>(agg);
    auto colIdx = columnExp->GetColIdx();
    // 同一个column可能会有重复的column aggregation  比如sum(colA),avg(colA)...
    while (col_to_value_map_.count(colIdx) == 1U) {
      colIdx++;
    }
    col_to_value_map_[colIdx] = valueIdx++;
  }
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple t;
  RID r;
  // insert every tuple to the aht
  // aggregation must iterator every tuple
  // TODO(verify) 确定：input schema 和output schema 的column是一致的
  const Schema *inputSchema = child_->GetOutputSchema();

  while (child_->Next(&t, &r)) {
    std::vector<Value> aggKeys;
    std::vector<Value> aggValues;
    for (const AbstractExpression *groupBy : plan_->GetGroupBys()) {
      const ColumnValueExpression *columnExp = dynamic_cast<const ColumnValueExpression *>(groupBy);
      Value v = columnExp->Evaluate(&t, inputSchema);
      aggKeys.push_back(v);
    }
    for (auto agg : plan_->GetAggregates()) {
      const ColumnValueExpression *columnExp = dynamic_cast<const ColumnValueExpression *>(agg);
      Value v = columnExp->Evaluate(&t, inputSchema);
      aggValues.push_back(v);
    }
    aht_.InsertCombine({aggKeys}, {aggValues});
  }
  if (aht_iterator_ == nullptr) {
    aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_.Begin());
  }
  while (*aht_iterator_ != aht_.End()) {
    auto keys = aht_iterator_->Key();
    auto values = aht_iterator_->Val();
    aht_iterator_->operator++();
    auto having_predict = plan_->GetHaving();
    bool having = true;
    if (having_predict != nullptr) {
      having = having_predict->EvaluateAggregate(keys.group_bys_, values.aggregates_).GetAs<bool>();
    }
    if (!having) {
      continue;
    }
    // generate result tuple
    std::vector<Value> tupleValue{keys.group_bys_};
    // tuple value布局 ： groupBuy values|aggregation Values
    tupleValue.insert(tupleValue.end(), values.aggregates_.begin(), values.aggregates_.end());
    // 要有办法知道value对应的是哪个column
    std::vector<Value> schemaTupleValue{};
    // 直接维持input schema的原始顺序
    for (uint32_t colIdx = 0; colIdx < plan_->OutputSchema()->GetColumnCount(); ++colIdx) {
      // 找到这个colIndex对应的value index
      uint32_t vIdx = col_to_value_map_[colIdx];
      schemaTupleValue.push_back(tupleValue[vIdx]);
    }
    // 这里schema tuple values 的顺序要和schema里面的column一致
    *tuple = Tuple(schemaTupleValue, plan_->OutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
