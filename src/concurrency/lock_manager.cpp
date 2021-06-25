//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

class TransactionManager;
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // check这个rid是否已经被获取了exclusive lock
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
  }
  if (txn->GetState() == TransactionState::ABORTED) {  // repeatable read 的隔离级别才是2PL
    return false;
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  RIDLockState &ridLockState = rid_lock_state_[rid];
  txn->SetState(TransactionState::GROWING);
  if (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
    // 当前txn有可能已经获取exclusive lock了。
    if (ridLockState.writer_txn_id_ == txn->GetTransactionId()) {
      return true;
    }
    // add edge 只执行一次
    AddEdge(txn->GetTransactionId(), ridLockState.writer_txn_id_);
    // 插入时记录位置，方便后面删除
    // 存这个信息的意义是？
    auto iter = lock_table_[rid].request_queue_.insert(lock_table_[rid].request_queue_.end(),
                                                       LockRequest(txn->GetTransactionId(), LockMode::SHARED));
    std::condition_variable &cond = lock_table_[rid].cv_;
    while (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
      if (txn->GetState() == TransactionState::ABORTED) {
        RemoveEdge(txn->GetTransactionId(), ridLockState.writer_txn_id_);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
      cond.wait_for(latch, cycle_detection_interval);
    }
    // 获取锁过后应该把这个queue里面对应的transactionID删除
    lock_table_[rid].request_queue_.erase(iter);
    // wlatch一释放，所有的reader都可以同时获取rlatch
    RemoveEdge(txn->GetTransactionId(), ridLockState.writer_txn_id_);
  }
  ridLockState.reader_txn_ids_.insert(txn->GetTransactionId());
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  RIDLockState &ridLockState = rid_lock_state_[rid];
  std::deque<txn_id_t> waitedTxns;
  if (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
    AddEdge(txn->GetTransactionId(), ridLockState.writer_txn_id_);
    waitedTxns.push_back(ridLockState.writer_txn_id_);
  }
  if (!ridLockState.reader_txn_ids_.empty()) {
    // add all waiting edges
    for (txn_id_t tid : ridLockState.reader_txn_ids_) {
      AddEdge(txn->GetTransactionId(), tid);
      waitedTxns.push_back(tid);
    }
  }
  auto iter = lock_table_[rid].request_queue_.insert(lock_table_[rid].request_queue_.end(),
                                                     LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  std::condition_variable &cond = lock_table_[rid].cv_;
  while (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (txn_id_t tid : waitedTxns) {
        RemoveEdge(txn->GetTransactionId(), tid);
      }
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
    cond.wait_for(latch, cycle_detection_interval);
  }
  ridLockState.writer_txn_id_ = txn->GetTransactionId();
  while (!ridLockState.reader_txn_ids_.empty()) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (txn_id_t tid : waitedTxns) {
        RemoveEdge(txn->GetTransactionId(), tid);
      }
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
    cond.wait_for(latch, cycle_detection_interval);
  }
  lock_table_[rid].request_queue_.erase(iter);
  for (txn_id_t tid : waitedTxns) {
    RemoveEdge(txn->GetTransactionId(), tid);
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  RIDLockState &ridLockState = rid_lock_state_[rid];
  // first,release the shared latch
  ridLockState.reader_txn_ids_.erase(txn->GetTransactionId());
  txn->GetSharedLockSet()->erase(rid);
  // then wait until get the wlatch
  std::condition_variable &cond = lock_table_[rid].cv_;
  std::deque<txn_id_t> waitedTxns;
  if (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
    AddEdge(txn->GetTransactionId(), ridLockState.writer_txn_id_);
    waitedTxns.push_back(ridLockState.writer_txn_id_);
  }
  while (ridLockState.writer_txn_id_ != INVALID_TXN_ID) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (txn_id_t txnId : waitedTxns) {
        RemoveEdge(txn->GetTransactionId(), txnId);
      }
      return false;
    }
    cond.wait_for(latch, cycle_detection_interval);
  }
  ridLockState.writer_txn_id_ = txn->GetTransactionId();
  for (const txn_id_t &txnId : ridLockState.reader_txn_ids_) {
    AddEdge(txn->GetTransactionId(), txnId);
    waitedTxns.push_back(txnId);
  }
  while (!ridLockState.reader_txn_ids_.empty()) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (txn_id_t txnId : waitedTxns) {
        RemoveEdge(txn->GetTransactionId(), txnId);
      }
      return false;
    }
    cond.wait_for(latch, cycle_detection_interval);
  }
  for (txn_id_t txnId : waitedTxns) {
    RemoveEdge(txn->GetTransactionId(), txnId);
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  RIDLockState &ridLockState = rid_lock_state_[rid];
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  std::condition_variable &cond = lock_table_[rid].cv_;
  if (ridLockState.writer_txn_id_ == txn->GetTransactionId()) {
    ridLockState.writer_txn_id_ = INVALID_TXN_ID;
    assert(ridLockState.reader_txn_ids_.empty());
    // rid_lock_state_.erase(rid);
    cond.notify_all();
  } else {
    ridLockState.reader_txn_ids_.erase(txn->GetTransactionId());
    if (ridLockState.reader_txn_ids_.empty()) {
      // assert(ridLockState.writer_txn_id_ == INVALID_TXN_ID); 这个assertion是错的！
      // rid_lock_state_.erase(rid);
      cond.notify_one();
    }
  }
  // TODO(rewindding) 如何删除lock_table_和rid_lock_state_里面的entry？
  //  if(ridLockState.writer_txn_id_ == INVALID_TXN_ID && ridLockState.reader_txn_ids_.empty()) {
  //    rid_lock_state_.erase(rid);
  //    lock_table_.erase(rid);
  //  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_edges_.insert({t1, t2}); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_edges_.erase({t1, t2}); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // build a fly graph
  waits_for_.clear();
  cycle_start_ = INVALID_TXN_ID;
  target_cycle_txn_ = INVALID_TXN_ID;
  std::set<txn_id_t> vertices;
  for (const auto &edge : waits_for_edges_) {
    txn_id_t v1 = edge.first;
    txn_id_t v2 = edge.second;
    waits_for_[v1].push_back(v2);
    vertices.insert(v1);
    vertices.insert(v2);
  }
  for (auto &adjs : waits_for_) {
    std::sort(adjs.second.begin(), adjs.second.end());
  }
  vertex_states_.clear();
  // set里面取出来的元素是有序的吗？
  for (txn_id_t v : vertices) {
    if (vertex_states_[v] == 0) {
      vertex_states_.clear();
      Dfs(v);
    }
    if (target_cycle_txn_ != INVALID_TXN_ID) {
      waits_for_.clear();
      *txn_id = target_cycle_txn_;
      LOG_INFO("cycle detected,abort txn:%d", *txn_id);
      return true;
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  return {waits_for_edges_.begin(), waits_for_edges_.end()};
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // build Graph here?
      txn_id_t aborted_txn = INVALID_TXN_ID;
      if (HasCycle(&aborted_txn)) {
        Transaction *txn = TransactionManager::GetTransaction(aborted_txn);
        txn->SetState(TransactionState::ABORTED);
      }
      // remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

bool LockManager::Dfs(txn_id_t v) {
  vertex_states_[v] = 1;
  for (txn_id_t adj : waits_for_[v]) {
    if (vertex_states_[adj] > 0) {  // first cycle detected,find the max txn in the cycle
      // 记录下当前的节点txn_id，作为cycle start，一直return回去
      cycle_start_ = adj;
      target_cycle_txn_ = std::max(target_cycle_txn_, v);
      return true;
    }
    bool inCycle = Dfs(adj);
    if (inCycle) {
      target_cycle_txn_ = std::max(target_cycle_txn_, v);
      // tell the prev node whether it's in this cycle
      return v != cycle_start_;
    }
  }
  return false;
}

}  // namespace bustub
