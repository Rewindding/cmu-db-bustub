//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {
// double linked list structure
struct Linked_list {
  frame_id_t data = 0;
  Linked_list *left = nullptr, *right = nullptr;
  Linked_list() = default;
  explicit Linked_list(frame_id_t p) { data = p; }
};
/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!

  // capacity
  size_t _capacity = 0;

  Linked_list *front = nullptr, *rear = nullptr;

  std::unordered_map<frame_id_t, Linked_list *> map;
  // remove from list but not delete
  void RemoveNode(Linked_list *node) {
    node->left->right = node->right;
    node->right->left = node->left;
  }
  // push a node to the back
  void PushBack(Linked_list *node) {
    node->left = rear->left;
    rear->left->right = node;
    node->right = rear;
    rear->left = node;
  }
};

}  // namespace bustub
