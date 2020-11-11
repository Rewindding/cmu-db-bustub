//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

/**
 * maybe some concurrency control issue not handled
 * 2020年9月30日 test case passed
 *
 */
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : _capacity(num_pages) {
  front = new Linked_list();
  rear = new Linked_list();
  front->right = rear;
  rear->left = front;
}

LRUReplacer::~LRUReplacer() {
  delete front;
  delete rear;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // how to konw the frame's pin counter is 0 or not??? track by it self
  // choose the list used page_frame_id and write it to the output parame
  std::lock_guard<std::mutex> guard(latch_);
  auto least_used = front->right;
  if (least_used == rear) {
    return false;
  }
  *frame_id = least_used->data;
  // delete the node
  RemoveNode(least_used);
  map.erase(least_used->data);
  delete least_used;
  return true;
}
// will be called when access a frame?
// It should remove the frame containing the pinned page from the LRUReplacer.
// because pined page cann't be replaced!
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (map.count(frame_id) == 0U) {
    return;  // frame not in the replacer
  }
  auto pined_frame = map[frame_id];
  // remove the node from list
  RemoveNode(pined_frame);
  map.erase(frame_id);
  delete pined_frame;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // capacity full
  if (map.size() == _capacity) {
    return;
  }
  Linked_list *node = nullptr;
  if (map.count(frame_id) != 0U) {
    return;  // already unpined!
  }
  node = new Linked_list(frame_id);
  PushBack(node);
  map[frame_id] = node;
}

// return the number of unpined frames?
// this method returns the number of frames that are currently in the LRUReplacer.
// the lRU replacer don't store pined pages
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return map.size();
}

}  // namespace bustub
