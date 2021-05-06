//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"
/**
 *
 * TO DO handle concurrency control how to use latch?
 */
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  std::lock_guard<std::mutex> lock(latch_);
  // LOG_INFO("FetchPageImpl(pid:%d)", page_id);
  if (page_table_.count(page_id) != 0U) {
    auto frame_id = page_table_[page_id];
    // assertion failed??
    if (page_id != (pages_ + frame_id)->page_id_) {
      LOG_DEBUG("FetchPageImpl,pid unequal,frame_id:%d,aim pid:%d,pid in ptable:%d", frame_id, page_id,
                (pages_ + frame_id)->page_id_);
    }
    replacer_->Pin(frame_id);
    (pages_ + frame_id)->pin_count_++;
    return pages_ + frame_id;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t stale_frame = -1;
  if (!free_list_.empty()) {
    stale_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&stale_frame);
  }
  if (stale_frame == -1) {
    return nullptr;
  }  // no free frames to be replaced? how to handle? what to return?
  // 2.     If R is dirty, write it back to the disk.
  Page &page = pages_[stale_frame];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page.GetPageId());
  page_table_[page_id] = stale_frame;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page.ResetMemory();
  page.page_id_ = page_id;
  (pages_ + stale_frame)->pin_count_++;  // this page is newly loaded to memory, pin_count must be 1
  disk_manager_->ReadPage(page_id, page.GetData());
  return pages_ + stale_frame;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  // LOG_INFO("UnpinPageImpl(pid:%d)", page_id);
  if (page_table_.count(page_id) == 0U) {
    LOG_DEBUG("UnpinPageImpl,pid %d not found in ptable", page_id);
    return false;
  }
  auto frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  if (page.GetPinCount() <= 0) {
    LOG_DEBUG("unpin a not pined page,pid:%d", page_id);
    return false;
  }
  if (--page.pin_count_ == 0) {
    // put it to lru re placer??
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page.is_dirty_ = true;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }
  if (frame_id == -1) {
    return nullptr;
  }  // no free frames to be replaced
  Page &P = pages_[frame_id];
  if (P.IsDirty()) {
    // Flush this page to disk
    disk_manager_->WritePage(P.GetPageId(), P.GetData());
    P.is_dirty_ = false;
  }
  *page_id = disk_manager_->AllocatePage();
  // LOG_INFO("NewPageImpl(),pid:%d", *page_id);
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_table_.erase(P.GetPageId());
  P.ResetMemory();
  // new page is dirty?? ,not write to disk yet???
  P.is_dirty_ = false;
  P.page_id_ = *page_id;
  page_table_[*page_id] = frame_id;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  (pages_ + frame_id)->pin_count_++;
  return pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }  // page not found!
  auto frame_id = page_table_[page_id];
  Page &P = pages_[frame_id];
  if (P.GetPinCount() != 0) {
    LOG_DEBUG("delete a pin page,pid:%d,pin_cnt:%d", P.GetPageId(), P.GetPinCount());
    return false;
  }
  // delete 掉的page，也要从lru replacer里面去除掉。。。
  replacer_->Pin(frame_id);
  disk_manager_->DeallocatePage(page_id);
  P.ResetMemory();
  P.page_id_ = INVALID_PAGE_ID;
  P.is_dirty_ = false;
  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (auto p : page_table_) {
    Page *page = pages_ + p.second;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
