//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 * TODO read lock
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key);

  if (leaf_page == nullptr) {
    return false;
  }
  LeafPage *p = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType value{};
  if (p->Lookup(key, &value, comparator_)) {
    result->push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  int inserted = optimisticInsert(key, value, transaction);
  if (inserted == -1) {  // duplicate key
    return false;
  }
  if (inserted == 1) {
    return true;
  }
  return concurrentInsert(key, value, transaction);
}
// assuming that no split needed, if not,return false immediately
INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::optimisticInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  std::deque<Page *> rLatchPages;
  dummy_page.RLatch();
  rLatchPages.push_back(&dummy_page);
  if (IsEmpty()) {  // 这种情况属于需要修改root_page_id，所以应该直接失败，交给concurrentInsert处理
    dummy_page.RUnlatch();
    return 0;
  }
  // 0. find the leaf treePage
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (treePage->IsLeafPage()) {
    page->WLatch();
  } else {
    page->RLatch();
    rLatchPages.push_back(page);
  }
  while (!treePage->IsLeafPage()) {
    page_id_t childPid = static_cast<InternalPage *>(treePage)->Lookup(key, comparator_);
    Page *childPage = buffer_pool_manager_->FetchPage(childPid);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childPage->GetData());

    // 能不能在这里，先判断它是不是leaf node,如果是就直接获取wlatch.判断的时候就读取了page数据...读取就必须获取rlatch?
    // 不影响
    if (childTreePage->IsLeafPage()) {
      childPage->WLatch();
    } else {
      childPage->RLatch();
      rLatchPages.push_back(childPage);
    }

    if (childTreePage->IsSafeForInsert()) {  // can release all latches above
      // unlatch and unpin
      // TO DO(rewindding): figure out unlock order.. top-down or down-top
      while (rLatchPages.size() > 1) {  // 不能unlach此时的childPage
        Page *p = rLatchPages.front();
        rLatchPages.pop_front();
        p->RUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      }
    }
    page = childPage;
    treePage = childTreePage;
  }
  // 此时一定获取了target page的WLatch.
  if (!treePage->IsSafeForInsert()) {  // should split! optimistic insert failed.
    // unlatch and unpin all before return
    while (!rLatchPages.empty()) {
      Page *p = rLatchPages.front();
      rLatchPages.pop_front();
      p->RUnlatch();
      buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return 0;
  }
  // don't need split
  int size = reinterpret_cast<LeafPage *>(treePage)->GetSize();
  int insertSize = reinterpret_cast<LeafPage *>(treePage)->Insert(key, value, comparator_);

  // unlatch and unpin all before return
  while (!rLatchPages.empty()) {
    Page *p = rLatchPages.front();
    rLatchPages.pop_front();
    p->RUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  if (size + 1 != insertSize) {
    return -1;  // duplicate key insert failed.
  }
  // LOG_DEBUG("opt insert");
  return 1;
}

// question :what happened if i lock a page and unpin this page?
// get latch and insert to the whole tree
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::concurrentInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // this should not be a empty tree when this function been called
  std::deque<Page *> wLatchPages;
  dummy_page.WLatch();
  wLatchPages.push_back(&dummy_page);
  if (IsEmpty()) {
    try {
      StartNewTree(key, value);
    } catch (char *exception) {
      dummy_page.WUnlatch();
      return false;
    }
    // no need to unpin.
    dummy_page.WUnlatch();
    return true;
  }
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  root_page->WLatch();
  wLatchPages.push_back(root_page);

  TreePage *treePage = reinterpret_cast<TreePage *>(root_page->GetData());
  // unlock this only if root page is safe to insert

  while (!treePage->IsLeafPage()) {
    // get the child page
    // cast to internal node
    page_id_t child_page_id = static_cast<InternalPage *>(treePage)->Lookup(key, comparator_);
    Page *childPage = buffer_pool_manager_->FetchPage(child_page_id);
    childPage->WLatch();
    TreePage *child_node = reinterpret_cast<TreePage *>(childPage->GetData());
    if (child_node->IsSafeForInsert()) {
      // this node is safe, release all the latches above (unlock and unpin)
      while (!wLatchPages.empty()) {
        Page *p = wLatchPages.front();
        wLatchPages.pop_front();
        p->WUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      }
    }
    wLatchPages.push_back(childPage);
    treePage = child_node;
  }
  LeafPage *targetLeafTreePage = static_cast<LeafPage *>(treePage);
  int pageSize = targetLeafTreePage->GetSize();
  int insertSize = targetLeafTreePage->Insert(key, value, comparator_);
  if (targetLeafTreePage->GetSize() >= targetLeafTreePage->GetMaxSize()) {  // leaf node should split
    Split<LeafPage>(targetLeafTreePage);
  }
  // release all the latches
  // unpin all the pages here
  while (!wLatchPages.empty()) {
    Page *p = wLatchPages.front();
    wLatchPages.pop_front();
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(), p->GetPageId() == targetLeafTreePage->GetPageId());
  }
  // LOG_DEBUG("con insert,key:%lld\n",key.ToString());
  return pageSize + 1 == insertSize;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id);
  if (root_page == nullptr) {
    throw "out of memory";
  }
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 此时root是leaf node？
  root_node->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // insert
  root_node->Insert(key, value, comparator_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key);
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // if duplicate key,return false
  if (leaf_node->Lookup(key, new ValueType{}, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  int size = leaf_node->Insert(key, value, comparator_);
  if (size >= leaf_node->GetMaxSize()) {  // should split
    Split<LeafPage>(leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

// TODO(lint) split should not unpin pages except the new created page
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t pid;
  Page *new_right_page = buffer_pool_manager_->NewPage(&pid);
  if (!new_right_page) {
    throw "out of memory";
  }
  N *new_right_node = reinterpret_cast<N *>(new_right_page->GetData());
  new_right_node->Init(pid, node->GetParentPageId(), node->IsLeafPage() ? leaf_max_size_ : internal_max_size_);
  node->MoveHalfTo(new_right_node, buffer_pool_manager_);
  InsertIntoParent(node, new_right_node->KeyAt(0), new_right_node);
  buffer_pool_manager_->UnpinPage(new_right_page->GetPageId(), true);
  return new_right_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {  // generate new root
    page_id_t page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&page_id);
    if (new_root_page == nullptr) {
      return;
    }
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    // update root info
    root_page_id_ = new_root->GetPageId();
    UpdateRootPageId(0);
    return;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(old_node->GetParentPageId());
  if (size > internal_max_size_) {
    Split<InternalPage>(parent_node);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  int delRes = optimisticDelete(key, transaction);
  if (delRes == -1 || delRes == 1) {
    return;
  }
  concurrentDelete(key, transaction);
}
INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::optimisticDelete(const KeyType &key, Transaction *transaction) {
  auto rLatchPages = transaction->GetPageSet();
  dummy_page.RLatch();
  rLatchPages->push_back(&dummy_page);
  if (IsEmpty()) {
    return -1;  // key not exist
  }
  // TO DO 找到插入删除位置 获取锁过程类似，应该封装一个函数公用？
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (treePage->IsLeafPage()) {
    page->WLatch();
  } else {
    page->RLatch();
    rLatchPages->push_back(page);
  }
  while (!treePage->IsLeafPage()) {
    Page *childPage = buffer_pool_manager_->FetchPage(static_cast<InternalPage *>(treePage)->Lookup(key, comparator_));
    TreePage *childTreePage = reinterpret_cast<TreePage *>(page->GetData());

    if (childTreePage->IsLeafPage()) {
      childPage->WLatch();
    } else {
      childPage->RLatch();
      rLatchPages->push_back(childPage);
    }
    if (childTreePage->IsSafeForDelete()) {
      while (rLatchPages->size() > 1) {
        Page *p = rLatchPages->front();
        rLatchPages->pop_front();
        p->RUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      }
    }
    page = childPage;
    treePage = childTreePage;
  }
  LeafPage *targetLeaf = static_cast<LeafPage *>(treePage);
  if (!targetLeaf->IsSafeForDelete()) {
    while (!rLatchPages->empty()) {
      Page *p = rLatchPages->front();
      rLatchPages->pop_front();
      p->RUnlatch();
      buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return 0;
  }
  int size = targetLeaf->GetSize();
  int sizeAfterDel = targetLeaf->RemoveAndDeleteRecord(key, comparator_);

  while (!rLatchPages->empty()) {
    Page *p = rLatchPages->front();
    rLatchPages->pop_front();
    p->RUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  if (size == sizeAfterDel) {
    return -1;  // key not exist;
  }
  return 1;
}
INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::concurrentDelete(const KeyType &key, Transaction *transaction) {
  // std::deque<Page*> wLatchPages;
  auto wLatchPages = transaction->GetPageSet();
  dummy_page.WLatch();
  wLatchPages->push_back(&dummy_page);
  if (IsEmpty()) {
    dummy_page.WUnlatch();
    wLatchPages->clear();
    return -1;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  TreePage *treePage = reinterpret_cast<TreePage *>(page->GetData());
  while (!treePage->IsLeafPage()) {
    Page *childPage = buffer_pool_manager_->FetchPage(static_cast<InternalPage *>(treePage)->Lookup(key, comparator_));
    TreePage *childTreePage = reinterpret_cast<TreePage *>(childPage->GetData());
    childPage->WLatch();
    wLatchPages->push_back(childPage);
    if (childTreePage->IsSafeForDelete()) {
      while (wLatchPages->size() > 1) {
        Page *p = wLatchPages->front();
        wLatchPages->pop_front();
        p->WUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      }
    }
    treePage = childTreePage;
    page = childPage;
  }
  LeafPage *targetLeaf = static_cast<LeafPage *>(treePage);
  targetLeaf->RemoveAndDeleteRecord(key, comparator_);

  if (targetLeaf->GetSize() < targetLeaf->GetMinSize()) {
    CoalesceOrRedistribute<LeafPage>(targetLeaf, transaction);
  }
  while (!wLatchPages->empty()) {
    Page *p = wLatchPages->front();
    wLatchPages->pop_front();
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(), targetLeaf->GetPageId() == p->GetPageId());
  }
  for (page_id_t deletedPid : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(deletedPid);
  }
  transaction->GetDeletedPageSet()->clear();
  return 1;
}
/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * return true means current node need to be merged to it's left sibling
 * means this node should be deleted,but where should excute this delete ?
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->GetSize() >= node->GetMinSize()) {  // param check.
    return false;
  }
  if (node->IsRootPage()) {
    if (AdjustRoot(node)) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return false;
  }
  bool res = false;
  // get the parent node
  // 在调用之前，一定已经获取parent的wlatch了，所以只需要在return后unpin即可
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  page_id_t parent_index = parent_node->ValueIndex(node->GetPageId());
  // get the left and right sibling
  int pageMaxSize = node->GetMaxSize();
  if (node->IsLeafPage()) {
    pageMaxSize -= 1;
  }
  if (parent_index - 1 >= 0) {
    Page *left_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_index - 1));
    left_page->WLatch();
    N *left_sibling = reinterpret_cast<N *>(left_page->GetData());
    if (left_sibling->GetSize() + node->GetSize() <= pageMaxSize) {  // merge node to left sibling
      res = true;
      Coalesce<N>(&left_sibling, &node, &parent_node, parent_index, transaction);
    } else {  // redistribute
      Redistribute(left_sibling, node, 0);
    }
    left_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
  } else if (parent_index + 1 < parent_node->GetSize()) {
    Page *right_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_index + 1));
    right_page->WLatch();
    N *right_sibling = reinterpret_cast<N *>(right_page->GetData());
    if (right_sibling->GetSize() + node->GetSize() <= pageMaxSize) {  // merge to right sibling
      Coalesce<N>(&node, &right_sibling, &parent_node, parent_index, transaction);
    } else {  // redistribute
      Redistribute(node, right_sibling, 0);
    }
    right_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
  }
  // unpin parent pages
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  return res;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 * move all k-v from node to it's neighbor
 * node is always merged from right to it's left sibling
 * return true means parent node underflow and xxxxx...
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  auto neighbor_t = *neighbor_node;
  auto node_t = *node;
  auto parent_t = *parent;
  node_t->MoveAllTo(neighbor_t, index, buffer_pool_manager_);
  transaction->AddIntoDeletedPageSet(node_t->GetPageId());
  parent_t->Remove(index);
  if (parent_t->GetSize() < parent_t->GetMinSize()) {
    // deal with parent size,return coalesceOrRedistribute()...
    return CoalesceOrRedistribute<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(parent_t);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  int root_size = old_root_node->GetSize();
  if (old_root_node->IsLeafPage() && root_size == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  if (!old_root_node->IsLeafPage() && root_size == 1) {
    root_page_id_ = static_cast<InternalPage *>(old_root_node)->ValueAt(0);
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *btp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!btp->IsLeafPage()) {
    auto page_id = reinterpret_cast<InternalPage *>(btp)->ValueAt(0);
    Page *p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  // get the left most leaf page id to build the iterator
  buffer_pool_manager_->UnpinPage(btp->GetPageId(), false);
  return INDEXITERATOR_TYPE(btp->GetPageId(), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
// TO DO index iterator read latch
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *btp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!btp->IsLeafPage()) {
    auto page_id = reinterpret_cast<InternalPage *>(btp)->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(btp->GetPageId(), false);
    Page *p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  int pos = reinterpret_cast<LeafPage *>(btp)->KeyIndex(key, comparator_);
  buffer_pool_manager_->UnpinPage(btp->GetPageId(), false);
  return INDEXITERATOR_TYPE(btp->GetPageId(), pos, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  // go to the right most leaf page
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *btp = reinterpret_cast<InternalPage *>(page->GetData());
  while (!btp->IsLeafPage()) {
    int rightMostIdx = btp->GetSize() - 1;
    auto page_id = reinterpret_cast<InternalPage *>(btp)->ValueAt(rightMostIdx);
    buffer_pool_manager_->UnpinPage(btp->GetPageId(), false);
    Page *p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  int rightMostIdx = btp->GetSize();
  buffer_pool_manager_->UnpinPage(btp->GetPageId(), false);
  return INDEXITERATOR_TYPE(btp->GetPageId(), rightMostIdx, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * i think it's proper to always return the left most leaf node
 * TODO optimistic insert still need read latch? 2021.4.29
 * TODO
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  TreePage *p = reinterpret_cast<TreePage *>(page->GetData());

  while (!p->IsLeafPage()) {
    // cast to leaf page
    // auto internalNode = static_cast<InternalPage *>(p);
    page_id_t childPid;
    if (leftMost) {
      childPid = static_cast<InternalPage *>(p)->ValueAt(0);
    } else {
      childPid = static_cast<InternalPage *>(p)->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);

    page = buffer_pool_manager_->FetchPage(childPid);
    p = reinterpret_cast<TreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
