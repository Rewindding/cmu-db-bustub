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
  Page *leaf_page = FindLeafPage(key, true);

  if (leaf_page == nullptr) {
    return false;
  }
  LeafPage *p = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType value{};
  if (p->Lookup(key, &value, comparator_)) {
    result->push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
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
  // LOG_DEBUG("insert,key:%lld,value:%s",key.ToString(),value.ToString().c_str());
  bool inserted = optimisticInsert(key,value, transaction);
  if(!inserted) {
    return concurrentInsert(key,value,transaction);
  }
  return true;
}
// assuming that no split needed, if not,return false immediately
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::optimisticInsert(const KeyType &key, const ValueType &value,Transaction* transaction) {
  if (IsEmpty()) {
    try {
      StartNewTree(key, value);
      return true;
    } catch (const char *msg) {
      // corner case : if the tree is empty and concurrent insert been called?
      if(strcmp(msg,"not empty tree") == 0) {
        // continue insert as a normal tree
        // return InsertIntoLeaf(key,value);
      } else {
        return false;
      }

    }
  }
  //0. find the leaf treePage
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* treePage = reinterpret_cast<BPlusTreePage*>(page->GetData());

  while(!treePage->IsLeafPage()){
    page->RLatch();
    page_id_t childPid = reinterpret_cast<InternalPage *>(treePage)->Lookup(key,comparator_);
    page->RUnlatch();

    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);

    Page* childPage = buffer_pool_manager_->FetchPage(childPid);
    // childPage->RLatch();
    BPlusTreePage* childTreePage = reinterpret_cast<BPlusTreePage*>(childPage->GetData());

    page = childPage;
    treePage = childTreePage;
  }
  // 在child page没有lock的时候释放掉parent page的lock，会不会有危险？
  //1. get the write latch of this leaf treePage
  page->WLatch();
  //2. judge if will split

  if(treePage->GetSize()==leaf_max_size_) {// should split! optimistic insert failed.
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  // don't need split
  reinterpret_cast<LeafPage *>(treePage)->Insert(key,value,comparator_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(treePage->GetPageId(),true);
  // LOG_DEBUG("opt insert,key:%lld\n",key.ToString());
  return true;
}


// question :what happened if i lock a page and unpin this page?
// get latch and insert to the whole tree
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::concurrentInsert(const KeyType &key, const ValueType &value, Transaction* transaction) {
  // this should not be a empty tree when this function been called
  start_new_tree_mutex.lock();
  Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // use a que to store ids of locked pages
  std::deque<Page* > wLockedPages;
  root_page->WLatch();

  wLockedPages.push_back(root_page);

  // special case : if this thread blocked then revoke and
  // turns out that the root page has been updated?
  // how to handle this? -- use mutex when read or write to that field

  // when a node is safe,release all its parents' latches
  // a node is safe when it will not split or merge or redistribute
  bool unlock_start_new_tree =false;
  TreePage* current_node = reinterpret_cast<TreePage*>(root_page->GetData());
  // unlock this only if root page is safe to insert
  if(current_node->IsSafeForInsert()) {
    start_new_tree_mutex.unlock();
  } else {
    unlock_start_new_tree = true;
  }
  while(!current_node->IsLeafPage()) {
    // get the child page
    // cast to internal node
    InternalPage* current_internal_node = static_cast<InternalPage *>(current_node);
    page_id_t child_page_id = current_internal_node->Lookup(key,comparator_);
    Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
    child_page->WLatch();

    // judge if this page need split
    TreePage* child_node = reinterpret_cast<TreePage*>(child_page->GetData());
    int max_page_size = child_node->IsLeafPage() ? leaf_max_size_:internal_max_size_;

    if(child_node->IsSafeForInsert()) {
      // this node is safe, release all the latches above (unlock and unpin)
      for(Page* p:wLockedPages) {
        p->WUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(),false);
      }
      wLockedPages.clear();
    }
    wLockedPages.push_back(child_page);

    current_node = child_node;
  }
  LeafPage* target_leaf_node = static_cast<LeafPage* >(current_node);
  target_leaf_node->Insert(key,value,comparator_);
  if(target_leaf_node->GetSize()>leaf_max_size_) {
    Split<LeafPage>(target_leaf_node);
  }
  // release all the latches
  // unpin all the pages here
  for(Page* p :wLockedPages) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(),true);
  }
  if(unlock_start_new_tree) {
    start_new_tree_mutex.unlock();
  }
//  LOG_DEBUG("con insert,key:%lld\n",key.ToString());
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  start_new_tree_mutex.lock();
  if(root_page_id_!=INVALID_PAGE_ID) {
    start_new_tree_mutex.unlock();
    throw "not empty tree";
  }
  page_id_t root_page_id;
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id);
  if (root_page == nullptr) {
    start_new_tree_mutex.unlock();
    throw "out of memory";
  }
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 此时root是leaf node？
  root_node->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // insert
  root_node->Insert(key, value, comparator_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(),true);
  start_new_tree_mutex.unlock();
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
  Page *leaf_page = FindLeafPage(key, true);
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // if duplicate key,return false
  if (leaf_node->Lookup(key, new ValueType{}, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    return false;
  }
  int size = leaf_node->Insert(key, value, comparator_);
  if (size > leaf_max_size_) {  // should split
    Split<LeafPage>(leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

// TODO split should not unpin pages except the new created page
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t pid;
  Page *new_right_page = buffer_pool_manager_->NewPage(&pid);
  if (!new_right_page) {
    throw "out of memory";
  }
  N *new_right_node = reinterpret_cast<N *>(new_right_page->GetData());
  new_right_node->Init(pid, node->GetParentPageId(),
                       new_right_node->IsLeafPage() ? leaf_max_size_ : internal_max_size_);
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
  if (size > internal_max_size_) {
    Split<InternalPage>(parent_node);
  }
  new_node->SetParentPageId(old_node->GetParentPageId());
  // buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
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
  if (IsEmpty()) {
    return;
  }
  auto leaf = FindLeafPage(key, true);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf->GetData());
  int beforeSize = leaf_node->GetSize();
  int size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (size < leaf_node->GetMinSize()) {
    // merge or redistribute
    if (CoalesceOrRedistribute<LeafPage>(leaf_node)) {
      // current page has been merged to left siblling and deleted, so don't need to unpin      
    } else {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
    }
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),leaf_node->GetSize()!=beforeSize);
  }
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
  if (node->IsRootPage()) {
    if (node->GetSize() > 1) {
      return false;
    }
    if (AdjustRoot(node)) {
      buffer_pool_manager_->DeletePage(node->GetPageId());
      return true;
    }
    return false;
  }
  if (node->GetSize() >= node->GetMinSize()) {  // current node is not underflow, return directly
    return false;
  }
  // get the parent node
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  page_id_t parent_index = parent_node->ValueIndex(node->GetPageId());
  // get the left and right sibling
  N *left_sibling = nullptr;
  N *right_sibling = nullptr;
  if (parent_index - 1 >= 0) {
    Page *left_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_index - 1));
    left_sibling = reinterpret_cast<N *>(left_page->GetData());
  }
  if (parent_index + 1 < parent_node->GetSize()) {
    Page *right_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_index + 1));
    right_sibling = reinterpret_cast<N *>(right_page->GetData());
  }
  // get the max page size
  int max_size_t = node->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
  N *sibling = left_sibling != nullptr ? left_sibling : right_sibling;
  // at least has one sibling
  assert(sibling != nullptr);
  if (sibling->GetSize() + node->GetSize() <= max_size_t) {
    // merge
    bool res = sibling == left_sibling;
    bool parent_deleted = false;
    if (res) { // the page to be merged is left page
      parent_deleted |= Coalesce<N>(&left_sibling, &node, &parent_node, parent_index);
      // coalesce will delete this page, so don't need to unpin 
      buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(),true);
      if(right_sibling != nullptr) {
        buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(),false);
      }
    } else {
      parent_deleted |= Coalesce<N>(&node, &right_sibling, &parent_node, parent_index);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      if(left_sibling != nullptr) {
        buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(),false);
      }
    }
    // unpin parent node
    if(!parent_deleted) { // if parent node has been deleted, don't need to unpin
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
    }
    return res;  // this page has been merged to it's left sibling and deleted ,return true;
  }
  // redistribute
  int index = (sibling == left_sibling) ? 1 : 0;
  Redistribute<N>(sibling, node, index);
  // unpin fetched pages
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
  if(left_sibling != nullptr) {
    buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(),index == 1);
  }
  if(right_sibling != nullptr) {
    buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(),index == 1);
  }
  return false;
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
  buffer_pool_manager_->DeletePage(node_t->GetPageId());  // delete here...
  parent_t->Remove(index);
  if ((parent_t->IsRootPage() && parent_t->GetSize() <= 1) ||
      (!parent_t->IsRootPage() && parent_t->GetSize() < parent_t->GetMinSize())) {
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
    UpdateRootPageId(false);
    return true;
  }
  if (!old_root_node->IsLeafPage() && root_size == 1) {
    InternalPage *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = root_node->ValueAt(0);
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(false);
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
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* btp = reinterpret_cast<BPlusTreePage* >(page->GetData());
  while(!btp->IsLeafPage()) {
    auto page_id = reinterpret_cast<InternalPage* >(btp)->ValueAt(0);
    Page* p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage*>(p->GetData()); 
  }
  // get the left most leaf page id to build the iterator
  buffer_pool_manager_->UnpinPage(btp->GetPageId(),false);
  return INDEXITERATOR_TYPE(btp->GetPageId(),0,buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* btp = reinterpret_cast<BPlusTreePage* >(page->GetData());
  while(!btp->IsLeafPage()) {
    auto page_id = reinterpret_cast<InternalPage* >(btp)->Lookup(key,comparator_);
    Page* p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage*>(p->GetData()); 
  }
  int pos = reinterpret_cast<LeafPage*>(btp)->KeyIndex(key,comparator_);
  buffer_pool_manager_->UnpinPage(btp->GetPageId(),false);
  return INDEXITERATOR_TYPE(btp->GetPageId(),pos,buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  // go to the right most leaf page
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* btp = reinterpret_cast<InternalPage* >(page->GetData());
  while(!btp->IsLeafPage()) {
    int rightMostIdx = btp->GetSize()-1;
    auto page_id = reinterpret_cast<InternalPage* >(btp)->ValueAt(rightMostIdx);
    buffer_pool_manager_->UnpinPage(btp->GetPageId(),false);
    Page* p = buffer_pool_manager_->FetchPage(page_id);
    btp = reinterpret_cast<BPlusTreePage* >(p->GetData());
  }
  int rightMostIdx = btp->GetSize();
  buffer_pool_manager_->UnpinPage(btp->GetPageId(),false);
  return INDEXITERATOR_TYPE(btp->GetPageId(),rightMostIdx,buffer_pool_manager_);
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
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // what's the type of root? leaf or non-leaf? what should i cast it to? can leaf node be interpreted as internal node?

  TreePage *p = reinterpret_cast<TreePage *>(root_page->GetData());
  while (!p->IsLeafPage()) {
    // cast to leaf page
    auto internalNode = static_cast<InternalPage *>(p);
    auto child_page_id = internalNode->Lookup(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    buffer_pool_manager_->UnpinPage(internalNode->GetPageId(),false);
    p = reinterpret_cast<TreePage *>(child_page->GetData());
  }
  return buffer_pool_manager_->FetchPage(p->GetPageId());
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
