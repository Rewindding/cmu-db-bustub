//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_=page_id;
  parent_id_=parent_id;
  max_size_=max_size;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first=key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i=0;i<GetSize();++i){
    if(array[i].second==value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  //TO DO optimize to binary search!
  for(int i=1;i<GetSize();++i){
    if(array[i].first>key) return array[i-1].second;
  }
  return array[GetSize()-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  array[0].second=old_value;
  array[1].first=new_key;
  array[1].second=new_value;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  //0.find insert position
  int pos=-1;
  for(int p=0;p<GetSize();++p){
    if(array[p].second==old_value) {
      pos=p+1;
      break;
    }
  }
  assert(pos!=-1);
  //1.move array and insert
  for(int i=GetSize()-1;i>=pos;--i){
    array[i+1].first=array[i].frist;
    array[i+1].second=array[i].second;
  }
  array[pos].first=new_key;
  array[pos].second=new_value;
  //2.update size
  SetSize(GetSize()+1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  //copy the right half or left half? right half
  int s=(GetSize()+1)/2;
  if(s<=0) return;
  // remember every B tree page is stored on disk!
  // don't forget update their parent_page_id!
  for(int i=GetSize()-s;i<GetSize();++i){
    Page* child_page=buffer_pool_manager->FetchPage(array[i].second);
    auto child_page_node = reinterpret_cast<BPlusTreePage* >(child_page->GetData());//cast to in mem object
    child_page_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_node->GetPageId(),true);
  }
  //update size
  IncreaseSize(-s);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size,
                                                  BufferPoolManager *buffer_pool_manager) {
  //wtf??
  memcpy(array, items, size*sizeof(MappingType));
  IncreaseSize(size-1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for(int i=index;i<GetSize()-1;++i){
    array[i].first=array[i+1].first;
    array[i].second=array[i+1].second;
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto res=array[1].second;
  //return the only child??? but it has at least two child! which should return then?
  SetSize(0);
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent,
                                               BufferPoolManager *buffer_pool_manager) {
  //how to update relevent key & value parin in its parent page?
  auto parnet_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parnet_page->GetData());
  SetKeyAt(0,parent_node->KeyAt(index_in_parent));
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(),false);
  CopyAllFrom(array,GetSize(),buffer_pool_manager);
  //update child page's parent_page_id property
  for(int i=0;i<GetSize();++i){
    auto page=buffer_pool_manager->FetchPage(array[i].second);
    auto page_node=reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    page_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_node->GetPageId(),true);
  }
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int current_size = GetSize();
    memcpy(array+current_size, items, (size_t)size*sizeof(MappingType));
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {
  //why should got to the parent page to fetch the key?
  Page* parent_page = buffer_pool_manager->FetchPage(GetPageId());
  BPlusTreeInternalPage* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  auto key=parent_node->ValueIndex(GetPageId());
  MappingType pair={key,array[0].second};
  recipient->CopyLastFrom(pair,buffer_pool_manager);
  parent_node->SetKeyAt(key,array[1].first);
  Remove(0);
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(),true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()]=pair;
  IncreaseSize(1);
  Page* child_page = buffer_pool_manager->FetchPage(pair.second);
  auto child_node = reinterpret_cast<BPlusTreeInternalPage*>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_node->GetPageId(),true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, int parent_index,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array[GetSize()-1],parent_index,buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, int parent_index,
                                                   BufferPoolManager *buffer_pool_manager) {
  Page* parent_page=buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  array[0].first=parent_node->KeyAt(parent_index);
  memmove(array+1,array,GetSize()*sizeof(pair));
  array[0]=pair;
  parent_node->SetKeyAt(parent_index,pair.first);

  Page* child_page=buffer_pool_manager->FetchPage(pair.second);
  BPlusTreeInternalPage* child_node=reinterpret_cast<BPlusTreeInternalPage*>(child_page->GetData());
  child_node->SetPageId(GetPageId());

  buffer_pool_manager->UnpinPage(GetParentPageId(),true);
  buffer_pool_manager->UnpinPage(child_node->GetPageId(),true);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
