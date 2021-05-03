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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (array[i].second == value) {
      return i;
    }
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
  // TO DO optimize to binary search!
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) > 0) {
      return array[i - 1].second;
    }
  }
  return array[GetSize() - 1].second;
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
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 0.find insert position
  int pos = ValueIndex(old_value);
  // 1.move array and insert
  // memmove(array+pos+2,array+pos+1,sizeof(MappingType)*(GetSize()-pos-1));
  for (int i = GetSize(); i > pos + 1; --i) {
    array[i] = array[i - 1];
  }
  array[pos + 1] = {new_key, new_value};
  // 2.update size
  IncreaseSize(1);
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
  // copy the right half or left half? right half
  int s = (GetSize() + 1) / 2;
  if (s <= 0) {
    return;
  }
  recipient->CopyHalfFrom(array + GetSize() - s, s, buffer_pool_manager);
  // remember every B tree page is stored on disk!
  // don't forget update their parent_page_id!
  for (int i = GetSize() - s; i < GetSize(); ++i) {
    Page *child_page = buffer_pool_manager->FetchPage(array[i].second);
    // child node 不一定是internal node
    auto child_page_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());  // cast to in mem object
    child_page_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_node->GetPageId(), true);
  }
  // update size
  IncreaseSize(-s);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size,
                                                  BufferPoolManager *buffer_pool_manager) {
  // memcpy(array, items, size*sizeof(MappingType));
  int current_size = GetSize();
  for (int i = 0; i < size; ++i) {
    array[current_size + i] = items[i];
  }
  IncreaseSize(size);  // why inc size-1 in other's code, not correct??
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
  // memmove(array+index,array+index+1,sizeof(MappingType)*(GetSize()-index-1));
  for (int i = index; i < GetSize() - 2; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto res = array[1].second;
  // return the only child??? but it has at least two child! which should return then? why always return the right one??
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 *
 * recipient page is the predecessor(left) page of current page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent,
                                               BufferPoolManager *buffer_pool_manager) {
  // how to update relevent key & value parin in its parent page? this function doesn't care
  auto parnet_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parnet_page->GetData());
  // move the parent key down
  SetKeyAt(0, parent_node->KeyAt(index_in_parent));
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), false);
  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
  // update child page's parent_page_id property
  for (int i = 0; i < GetSize(); ++i) {
    auto page = buffer_pool_manager->FetchPage(array[i].second);
    auto page_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
    page_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_node->GetPageId(), true);
  }
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int current_size = GetSize();
  // memcpy(array+current_size, items, (size_t)size*sizeof(MappingType));
  for (int i = 0; i < size; ++i) {
    array[current_size + i] = items[i];
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {
  // why should got to the parent page to fetch the key?
  Page *parent_page = buffer_pool_manager->FetchPage(GetPageId());
  BPlusTreeInternalPage *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto key = parent_node->ValueIndex(GetPageId());
  MappingType pair = std::make_pair(parent_node->KeyAt(key), array[0].second);
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  parent_node->SetKeyAt(key, array[1].first);
  Remove(0);
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = pair;
  IncreaseSize(1);
  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                       BufferPoolManager *buffer_pool_manager) {
  Page *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  page_id_t parent_index =
      parent_node->ValueIndex(GetPageId()) + 1;  // add one,recipient is the right sibling of current node
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), false);
  recipient->CopyFirstFrom(array[GetSize() - 1], parent_index, buffer_pool_manager);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, int parent_index,
                                                   BufferPoolManager *buffer_pool_manager) {
  Page *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  array[0].first = parent_node->KeyAt(parent_index);
  // memmove(array+1,array,GetSize()*sizeof(pair));
  for (int i = GetSize(); i > 0; --i) {
    array[i - 1] = array[i];
  }
  array[0] = pair;
  parent_node->SetKeyAt(parent_index, pair.first);

  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreeInternalPage *child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
  child_node->SetPageId(GetPageId());

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
