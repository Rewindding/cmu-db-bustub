/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t pageId, int Index, BufferPoolManager* bmp) :bufferPoolManager(bmp),leafPageId(pageId),kvIndex(Index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    Page* page = bufferPoolManager->FetchPage(leafPageId);
    B_PLUS_TREE_LEAF_PAGE_TYPE* leafPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE* >(page->GetData());
    if (leafPage->GetNextPageId() != INVALID_PAGE_ID) {
       return false; 
    }
    bufferPoolManager->UnpinPage(page->GetPageId(),false);
    return kvIndex == leafPage->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    Page* page = bufferPoolManager->FetchPage(leafPageId);
    B_PLUS_TREE_LEAF_PAGE_TYPE* leafPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE* >(page->GetData());
    bufferPoolManager->UnpinPage(page->GetPageId(),false);
    return leafPage->GetItem(kvIndex);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    Page* page = bufferPoolManager->FetchPage(leafPageId);
    B_PLUS_TREE_LEAF_PAGE_TYPE* leafPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE* >(page->GetData());
    ++kvIndex;
    if(kvIndex >= leafPage->GetSize()) {
        kvIndex = 0;
        leafPageId = leafPage->GetNextPageId();
    }
    bufferPoolManager->UnpinPage(page->GetPageId(),false);
    return *this; // ???
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
    return itr.leafPageId==this->leafPageId&&itr.kvIndex==this->kvIndex;
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
    return itr.leafPageId!=this->leafPageId||itr.kvIndex!=this->kvIndex;
}
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
