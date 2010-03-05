#include "logstore.h"
#include "logiterators.h"

/////////////////////////////////////////////////////////////////////
// tree iterator implementation
/////////////////////////////////////////////////////////////////////

template <class TUPLE>
void diskTreeIterator<TUPLE>::init_iterators(TUPLE * key1, TUPLE * key2) {
	assert(!key2); // unimplemented
  	if(tree_.size == INVALID_SIZE) {
  		lsmIterator_ = NULL;
  	} else {
  		if(key1) {
  			lsmIterator_ = new diskTreeComponentIterator(-1, tree_, key1->key(), key1->keylen());
  		} else {
  			lsmIterator_ = new diskTreeComponentIterator(-1, tree_);
  		}
  	}
  }


template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(recordid tree) :
    tree_(tree)
{
	init_iterators(NULL,NULL);
    init_helper(NULL);
}

template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(recordid tree, TUPLE& key) :
    tree_(tree)
{
	init_iterators(&key,NULL);
    init_helper(&key);

}
template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(diskTreeComponent *tree) :
    tree_(tree ? tree->get_root_rec() : NULLRID)
{
	init_iterators(NULL, NULL);
    init_helper(NULL);
}

template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(diskTreeComponent *tree, TUPLE& key) :
    tree_(tree ? tree->get_root_rec() : NULLRID)
{
    init_iterators(&key,NULL);
    init_helper(&key);

}

template <class TUPLE>
diskTreeIterator<TUPLE>::~diskTreeIterator()
{
    if(lsmIterator_) {
        lsmIterator_->close();
        delete lsmIterator_;
    }

    if(curr_page!=NULL)
    {
        delete curr_page;
        curr_page = 0;
    }

    
}

template <class TUPLE>
void diskTreeIterator<TUPLE>::init_helper(TUPLE* key1)
{
    if(!lsmIterator_)
    {
        DEBUG("treeIterator:\t__error__ init_helper():\tnull lsmIterator_");
        curr_page = 0;
        dp_itr = 0;
    }
    else
    {
        if(lsmIterator_->next() == 0)
        {    
            DEBUG("diskTreeIterator:\t__error__ init_helper():\tlogtreeIteratr::next returned 0." );
            curr_page = 0;
            dp_itr = 0;
        }
        else
        {
            pageid_t * pid_tmp;
            pageid_t ** hack = &pid_tmp;
            lsmIterator_->value((byte**)hack);
            
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<TUPLE>(-1, curr_pageid);

            DEBUG("opening datapage iterator %lld at key %s\n.", curr_pageid, key1 ? (char*)key1->key() : "NULL");
            dp_itr = new DPITR_T(curr_page, key1);
        }
        
    }
}

template <class TUPLE>
TUPLE * diskTreeIterator<TUPLE>::getnext()
{
	if(!this->lsmIterator_) { return NULL; }

    if(dp_itr == 0)
        return 0;
    
    TUPLE* readTuple = dp_itr->getnext();

    
    if(!readTuple)
    {
        delete dp_itr;
        dp_itr = 0;
        delete curr_page;
        curr_page = 0;
        
        if(lsmIterator_->next())
        {
            pageid_t *pid_tmp;

            pageid_t **hack = &pid_tmp;
            size_t ret = lsmIterator_->value((byte**)hack);
            assert(ret == sizeof(pageid_t));
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<TUPLE>(-1, curr_pageid);
            DEBUG("opening datapage iterator %lld at beginning\n.", curr_pageid);
            dp_itr = new DPITR_T(curr_page->begin());
            

            readTuple = dp_itr->getnext();
            assert(readTuple);
        }
      // else readTuple is null.  We're done.
    }
    
    return readTuple;
}

template class diskTreeIterator<datatuple>;
template class changingMemTreeIterator<rbtree_t, datatuple>;
