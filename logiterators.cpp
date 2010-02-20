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
  			lsmIterator_ = diskTreeComponentIterator::openAt(-1, tree_, key1->key());
  		} else {
  			lsmIterator_ = diskTreeComponentIterator::open(-1, tree_);
  		}
  	}
  }


template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(recordid tree) :
    tree_(tree),    
//    lsmIterator_(diskTreeComponentIterator::open(-1,tree)),
    curr_tuple(0)
{
	init_iterators(NULL,NULL);
    init_helper();
}

template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(recordid tree, TUPLE& key) :
    tree_(tree),
    //lsmIterator_(diskTreeComponentIterator::openAt(-1,tree,key.key()))
    curr_tuple(0)
{
	init_iterators(&key,NULL);
    init_helper();

}
template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(diskTreeComponent *tree) :
    tree_(tree ? tree->get_root_rec() : NULLRID),
    //lsmIterator_(diskTreeComponentIterator::open(-1,tree->get_root_rec())),
    curr_tuple(0)
{
	init_iterators(NULL, NULL);
    init_helper();
}

template <class TUPLE>
diskTreeIterator<TUPLE>::diskTreeIterator(diskTreeComponent *tree, TUPLE& key) :
    tree_(tree ? tree->get_root_rec() : NULLRID),
//    lsmIterator_(diskTreeComponentIterator::openAt(-1,tree->get_root_rec(),key.key()))
    curr_tuple(0)
{
	init_iterators(&key,NULL);
	init_helper();

}

template <class TUPLE>
diskTreeIterator<TUPLE>::~diskTreeIterator()
{
    if(lsmIterator_) 
        diskTreeComponentIterator::close(-1, lsmIterator_);

    if(curr_tuple != NULL)
        free(curr_tuple);
    
    if(curr_page!=NULL)
    {
        delete curr_page;
        curr_page = 0;
    }

    
}

template <class TUPLE>
void diskTreeIterator<TUPLE>::init_helper()
{
    if(!lsmIterator_)
    {
      //  printf("treeIterator:\t__error__ init_helper():\tnull lsmIterator_");
        curr_page = 0;
        dp_itr = 0;
    }
    else
    {
        if(diskTreeComponentIterator::next(-1, lsmIterator_) == 0)
        {    
            //printf("diskTreeIterator:\t__error__ init_helper():\tlogtreeIteratr::next returned 0." );
            curr_page = 0;
            dp_itr = 0;
        }
        else
        {
            pageid_t * pid_tmp;
            pageid_t ** hack = &pid_tmp;
            diskTreeComponentIterator::value(-1,lsmIterator_,(byte**)hack);
            
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<TUPLE>(-1, curr_pageid);
            dp_itr = new DPITR_T(curr_page->begin());
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
        
        if(diskTreeComponentIterator::next(-1,lsmIterator_))
        {
            pageid_t *pid_tmp;

            pageid_t **hack = &pid_tmp;
            diskTreeComponentIterator::value(-1,lsmIterator_,(byte**)hack);
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<TUPLE>(-1, curr_pageid);
            dp_itr = new DPITR_T(curr_page->begin());
            

            readTuple = dp_itr->getnext();
            assert(readTuple);
        }
      // else readTuple is null.  We're done.
    }
    
    curr_tuple = readTuple;
    return curr_tuple;
}

template class diskTreeIterator<datatuple>;
template class changingMemTreeIterator<rbtree_t, datatuple>;
