#include "logstore.h"
#include "logiterators.h"

/////////////////////////////////////////////////////////////////////
// tree iterator implementation
/////////////////////////////////////////////////////////////////////

template <class TUPLE>
treeIterator<TUPLE>::treeIterator(recordid tree) :
    tree_(tree),    
    lsmIterator_(logtreeIterator::open(-1,tree)),
    curr_tuple(0)
{
    init_helper();
}

template <class TUPLE>
treeIterator<TUPLE>::treeIterator(recordid tree, TUPLE& key) :
    tree_(tree),
    lsmIterator_(logtreeIterator::openAt(-1,tree,key.get_key()))
{
    init_helper();

}

template <class TUPLE>
treeIterator<TUPLE>::~treeIterator()
{
    if(lsmIterator_) 
        logtreeIterator::close(-1, lsmIterator_);

    if(curr_tuple != NULL)
        free(curr_tuple);
    
    if(curr_page!=NULL)
    {
        delete curr_page;
        curr_page = 0;
    }

    
}

template <class TUPLE>
void treeIterator<TUPLE>::init_helper()
{
    if(!lsmIterator_)
    {
        printf("treeIterator:\t__error__ init_helper():\tnull lsmIterator_");
        curr_page = 0;
        dp_itr = 0;
    }
    else
    {
        if(logtreeIterator::next(-1, lsmIterator_) == 0)
        {    
            //printf("treeIterator:\t__error__ init_helper():\tlogtreeIteratr::next returned 0." );
            curr_page = 0;
            dp_itr = 0;
        }
        else
        {
            pageid_t * pid_tmp;
            pageid_t ** hack = &pid_tmp;
            logtreeIterator::value(-1,lsmIterator_,(byte**)hack);
            
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<TUPLE>(-1, curr_pageid);
            dp_itr = new DPITR_T(curr_page->begin());
        }
        
    }
}

template <class TUPLE>
TUPLE * treeIterator<TUPLE>::getnext()
{
    assert(this->lsmIterator_);

    if(dp_itr == 0)
        return 0;
    
    TUPLE* readTuple = dp_itr->getnext();

    
    if(!readTuple)
    {
        delete dp_itr;
        dp_itr = 0;
        delete curr_page;
        curr_page = 0;
        
        if(logtreeIterator::next(-1,lsmIterator_))
        {
            pageid_t *pid_tmp;

            pageid_t **hack = &pid_tmp;
            logtreeIterator::value(-1,lsmIterator_,(byte**)hack);
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
