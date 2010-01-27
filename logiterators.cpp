#include "logstore.h"
#include "logiterators.h"

//template <class MEMTREE, class TUPLE>
/*
template <>
const byte* toByteArray<std::set<datatuple,datatuple>, datatuple>(
    memTreeIterator<std::set<datatuple,datatuple>, datatuple> * const t)
{
    return (*(t->it_)).to_bytes();
}
*/


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
    //scratch_(),
    lsmIterator_(logtreeIterator::openAt(-1,tree,key.get_key()))//toByteArray())),
    //slot_(0)
{
    init_helper();

    /*
    treeIterator * end = this->end();
    for(;*this != *end && **this < key; ++(*this))
    {
        DEBUG("treeIterator was not at the given TUPLE");
    }
    delete end;
    */

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
    
    TUPLE* readTuple = dp_itr->getnext(-1);

    
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
            

            readTuple = dp_itr->getnext(-1); 
            assert(readTuple);
        }
        else
        {
            // TODO: what is this?
            //past end of iterator!  "end" should contain the pageid of the
            // last leaf, and 1+ numslots on that page.
            //abort();            
        }
    }
    
    return curr_tuple=readTuple;
}



/*
template <class TUPLE>
treeIterator<TUPLE>::treeIterator(treeIteratorHandle* tree, TUPLE& key) :
    tree_(tree?tree->r_:NULLRID),
    scratch_(),
    lsmIterator_(logtreeIterator::openAt(-1,tree?tree->r_:NULLRID,key.get_key())),//toByteArray())),
    slot_(0)
{
    init_helper();
    if(lsmIterator_) {
        treeIterator * end = this->end();
        for(;*this != *end && **this < key; ++(*this)) { }
        delete end;
    } else {
        this->slot_ = 0;
        this->pageid_ = 0;
    }
}

template <class TUPLE>
treeIterator<TUPLE>::treeIterator(recordid tree, TUPLE &scratch) :
    tree_(tree),
    scratch_(scratch),
    lsmIterator_(logtreeIterator::open(-1,tree)),
    slot_(0)
{
    init_helper();
}

template <class TUPLE>
treeIterator<TUPLE>::treeIterator(treeIteratorHandle* tree) :
    tree_(tree?tree->r_:NULLRID),
    scratch_(),
    lsmIterator_(logtreeIterator::open(-1,tree?tree->r_:NULLRID)),
    slot_(0)
{
    init_helper();
}

template <class TUPLE>
treeIterator<TUPLE>::treeIterator(treeIterator& t) :
    tree_(t.tree_),
    scratch_(t.scratch_),    
    lsmIterator_(t.lsmIterator_?logtreeIterator::copy(-1,t.lsmIterator_):0),
    slot_(t.slot_),
    pageid_(t.pageid_),
    p_((Page*)((t.p_)?loadPage(-1,t.p_->id):0))
    //currentPage_((PAGELAYOUT*)((p_)?p_->impl:0))
{
    if(p_)
        readlock(p_->rwlatch,0);
}
*/
