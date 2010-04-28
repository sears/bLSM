#include "logstore.h"
#include "merger.h"

#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>

#undef try
#undef end

static inline double tv_to_double(struct timeval tv)
{
  return static_cast<double>(tv.tv_sec) +
      (static_cast<double>(tv.tv_usec) / 1000000.0);
}

/////////////////////////////////////////////////////////////////
// LOG TABLE IMPLEMENTATION
/////////////////////////////////////////////////////////////////

template<class TUPLE>
logtable<TUPLE>::logtable(pageid_t internal_region_size, pageid_t datapage_region_size, pageid_t datapage_size)
{

    tree_c0 = NULL;
    tree_c0_mergeable = NULL;
    tree_c1 = NULL;
    tree_c1_mergeable = NULL;
    tree_c2 = NULL;
    this->still_running_ = true;
    this->mergedata = 0;
    //tmerger = new tuplemerger(&append_merger);
    tmerger = new tuplemerger(&replace_merger);

    header_lock = initlock();

    tsize = 0;
    tree_bytes = 0;
        
    epoch = 0;

    this->internal_region_size = internal_region_size;
    this->datapage_region_size = datapage_region_size;
    this->datapage_size = datapage_size;
}

template<class TUPLE>
logtable<TUPLE>::~logtable()
{
    if(tree_c1 != NULL)        
        delete tree_c1;
    if(tree_c2 != NULL)
        delete tree_c2;

    if(tree_c0 != NULL)
    {
      memTreeComponent<datatuple>::tearDownTree(tree_c0);
    }

    deletelock(header_lock);
    delete tmerger;
}

template<class TUPLE>
void logtable<TUPLE>::init_stasis() {

  DataPage<datatuple>::register_stasis_page_impl();

  Tinit();

}

template<class TUPLE>
void logtable<TUPLE>::deinit_stasis() { Tdeinit(); }

template<class TUPLE>
recordid logtable<TUPLE>::allocTable(int xid)
{

    table_rec = Talloc(xid, sizeof(tbl_header));
    
    //create the big tree
    tree_c2 = new diskTreeComponent(xid, internal_region_size, datapage_region_size, datapage_size);

    //create the small tree
    tree_c1 = new diskTreeComponent(xid, internal_region_size, datapage_region_size, datapage_size);

    update_persistent_header(xid);

    return table_rec;
}
template<class TUPLE>
void logtable<TUPLE>::openTable(int xid, recordid rid) {
  table_rec = rid;
  Tread(xid, table_rec, &tbl_header);
  tree_c2 = new diskTreeComponent(xid, tbl_header.c2_root, tbl_header.c2_state, tbl_header.c2_dp_state);
  tree_c1 = new diskTreeComponent(xid, tbl_header.c1_root, tbl_header.c1_state, tbl_header.c1_dp_state);
}
template<class TUPLE>
void logtable<TUPLE>::update_persistent_header(int xid) {

	tbl_header.c2_root = tree_c2->get_root_rid();
    tbl_header.c2_dp_state = tree_c2->get_datapage_allocator_rid();
    tbl_header.c2_state = tree_c2->get_internal_node_allocator_rid();
    tbl_header.c1_root = tree_c1->get_root_rid();
    tbl_header.c1_dp_state = tree_c1->get_datapage_allocator_rid();
    tbl_header.c1_state = tree_c1->get_internal_node_allocator_rid();
    
    Tset(xid, table_rec, &tbl_header);    
}

template<class TUPLE>
void logtable<TUPLE>::setMergeData(logtable_mergedata * mdata){
  this->mergedata = mdata;
  mdata->internal_region_size = internal_region_size;
  mdata->datapage_region_size = datapage_region_size;
  mdata->datapage_size = datapage_size;

  bump_epoch();
}

template<class TUPLE>
void logtable<TUPLE>::flushTable()
{
    struct timeval start_tv, stop_tv;
    double start, stop;
    
    static double last_start;
    static bool first = 1;
    static int merge_count = 0;
    
    gettimeofday(&start_tv,0);
    start = tv_to_double(start_tv);

    
    writelock(header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);
    
    int expmcount = merge_count;


    //this is for waiting the previous merger of the mem-tree
    //hopefullly this wont happen

    while(get_tree_c0_mergeable()) {
        unlock(header_lock);
        if(tree_bytes >= max_c0_size)
            pthread_cond_wait(mergedata->input_needed_cond, mergedata->rbtree_mut);
        else
        {
            pthread_mutex_unlock(mergedata->rbtree_mut);
            return;
        }

        
        pthread_mutex_unlock(mergedata->rbtree_mut);
        
        writelock(header_lock,0);
        pthread_mutex_lock(mergedata->rbtree_mut);
        
        if(expmcount != merge_count)
        {
            unlock(header_lock);
            pthread_mutex_unlock(mergedata->rbtree_mut);
            return;                    
        }
        
    }

    gettimeofday(&stop_tv,0);
    stop = tv_to_double(stop_tv);
    
    set_tree_c0_mergeable(get_tree_c0());

    pthread_cond_signal(mergedata->input_ready_cond);

    merge_count ++;
    set_tree_c0(new memTreeComponent<datatuple>::rbtree_t);

    tsize = 0;
    tree_bytes = 0;
    
    pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(header_lock);
    if(first)
    {
        printf("Blocked writes for %f sec\n", stop-start);
        first = 0;
    }
    else
    {
        printf("Blocked writes for %f sec (serviced writes for %f sec)\n",
               stop-start, start-last_start);
    }
    last_start = stop;

}

template<class TUPLE>
datatuple * logtable<TUPLE>::findTuple(int xid, const datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
	datatuple *search_tuple = datatuple::create(key, keySize);

    readlock(header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 

    //step 1: look in tree_c0
    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
    if(rbitr != get_tree_c0()->end())
    {
        DEBUG("tree_c0 size %d\n", get_tree_c0()->size());
        ret_tuple = (*rbitr)->create_copy();
    }

    bool done = false;
    //step: 2 look into first in tree if exists (a first level merge going on)
    if(get_tree_c0_mergeable() != 0)
    {
        DEBUG("old mem tree not null %d\n", (*(mergedata->old_c0))->size());
        rbitr = get_tree_c0_mergeable()->find(search_tuple);
        if(rbitr != get_tree_c0_mergeable()->end())
        {
            datatuple *tuple = *rbitr;

            if(tuple->isDelete())  //tuple deleted
                done = true;  //return ret_tuple            
            else if(ret_tuple != 0)  //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from current tree
                ret_tuple = mtuple; //set return tuple to merge result
            }
            else //key first found in old mem tree
            {
            	ret_tuple = tuple->create_copy();
            }
            //we cannot free tuple from old-tree 'cos it is not a copy
        }            
    }

    //release the memtree lock
    pthread_mutex_unlock(mergedata->rbtree_mut);
    
    //step 3: check c1    
    if(!done)
    {
        datatuple *tuple_c1 = get_tree_c1()->findTuple(xid, key, keySize);
        if(tuple_c1 != NULL)
        {
            bool use_copy = false;
            if(tuple_c1->isDelete()) //tuple deleted
                done = true;        
            else if(ret_tuple != 0) //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple_c1, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple);  //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }            
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_c1;
            }

            if(!use_copy)
            {
                datatuple::freetuple(tuple_c1); //free tuple from tree c1
            }
        }
    }

    //step 4: check old c1 if exists
    if(!done && get_tree_c1_mergeable() != 0)
    {
        DEBUG("old c1 tree not null\n");
        datatuple *tuple_oc1 = get_tree_c1_mergeable()->findTuple(xid, key, keySize);
        
        if(tuple_oc1 != NULL)
        {
            bool use_copy = false;
            if(tuple_oc1->isDelete())
                done = true;        
            else if(ret_tuple != 0) //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple_oc1, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_oc1;
            }

            if(!use_copy)
            {
            	datatuple::freetuple(tuple_oc1); //free tuple from tree old c1
            }
        }        
    }

    //step 5: check c2
    if(!done)
    {
        DEBUG("Not in old first disk tree\n");        
        datatuple *tuple_c2 = get_tree_c2()->findTuple(xid, key, keySize);

        if(tuple_c2 != NULL)
        {
            bool use_copy = false;
            if(tuple_c2->isDelete())
                done = true;        
            else if(ret_tuple != 0)
            {
                datatuple *mtuple = tmerger->merge(tuple_c2, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_c2;                
            }

            if(!use_copy)
            {
            	datatuple::freetuple(tuple_c2);  //free tuple from tree c2
            }
        }        
    }     

    //pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(header_lock);
    datatuple::freetuple(search_tuple);
    return ret_tuple;

}

/*
 * returns the first record found with the matching key
 * (not to be used together with diffs)
 **/
template<class TUPLE>
datatuple * logtable<TUPLE>::findTuple_first(int xid, datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
    datatuple * search_tuple = datatuple::create(key, keySize);
        
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 
    //step 1: look in tree_c0

    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
    if(rbitr != get_tree_c0()->end())
    {
        DEBUG("tree_c0 size %d\n", tree_c0->size());
        ret_tuple = (*rbitr)->create_copy();
        
    }
    else
    {
        DEBUG("Not in mem tree %d\n", tree_c0->size());
        //step: 2 look into first in tree if exists (a first level merge going on)
        if(get_tree_c0_mergeable() != NULL)
        {
            DEBUG("old mem tree not null %d\n", (*(mergedata->old_c0))->size());
            rbitr = get_tree_c0_mergeable()->find(search_tuple);
            if(rbitr != get_tree_c0_mergeable()->end())
            {
                ret_tuple = (*rbitr)->create_copy();
            }            
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in old mem tree\n");

            //step 3: check c1
            ret_tuple = get_tree_c1()->findTuple(xid, key, keySize);
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in first disk tree\n");

            //step 4: check old c1 if exists
            if( get_tree_c1_mergeable() != 0)
            {
              DEBUG("old c1 tree not null\n");
              ret_tuple = get_tree_c1_mergeable()->findTuple(xid, key, keySize);
            }
                
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in old first disk tree\n");

            //step 5: check c2
            ret_tuple = get_tree_c2()->findTuple(xid, key, keySize);
        }
    }

    pthread_mutex_unlock(mergedata->rbtree_mut);
    datatuple::freetuple(search_tuple);
    
    return ret_tuple;

}

template<class TUPLE>
void logtable<TUPLE>::insertTuple(datatuple *tuple)
{
    //lock the red-black tree
    readlock(header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);
    //find the previous tuple with same key in the memtree if exists
    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = tree_c0->find(tuple);
    if(rbitr != tree_c0->end())
    {        
        datatuple *pre_t = *rbitr;
        //do the merging
        datatuple *new_t = tmerger->merge(pre_t, tuple);
        tree_c0->erase(pre_t); //remove the previous tuple        

        tree_c0->insert(new_t); //insert the new tuple

        //update the tree size (+ new_t size - pre_t size)
        tree_bytes += ((int64_t)new_t->byte_length() - (int64_t)pre_t->byte_length());

        datatuple::freetuple(pre_t); //free the previous tuple
    }
    else //no tuple with same key exists in mem-tree
    {

    	datatuple *t = tuple->create_copy();

        //insert tuple into the rbtree        
        tree_c0->insert(t);
        tsize++;
        tree_bytes += t->byte_length() + RB_TREE_OVERHEAD;

    }

    //flushing logic
    if(tree_bytes >= max_c0_size )
    {
        DEBUG("tree size before merge %d tuples %lld bytes.\n", tsize, tree_bytes);
        pthread_mutex_unlock(mergedata->rbtree_mut);
        unlock(header_lock);
        flushTable();

        readlock(header_lock,0);
        pthread_mutex_lock(mergedata->rbtree_mut);
    }
    
    //unlock
    pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(header_lock);


    DEBUG("tree size %d tuples %lld bytes.\n", tsize, tree_bytes);
}

template<class TUPLE>
void logtable<TUPLE>::registerIterator(iterator * it) {
  its.push_back(it);
}
template<class TUPLE>
void logtable<TUPLE>::forgetIterator(iterator * it) {
  for(unsigned int i = 0; i < its.size(); i++) {
    if(its[i] == it) {
      its.erase(its.begin()+i);
      break;
    }
  }
}
template<class TUPLE>
void logtable<TUPLE>::bump_epoch() {
  assert(!trywritelock(header_lock,0));
  epoch++;
  for(unsigned int i = 0; i < its.size(); i++) {
    its[i]->invalidate();
  }
}
template class logtable<datatuple>;
