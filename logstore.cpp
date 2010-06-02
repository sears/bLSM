#include "logstore.h"
#include "merger.h"

#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>
#include "mergeStats.h"

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

    r_val = MIN_R;
    tree_c0 = NULL;
    tree_c0_mergeable = NULL;
    tree_c1 = NULL;
    tree_c1_mergeable = NULL;
    tree_c2 = NULL;
    this->still_running_ = true;
    this->merge_mgr = new mergeManager(this);
    this->mergedata = 0;
    //tmerger = new tuplemerger(&append_merger);
    tmerger = new tuplemerger(&replace_merger);

    header_mut = rwlc_initlock();
    pthread_mutex_init(&rb_mut, 0);
    pthread_cond_init(&c0_needed, 0);
    pthread_cond_init(&c0_ready, 0);
    pthread_cond_init(&c1_needed, 0);
    pthread_cond_init(&c1_ready, 0);

    tsize = 0;
    tree_bytes = 0;
        
    epoch = 0;

    this->internal_region_size = internal_region_size;
    this->datapage_region_size = datapage_region_size;
    this->datapage_size = datapage_size;

    c0_stats = merge_mgr->get_merge_stats(0);
    merge_mgr->new_merge(0);
    c0_stats->starting_merge();
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

    pthread_mutex_destroy(&rb_mut);
    rwlc_deletelock(header_mut);
    pthread_cond_destroy(&c0_needed);
    pthread_cond_destroy(&c0_ready);
    pthread_cond_destroy(&c1_needed);
    pthread_cond_destroy(&c1_ready);
    delete tmerger;
}

template<class TUPLE>
void logtable<TUPLE>::init_stasis() {

  DataPage<datatuple>::register_stasis_page_impl();
  // XXX Workaround Stasis' (still broken) default concurrent buffer manager
  stasis_buffer_manager_factory = stasis_buffer_manager_hash_factory;

  Tinit();

}

template<class TUPLE>
void logtable<TUPLE>::deinit_stasis() { Tdeinit(); }

template<class TUPLE>
recordid logtable<TUPLE>::allocTable(int xid)
{

    table_rec = Talloc(xid, sizeof(tbl_header));
    mergeStats * stats = 0;
    //create the big tree
    tree_c2 = new diskTreeComponent(xid, internal_region_size, datapage_region_size, datapage_size, stats);

    //create the small tree
    tree_c1 = new diskTreeComponent(xid, internal_region_size, datapage_region_size, datapage_size, stats);

    update_persistent_header(xid);

    return table_rec;
}
template<class TUPLE>
void logtable<TUPLE>::openTable(int xid, recordid rid) {
  table_rec = rid;
  Tread(xid, table_rec, &tbl_header);
  tree_c2 = new diskTreeComponent(xid, tbl_header.c2_root, tbl_header.c2_state, tbl_header.c2_dp_state, 0);
  tree_c1 = new diskTreeComponent(xid, tbl_header.c1_root, tbl_header.c1_state, tbl_header.c1_dp_state, 0);
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


    int expmcount = merge_count;
    merge_mgr->finished_merge(0);

    //this is for waiting the previous merger of the mem-tree
    //hopefullly this wont happen

    bool blocked = false;

    while(get_tree_c0_mergeable()) {
      rwlc_cond_wait(&c0_needed, header_mut);
      blocked = true;
      if(expmcount != merge_count) {
          return;
      }
    }

    c0_stats->handed_off_tree();
    merge_mgr->new_merge(0);

    gettimeofday(&stop_tv,0);
    stop = tv_to_double(stop_tv);
    
    set_tree_c0_mergeable(get_tree_c0());

    pthread_cond_signal(&c0_ready);
    DEBUG("Signaled c0-c1 merge thread\n");

    merge_count ++;
    set_tree_c0(new memTreeComponent<datatuple>::rbtree_t);
    c0_stats->starting_merge();

    tsize = 0;
    tree_bytes = 0;

    if(blocked && stop - start > 0.1) {
      if(first)
      {
          printf("\nBlocked writes for %f sec\n", stop-start);
          first = 0;
      }
      else
      {
          printf("\nBlocked writes for %f sec (serviced writes for %f sec)\n",
                 stop-start, start-last_start);
      }
      last_start = stop;
    } else {
      DEBUG("signaled c0-c1 merge\n");
    }

}

template<class TUPLE>
datatuple * logtable<TUPLE>::findTuple(int xid, const datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
    datatuple *search_tuple = datatuple::create(key, keySize);

    rwlc_readlock(header_mut);

    pthread_mutex_lock(&rb_mut);

    datatuple *ret_tuple=0; 

    //step 1: look in tree_c0
    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
    if(rbitr != get_tree_c0()->end())
    {
        DEBUG("tree_c0 size %d\n", get_tree_c0()->size());
        ret_tuple = (*rbitr)->create_copy();
    }

    pthread_mutex_unlock(&rb_mut);

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

    rwlc_unlock(header_mut);
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
        
    rwlc_readlock(header_mut);

    datatuple *ret_tuple=0; 
    //step 1: look in tree_c0

    pthread_mutex_lock(&rb_mut);

    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
    if(rbitr != get_tree_c0()->end())
    {
        DEBUG("tree_c0 size %d\n", tree_c0->size());
        ret_tuple = (*rbitr)->create_copy();

        pthread_mutex_unlock(&rb_mut);
        
    }
    else
    {
        DEBUG("Not in mem tree %d\n", tree_c0->size());

        pthread_mutex_unlock(&rb_mut);

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

    rwlc_unlock(header_mut);
    datatuple::freetuple(search_tuple);
    
    return ret_tuple;

}

template<class TUPLE>
void logtable<TUPLE>::insertTuple(datatuple *tuple)
{
    rwlc_writelock(header_mut); // XXX want this to be a readlock, but tick, and the stats need it to be a writelock for now...
    //lock the red-black tree
    merge_mgr->read_tuple_from_small_component(0, tuple);  // has to be before rb_mut, since it calls tick with block = true, and that releases header_mut.
    pthread_mutex_lock(&rb_mut);
    //find the previous tuple with same key in the memtree if exists
    memTreeComponent<datatuple>::rbtree_t::iterator rbitr = tree_c0->find(tuple);
    datatuple * t  = 0;
    if(rbitr != tree_c0->end())
    {        
        datatuple *pre_t = *rbitr;
        //do the merging
        c0_stats->read_tuple_from_large_component(pre_t);
        datatuple *new_t = tmerger->merge(pre_t, tuple);
        c0_stats->merged_tuples(new_t, tuple, pre_t);
        t = new_t;
        tree_c0->erase(pre_t); //remove the previous tuple        

        tree_c0->insert(new_t); //insert the new tuple

        //update the tree size (+ new_t size - pre_t size)
        tree_bytes += ((int64_t)new_t->byte_length() - (int64_t)pre_t->byte_length());

        datatuple::freetuple(pre_t); //free the previous tuple
    }
    else //no tuple with same key exists in mem-tree
    {

        t = tuple->create_copy();

        //insert tuple into the rbtree        
        tree_c0->insert(t);

        tsize++;
        tree_bytes += t->byte_length();// + RB_TREE_OVERHEAD;

    }

    merge_mgr->wrote_tuple(0, t);

    pthread_mutex_unlock(&rb_mut);

    //flushing logic
    if(tree_bytes >= max_c0_size )
    {
      DEBUG("tree size before merge %d tuples %lld bytes.\n", tsize, tree_bytes);
//      rwlc_unlock(header_mut);
//      rwlc_writelock(header_mut);
      // the test of tree size needs to be atomic with the flushTable, and flushTable needs a writelock.
      if(tree_bytes >= max_c0_size) {
        flushTable();
      }
    }
    rwlc_unlock(header_mut);

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
  epoch++;
  for(unsigned int i = 0; i < its.size(); i++) {
    its[i]->invalidate();
  }
}
template class logtable<datatuple>;
