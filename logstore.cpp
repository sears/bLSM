#include <string.h>
#include <assert.h>
#include <math.h>
#include <ctype.h>

#include "merger.h"
#include "logstore.h"
#include "logiterators.h"
#include "datapage.cpp"

#include <stasis/transactional.h>
#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>

static inline double tv_to_double(struct timeval tv)
{
  return static_cast<double>(tv.tv_sec) +
      (static_cast<double>(tv.tv_usec) / 1000000.0);
}

/////////////////////////////////////////////////////////////////
// LOG TABLE IMPLEMENTATION
/////////////////////////////////////////////////////////////////

template class DataPage<datatuple>;

logtable::logtable()
{

    tree_c0 = NULL;
    tree_c0_mergeable = NULL;
    tree_c1 = NULL;
    tree_c1_mergeable = NULL;
    tree_c2 = NULL;
    this->still_running_ = true;
    this->mergedata = 0;
    fixed_page_count = -1;
    //tmerger = new tuplemerger(&append_merger);
    tmerger = new tuplemerger(&replace_merger);

    header_lock = initlock();

    tsize = 0;
    tree_bytes = 0;
        
    epoch = 0;
    
}

void logtable::tearDownTree(rbtree_ptr_t tree) {
    datatuple * t = 0;
    rbtree_t::iterator old;
    for(rbtree_t::iterator delitr  = tree->begin();
                           delitr != tree->end();
                           delitr++) {
    	if(t) {
    		tree->erase(old);
    		datatuple::freetuple(t);
    		t = 0;
    	}
    	t = *delitr;
    	old = delitr;
    }
	if(t) {
		tree->erase(old);
		datatuple::freetuple(t);
	}
    delete tree;
}

logtable::~logtable()
{
    if(tree_c1 != NULL)        
        delete tree_c1;
    if(tree_c2 != NULL)
        delete tree_c2;

    if(tree_c0 != NULL)
    {
    	tearDownTree(tree_c0);
    }

    deletelock(header_lock);
    delete tmerger;
}

recordid logtable::allocTable(int xid)
{

    table_rec = Talloc(xid, sizeof(tbl_header));
    
    //create the big tree
    tbl_header.c2_dp_state = Talloc(xid, DataPage<datatuple>::RegionAllocator::header_size);
    tree_c2 = new diskTreeComponent(xid);

    //create the small tree
    tbl_header.c1_dp_state = Talloc(xid, DataPage<datatuple>::RegionAllocator::header_size);
    tree_c1 = new diskTreeComponent(xid);
    
    tbl_header.c2_root = tree_c2->get_root_rec();
    tbl_header.c2_dp_state = tree_c2->get_alloc()->header_rid();
    tbl_header.c2_state = tree_c2->get_tree_state();
    tbl_header.c1_root = tree_c1->get_root_rec();
    tbl_header.c2_dp_state = tree_c1->get_alloc()->header_rid();
    tbl_header.c1_state = tree_c1->get_tree_state();
    
    Tset(xid, table_rec, &tbl_header);    
    
    return table_rec;
}

void logtable::flushTable()
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
    printf("prv merge not complete\n");


    while(get_tree_c0_mergeable()) {
        unlock(header_lock);
//        pthread_mutex_lock(mergedata->rbtree_mut);
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

    printf("prv merge complete\n");

    gettimeofday(&stop_tv,0);
    stop = tv_to_double(stop_tv);
    
    //rbtree_ptr *tmp_ptr = new rbtree_ptr_t; //(typeof(h->scratch_tree)*) malloc(sizeof(void*));
    set_tree_c0_mergeable(get_tree_c0());

//    pthread_mutex_lock(mergedata->rbtree_mut);
    pthread_cond_signal(mergedata->input_ready_cond);
//    pthread_mutex_unlock(mergedata->rbtree_mut);

    merge_count ++;
    set_tree_c0(new rbtree_t);

    tsize = 0;
    tree_bytes = 0;
    
    pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(header_lock);
    if(first)
    {
        printf("flush waited %f sec\n", stop-start);
        first = 0;
    }
    else
    {
        printf("flush waited %f sec (worked %f)\n",
               stop-start, start-last_start);
    }
    last_start = stop;

}

datatuple * logtable::findTuple(int xid, const datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
	datatuple *search_tuple = datatuple::create(key, keySize);

    readlock(header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 

    //step 1: look in tree_c0
    rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
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
        datatuple *tuple_c1 = findTuple(xid, key, keySize, get_tree_c1());
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
        datatuple *tuple_oc1 = findTuple(xid, key, keySize, get_tree_c1_mergeable());
        
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
        datatuple *tuple_c2 = findTuple(xid, key, keySize, get_tree_c2());

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
datatuple * logtable::findTuple_first(int xid, datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
    datatuple * search_tuple = datatuple::create(key, keySize);
        
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 
    //step 1: look in tree_c0

    rbtree_t::iterator rbitr = get_tree_c0()->find(search_tuple);
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
            ret_tuple = findTuple(xid, key, keySize, get_tree_c1());
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in first disk tree\n");

            //step 4: check old c1 if exists
            if( get_tree_c1_mergeable() != 0)
            {
                DEBUG("old c1 tree not null\n");
                ret_tuple = findTuple(xid, key, keySize, get_tree_c1_mergeable());
            }
                
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in old first disk tree\n");

            //step 5: check c2
            ret_tuple = findTuple(xid, key, keySize, tree_c2);            
        }        
    }


     

    pthread_mutex_unlock(mergedata->rbtree_mut);
    datatuple::freetuple(search_tuple);
    
    return ret_tuple;

}

void logtable::insertTuple(datatuple *tuple)
{
    //lock the red-black tree
    readlock(header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);
    //find the previous tuple with same key in the memtree if exists
    rbtree_t::iterator rbitr = tree_c0->find(tuple);
    if(rbitr != tree_c0->end())
    {        
        datatuple *pre_t = *rbitr;
        //do the merging
        datatuple *new_t = tmerger->merge(pre_t, tuple);
        tree_c0->erase(pre_t); //remove the previous tuple        

        tree_c0->insert(new_t); //insert the new tuple

        //update the tree size (+ new_t size - pre_t size)
        tree_bytes += (new_t->byte_length() - pre_t->byte_length());

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


DataPage<datatuple>* logtable::insertTuple(int xid, datatuple *tuple, diskTreeComponent *ltree)
{
    //create a new data page -- either the last region is full, or the last data page doesn't want our tuple.  (or both)
    
    DataPage<datatuple> * dp = 0;
    int count = 0;
    while(dp==0)
    {
      dp = new DataPage<datatuple>(xid, fixed_page_count, ltree->get_alloc());

        //insert the record into the data page
        if(!dp->append(tuple))
        {
        	// the last datapage must have not wanted the tuple, and then this datapage figured out the region is full.
        	delete dp;
            dp = 0;
		    assert(count == 0); // only retry once.
            count ++;
        }
    }
    

    RegionAllocConf_t alloc_conf;
    //insert the record key and id of the first page of the datapage to the diskTreeComponent
    Tread(xid,ltree->get_tree_state(), &alloc_conf);
    diskTreeComponent::appendPage(xid, ltree->get_root_rec(), ltree->lastLeaf,
                        tuple->key(),
                        tuple->keylen(),
                        ltree->alloc_region,
                        &alloc_conf,
                        dp->get_start_pid()
                        );
    Tset(xid,ltree->get_tree_state(),&alloc_conf);
                        

    //return the datapage
    return dp;
}

datatuple * logtable::findTuple(int xid, datatuple::key_t key, size_t keySize,  diskTreeComponent *ltree)
{
    datatuple * tup=0;

    //find the datapage
    pageid_t pid = ltree->findPage(xid, ltree->get_root_rec(), (byte*)key, keySize);

    if(pid!=-1)
    {
        DataPage<datatuple> * dp = new DataPage<datatuple>(xid, pid);
        dp->recordRead(key, keySize, &tup);
        delete dp;           
    }
    return tup;
}

void logtable::registerIterator(logtableIterator<datatuple> * it) {
  its.push_back(it);
}
void logtable::forgetIterator(logtableIterator<datatuple> * it) {
  for(unsigned int i = 0; i < its.size(); i++) {
    if(its[i] == it) {
      its.erase(its.begin()+i);
      break;
    }
  }
}
void logtable::bump_epoch() {
  assert(!trywritelock(header_lock,0));
  epoch++;
  for(unsigned int i = 0; i < its.size(); i++) {
    its[i]->invalidate();
  }
}



template class logtableIterator<datatuple>;
