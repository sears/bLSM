
#include <math.h>
#include "merger.h"
#include "logiterators.cpp"
#include "datapage.h"

inline DataPage<datatuple>*
insertTuple(int xid, DataPage<datatuple> *dp, datatuple *t,
            logtable *ltable,
            diskTreeComponent * ltree,
            int64_t &dpages, int64_t &npages);

int merge_scheduler::addlogtable(logtable *ltable)
{

    struct logtable_mergedata * mdata = new logtable_mergedata;

    // initialize merge data
    mdata->rbtree_mut = new pthread_mutex_t;
    pthread_mutex_init(mdata->rbtree_mut,0);
    ltable->set_tree_c0_mergeable(NULL);
    
    mdata->input_needed = new bool(false);
    
    mdata->input_ready_cond = new pthread_cond_t;
    pthread_cond_init(mdata->input_ready_cond,0);
    
    mdata->input_needed_cond = new pthread_cond_t;
    pthread_cond_init(mdata->input_needed_cond,0);

    mdata->input_size = new int64_t(100);

    mdata->diskmerge_args = new merger_args;
    mdata->memmerge_args = new merger_args;
    
    mergedata.push_back(std::make_pair(ltable, mdata));
    return mergedata.size()-1;
    
}

merge_scheduler::~merge_scheduler()
{
    for(size_t i=0; i<mergedata.size(); i++)
    {
        logtable *ltable = mergedata[i].first;
        logtable_mergedata *mdata = mergedata[i].second;

        //delete the mergedata fields
        delete mdata->rbtree_mut;        
        delete mdata->input_needed;
        delete mdata->input_ready_cond;
        delete mdata->input_needed_cond;
        delete mdata->input_size;

        //delete the merge thread structure variables
        pthread_cond_destroy(mdata->diskmerge_args->in_block_needed_cond);
        delete mdata->diskmerge_args->in_block_needed_cond;
        delete mdata->diskmerge_args->in_block_needed;
        
        pthread_cond_destroy(mdata->diskmerge_args->out_block_needed_cond);        
        delete mdata->diskmerge_args->out_block_needed_cond;
        delete mdata->diskmerge_args->out_block_needed;
        
        pthread_cond_destroy(mdata->diskmerge_args->in_block_ready_cond);
        delete mdata->diskmerge_args->in_block_ready_cond;
        pthread_cond_destroy(mdata->diskmerge_args->out_block_ready_cond);
        delete mdata->diskmerge_args->out_block_ready_cond;
        
        delete mdata->diskmerge_args;
        delete mdata->memmerge_args;
    }
    mergedata.clear();

}

void merge_scheduler::shutdown()
{
    //signal shutdown
    for(size_t i=0; i<mergedata.size(); i++)
    {
        logtable *ltable = mergedata[i].first;
        logtable_mergedata *mdata = mergedata[i].second;

        //flush the in memory table to write any tuples still in memory
        ltable->flushTable();
        
        pthread_mutex_lock(mdata->rbtree_mut);
        ltable->stop();
        pthread_cond_signal(mdata->input_ready_cond);
        
        //*(mdata->diskmerge_args->still_open)=false;//same pointer so no need
        
        pthread_mutex_unlock(mdata->rbtree_mut);

    }

    for(size_t i=0; i<mergedata.size(); i++)
    {
        logtable_mergedata *mdata = mergedata[i].second;
        
        pthread_join(mdata->memmerge_thread,0);
        pthread_join(mdata->diskmerge_thread,0);
    }
    

}

void merge_scheduler::startlogtable(int index, int64_t MAX_C0_SIZE)
{

    logtable * ltable = mergedata[index].first;
    struct logtable_mergedata *mdata = mergedata[index].second;

    pthread_cond_t * block1_needed_cond = new pthread_cond_t;
    pthread_cond_init(block1_needed_cond,0);
    pthread_cond_t * block2_needed_cond = new pthread_cond_t;
    pthread_cond_init(block2_needed_cond,0);

    pthread_cond_t * block1_ready_cond = new pthread_cond_t;
    pthread_cond_init(block1_ready_cond,0);
    pthread_cond_t * block2_ready_cond = new pthread_cond_t;
    pthread_cond_init(block2_ready_cond,0);

    bool *block1_needed = new bool(false);
    bool *block2_needed = new bool(false);
    
    //wait to merge the next block until we have merged block FUDGE times.
    static const int FUDGE = 1;
    static double R = MIN_R;
    int64_t * block1_size = new int64_t;
    *block1_size = FUDGE * ((int)R) * (*(mdata->input_size));

    //initialize rb-tree
    ltable->set_tree_c0(new rbtree_t);

    //disk merger args

    ltable->max_c0_size = MAX_C0_SIZE;

    diskTreeComponent ** block1_scratch = new diskTreeComponent*;
    *block1_scratch=0;

    DEBUG("Tree C1 is %lld\n", (long long)ltable->get_tree_c1()->get_root_rec().page);
    DEBUG("Tree C2 is %lld\n", (long long)ltable->get_tree_c2()->get_root_rec().page);

    struct merger_args diskmerge_args= {
        ltable, 
            1,  //worker id 
            mdata->rbtree_mut, //block_ready_mutex
            block1_needed_cond, //in_block_needed_cond
            block1_needed,      //in_block_needed
            block2_needed_cond, //out_block_needed_cond
            block2_needed,      //out_block_needed
            block1_ready_cond,  //in_block_ready_cond
            block2_ready_cond,  //out_block_ready_cond
        0, //max_tree_size No max size for biggest component
        &R, //r_i
                };

    *mdata->diskmerge_args = diskmerge_args;

    struct merger_args memmerge_args =
        {
            ltable,
            2,
            mdata->rbtree_mut,
            mdata->input_needed_cond,
            mdata->input_needed,
            block1_needed_cond,
            block1_needed,
            mdata->input_ready_cond,
            block1_ready_cond,
            (int64_t)(R * R * MAX_C0_SIZE),
            &R,
        };
    
    *mdata->memmerge_args = memmerge_args;

    void * (*diskmerger)(void*) = diskMergeThread;
    void * (*memmerger)(void*) = memMergeThread;

    pthread_create(&mdata->diskmerge_thread, 0, diskmerger, mdata->diskmerge_args);
    pthread_create(&mdata->memmerge_thread, 0, memmerger, mdata->memmerge_args);
    
}

/**
 *  Merge algorithm
 *<pre>
  1: while(1)
  2:    wait for c0_mergable
  3:    begin
  4:    merge c0_mergable and c1 into c1'
  5:    force c1'
  6:    delete c1
  7:    if c1' is too big
  8:       c1 = new_empty
  9:       c1_mergable = c1'
 10:    else
 11:       c1 = c1'
 12:    commit
  </pre>
 */
void* memMergeThread(void*arg)
{

    int xid;// = Tbegin();

    merger_args * a = (merger_args*)(arg);

    logtable * ltable = a->ltable;
    assert(ltable->get_tree_c1());
    
    int merge_count =0;
//    pthread_mutex_lock(a->block_ready_mut);
    
    while(true)
    {
        writelock(ltable->header_lock,0);
        int done = 0;
        // wait for c0_mergable
        while(!ltable->get_tree_c0_mergeable())
        {            
            pthread_mutex_lock(a->block_ready_mut);
            *a->in_block_needed = true;
            //pthread_cond_signal(a->in_block_needed_cond);
            pthread_cond_broadcast(a->in_block_needed_cond);

            if(!ltable->is_still_running()){
                done = 1;
                pthread_mutex_unlock(a->block_ready_mut);
                break;
            }
            
            printf("mmt:\twaiting for block ready cond\n");
            unlock(ltable->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);
            
            writelock(ltable->header_lock,0);
            printf("mmt:\tblock ready\n");
            
        }        
        *a->in_block_needed = false;

        if(done==1)
        {
            pthread_mutex_lock(a->block_ready_mut);
            pthread_cond_signal(a->out_block_ready_cond);  // no block is ready.  this allows the other thread to wake up, and see that we're shutting down.
            pthread_mutex_unlock(a->block_ready_mut);
            unlock(ltable->header_lock);
            break;
        }

        // 3: Begin transaction
        xid = Tbegin();

        // 4: Merge

        //create the iterators
        diskTreeIterator<datatuple> *itrA = new diskTreeIterator<datatuple>(ltable->get_tree_c1()->get_root_rec()); // XXX don't want get_root_rec() to be here.
        memTreeIterator<rbtree_t, datatuple> *itrB =
        		new memTreeIterator<rbtree_t, datatuple>(ltable->get_tree_c0_mergeable());

        
        //create a new tree
        diskTreeComponent * c1_prime = new diskTreeComponent(xid); // XXX should not hardcode region size)

        //pthread_mutex_unlock(a->block_ready_mut);
        unlock(ltable->header_lock);

        //: do the merge
        printf("mmt:\tMerging:\n");

        int64_t npages = 0;
        int64_t mergedPages = merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, c1_prime, npages, false);

        delete itrA;
        delete itrB;

        // 5: force c1'

        //force write the new region to disk
        diskTreeComponent::force_region_rid(xid, c1_prime->get_tree_state());
        //force write the new datapages
        c1_prime->get_alloc()->force_regions(xid);

        // 6: delete c1 and c0_mergeable
        diskTreeComponent::dealloc_region_rid(xid, ltable->get_tree_c1()->get_tree_state());
        ltable->get_tree_c1()->get_alloc()->dealloc_regions(xid);

        logtable::tearDownTree(ltable->get_tree_c0_mergeable());
        ltable->set_tree_c0_mergeable(NULL);

        //writes complete
        //now atomically replace the old c1 with new c1
        //pthread_mutex_lock(a->block_ready_mut);

        writelock(ltable->header_lock,0);
        merge_count++;        
        printf("mmt:\tmerge_count %d #pages written %lld\n", merge_count, npages);      

        //TODO: this is simplistic for now
        //signal the other merger if necessary
        double target_R = *(a->r_i);
        double new_c1_size = npages * PAGE_SIZE;
        assert(target_R >= MIN_R);
        if( (new_c1_size / ltable->max_c0_size > target_R) ||
            (a->max_size && new_c1_size > a->max_size ) )
        {
        	// 7: c1' is too big

			// 8: c1 = new empty.
            ltable->set_tree_c1(new diskTreeComponent(xid));

        	printf("mmt:\tsignaling C2 for merge\n");
            printf("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable->max_c0_size, a->max_size, target_R);

            // 9: c1_mergeable = c1'
            
            // XXX need to report backpressure here!  Also, shouldn't be inside a transaction while waiting on backpressure.
            while(ltable->get_tree_c1_mergeable()) {
                pthread_mutex_lock(a->block_ready_mut);
                unlock(ltable->header_lock);
                
                pthread_cond_wait(a->out_block_needed_cond, a->block_ready_mut);
                pthread_mutex_unlock(a->block_ready_mut);
                writelock(ltable->header_lock,0);
            }
            ltable->set_tree_c1_mergeable(c1_prime);

            pthread_cond_signal(a->out_block_ready_cond);

        } else {
        	// 11: c1 = c1'
        	ltable->set_tree_c1(c1_prime);
        }

        // XXX want to set this stuff somewhere.
        logtable::table_header h;
        printf("mmt:\tUpdated C1's position on disk to %lld\n",ltable->get_tree_c1()->get_root_rec().page);

		Tcommit(xid);

        unlock(ltable->header_lock);
        
        //TODO: get the freeing outside of the lock
    }

    //pthread_mutex_unlock(a->block_ready_mut);
    
    return 0;

}


void *diskMergeThread(void*arg)
{
    int xid;// = Tbegin();

    merger_args * a = (merger_args*)(arg);

    logtable * ltable = a->ltable;
    assert(ltable->get_tree_c2());
    
    int merge_count =0;
    //pthread_mutex_lock(a->block_ready_mut);
    
    while(true)
    {
        writelock(ltable->header_lock,0);
        int done = 0;
        // get a new input for merge
        while(!ltable->get_tree_c1_mergeable())
        {
            pthread_mutex_lock(a->block_ready_mut);
            *a->in_block_needed = true;
            pthread_cond_signal(a->in_block_needed_cond);

            if(!ltable->is_still_running()){
                done = 1;
                pthread_mutex_unlock(a->block_ready_mut);
                break;
            }
            
            printf("dmt:\twaiting for block ready cond\n");
            unlock(ltable->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);

            printf("dmt:\tblock ready\n");
            writelock(ltable->header_lock,0);
        }        
        *a->in_block_needed = false;
        if(done==1)
        {
            pthread_cond_signal(a->out_block_ready_cond);
            unlock(ltable->header_lock);
            break;
        }
        
        int64_t mergedPages=0;
        
        //create the iterators
        diskTreeIterator<datatuple> *itrA = new diskTreeIterator<datatuple>(ltable->get_tree_c2()->get_root_rec());
        diskTreeIterator<datatuple> *itrB =
            new diskTreeIterator<datatuple>(ltable->get_tree_c1_mergeable()->get_root_rec());
        
        xid = Tbegin();
        
        //create a new tree
        //TODO: maybe you want larger regions for the second tree?
        diskTreeComponent * c2_prime = new diskTreeComponent(xid);

        unlock(ltable->header_lock);
        
        
        //do the merge        
        printf("dmt:\tMerging:\n");

        int64_t npages = 0;
        mergedPages = merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, c2_prime, npages, true);
      
        delete itrA;
        delete itrB;        

        //force write the new region to disk
        diskTreeComponent::force_region_rid(xid, c2_prime->get_tree_state());
        c2_prime->get_alloc()->force_regions(xid);

        diskTreeComponent::dealloc_region_rid(xid, ltable->get_tree_c1_mergeable()->get_tree_state());
        ltable->get_tree_c1_mergeable()->get_alloc()->dealloc_regions(xid);
        delete ltable->get_tree_c1_mergeable();
        ltable->set_tree_c1_mergeable(0);

        diskTreeComponent::dealloc_region_rid(xid, ltable->get_tree_c2()->get_tree_state());
        ltable->get_tree_c2()->get_alloc()->dealloc_regions(xid);
        delete ltable->get_tree_c2();


        //writes complete
        //now atomically replace the old c2 with new c2
        //pthread_mutex_lock(a->block_ready_mut);
        writelock(ltable->header_lock,0);
        
        merge_count++;        
        //update the current optimal R value
        *(a->r_i) = std::max(MIN_R, sqrt( (npages * 1.0) / (ltable->max_c0_size/PAGE_SIZE) ) );
        
        printf("dmt:\tmerge_count %d\t#written pages: %lld\n optimal r %.2f", merge_count, npages, *(a->r_i));

        // 11: C2 is never too big.
        ltable->set_tree_c2(c2_prime);

        logtable::table_header h; // XXX Need to set header.

        printf("dmt:\tUpdated C2's position on disk to %lld\n",(long long)-1);

        Tcommit(xid);
        
        unlock(ltable->header_lock);
    }
    return 0;
}

template <class ITA, class ITB>
int64_t merge_iterators(int xid,
                        ITA *itrA, //iterator on c1 or c2
                        ITB *itrB, //iterator on c0 or c1, respectively
                        logtable *ltable,
                        diskTreeComponent *scratch_tree,
                        int64_t &npages,
                        bool dropDeletes  // should be true iff this is biggest component
                        )
{
    int64_t dpages = 0;
    int64_t ntuples = 0;
    DataPage<datatuple> *dp = 0;

    datatuple *t1 = itrA->getnext();
    datatuple *t2 = 0;
    
    while( (t2=itrB->getnext()) != 0)
    {        
        DEBUG("tuple\t%lld: keylen %d datalen %d\n",
               ntuples, *(t2->keylen),*(t2->datalen) );        

        while(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) < 0) // t1 is less than t2
        {
            //insert t1
            dp = insertTuple(xid, dp, t1, ltable, scratch_tree,
                             dpages, npages);

            datatuple::freetuple(t1);
            ntuples++;      
            //advance itrA
            t1 = itrA->getnext();
        }

        if(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) == 0)
        {
            datatuple *mtuple = ltable->gettuplemerger()->merge(t1,t2);
            
            //insert merged tuple, drop deletes
            if(dropDeletes && !mtuple->isDelete())
                dp = insertTuple(xid, dp, mtuple, ltable, scratch_tree,
                                 dpages, npages);
            
            datatuple::freetuple(t1);
            t1 = itrA->getnext();  //advance itrA
            datatuple::freetuple(mtuple);
        }
        else
        {        
            //insert t2
            dp = insertTuple(xid, dp, t2, ltable, scratch_tree,
                             dpages, npages);
            // cannot free any tuples here; they may still be read through a lookup
        }

        datatuple::freetuple(t2);
        ntuples++;
    }

    while(t1 != 0) // t1 is less than t2
        {
            dp = insertTuple(xid, dp, t1, ltable, scratch_tree,
                             dpages, npages);

            datatuple::freetuple(t1);
            ntuples++;      
            //advance itrA
            t1 = itrA->getnext();
        }

    if(dp!=NULL)
        delete dp;
    DEBUG("dpages: %d\tnpages: %d\tntuples: %d\n", dpages, npages, ntuples);
    
    return dpages;

}


inline DataPage<datatuple>*
insertTuple(int xid, DataPage<datatuple> *dp, datatuple *t,
            logtable *ltable,
            diskTreeComponent * ltree,
            int64_t &dpages, int64_t &npages)
{
    if(dp==0)
    {
        dp = ltable->insertTuple(xid, t, ltree);
        dpages++;
    }
    else if(!dp->append(t))
    {
        npages += dp->get_page_count();
        delete dp;
        dp = ltable->insertTuple(xid, t, ltree);
        dpages++;
    }

    return dp;    
}




