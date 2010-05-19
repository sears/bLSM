
#include <math.h>
#include "merger.h"


#include <stasis/transactional.h>
#undef try
#undef end

int merge_scheduler::addlogtable(logtable<datatuple> *ltable)
{

    struct logtable_mergedata * mdata = new logtable_mergedata;

    // initialize merge data
    ltable->set_tree_c0_mergeable(NULL);

    mdata->diskmerge_args = new merger_args;
    mdata->memmerge_args = new merger_args;
    
    mergedata.push_back(std::make_pair(ltable, mdata));
    return mergedata.size()-1;
    
}

merge_scheduler::~merge_scheduler()
{
    for(size_t i=0; i<mergedata.size(); i++)
    {
        logtable<datatuple> *ltable = mergedata[i].first;
        logtable_mergedata *mdata = mergedata[i].second;

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
        logtable<datatuple> *ltable = mergedata[i].first;

        ltable->stop();

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

    logtable<datatuple> * ltable = mergedata[index].first;
    struct logtable_mergedata *mdata = mergedata[index].second;
    
    static double R = MIN_R;

    //initialize rb-tree
    ltable->set_tree_c0(new memTreeComponent<datatuple>::rbtree_t);

    //disk merger args

    ltable->set_max_c0_size(MAX_C0_SIZE);

    diskTreeComponent ** block1_scratch = new diskTreeComponent*;
    *block1_scratch=0;

    DEBUG("Tree C1 is %lld\n", (long long)ltable->get_tree_c1()->get_root_rec().page);
    DEBUG("Tree C2 is %lld\n", (long long)ltable->get_tree_c2()->get_root_rec().page);

    struct merger_args diskmerge_args= {
        ltable, 
        0, //max_tree_size No max size for biggest component
        &R, //r_i
                };

    *mdata->diskmerge_args = diskmerge_args;

    struct merger_args memmerge_args =
        {
            ltable,
            (int64_t)(R * R * MAX_C0_SIZE),
            &R,
        };
    
    *mdata->memmerge_args = memmerge_args;

    void * (*diskmerger)(void*) = diskMergeThread;
    void * (*memmerger)(void*) = memMergeThread;

    pthread_create(&mdata->diskmerge_thread, 0, diskmerger, mdata->diskmerge_args);
    pthread_create(&mdata->memmerge_thread, 0, memmerger, mdata->memmerge_args);
    
}

template <class ITA, class ITB>
void merge_iterators(int xid, diskTreeComponent * forceMe,
                    ITA *itrA,
                    ITB *itrB,
                    logtable<datatuple> *ltable,
                    diskTreeComponent *scratch_tree,
                    mergeStats *stats,
                    bool dropDeletes);


/**
 *  Merge algorithm: Outsider's view
 *<pre>
  1: while(1)
  2:    wait for c0_mergable
  3:    begin
  4:    merge c0_mergable and c1 into c1'  # Blocks; tree must be consistent at this point
  5:    force c1'                          # Blocks
  6:    if c1' is too big      # Blocks; tree must be consistent at this point.
  7:       c1_mergable = c1'
  8:       c1 = new_empty
8.5:       delete old c1_mergeable  # Happens in other thread (not here)
  9:    else
 10:       c1 = c1'
 11:    c0_mergeable = NULL
 11.5:    delete old c0_mergeable
 12:    delete old c1
 13:    commit
  </pre>
     Merge algorithm: actual order: 1 2 3 4 5 6 12 11.5 11 [7 8 (9) 10] 13
 */
void* memMergeThread(void*arg)
{

    int xid;

    merger_args * a = (merger_args*)(arg);

    logtable<datatuple> * ltable = a->ltable;
    assert(ltable->get_tree_c1());
    
    int merge_count =0;
    mergeStats * stats = a->ltable->merge_mgr->newMergeStats(1);
    
    while(true) // 1
    {
        stats->new_merge();
        pthread_mutex_lock(&ltable->header_mut);
        int done = 0;
        // 2: wait for c0_mergable
        while(!ltable->get_tree_c0_mergeable())
        {            
            pthread_cond_signal(&ltable->c0_needed);

            if(!ltable->is_still_running()){
                done = 1;
                break;
            }
            
            DEBUG("mmt:\twaiting for block ready cond\n");
            
            pthread_cond_wait(&ltable->c0_ready, &ltable->header_mut);

            DEBUG("mmt:\tblock ready\n");
            
        }        

        if(done==1)
        {
            pthread_cond_signal(&ltable->c1_ready);  // no block is ready.  this allows the other thread to wake up, and see that we're shutting down.
            pthread_mutex_unlock(&ltable->header_mut);
            break;
        }

        stats->starting_merge();

        // 3: Begin transaction
        xid = Tbegin();

        // 4: Merge

        //create the iterators
        diskTreeComponent::iterator *itrA = ltable->get_tree_c1()->open_iterator();
        memTreeComponent<datatuple>::iterator *itrB =
            new memTreeComponent<datatuple>::iterator(ltable->get_tree_c0_mergeable());

        
        //create a new tree
        diskTreeComponent * c1_prime = new diskTreeComponent(xid,  ltable->internal_region_size, ltable->datapage_region_size, ltable->datapage_size, stats);

        pthread_mutex_unlock(&ltable->header_mut);

        //: do the merge
        DEBUG("mmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, c1_prime, itrA, itrB, ltable, c1_prime, stats, false);

        delete itrA;
        delete itrB;

        // 5: force c1'

        //force write the new tree to disk
        c1_prime->force(xid);

        merge_count++;        
        DEBUG("mmt:\tmerge_count %lld #bytes written %lld\n", stats.merge_count, stats.output_size());

        pthread_mutex_lock(&ltable->header_mut);

        // Immediately clean out c0 mergeable so that writers may continue.

        // first, we need to move the c1' into c1.

        // 12: delete old c1
        ltable->get_tree_c1()->dealloc(xid);
        delete ltable->get_tree_c1();

        // 10: c1 = c1'
        ltable->set_tree_c1(c1_prime);

        // 11.5: delete old c0_mergeable
        memTreeComponent<datatuple>::tearDownTree(ltable->get_tree_c0_mergeable());
        // 11: c0_mergeable = NULL
        ltable->set_tree_c0_mergeable(NULL);
        pthread_cond_signal(&ltable->c0_needed);

        ltable->update_persistent_header(xid);
        Tcommit(xid);

        //TODO: this is simplistic for now
        //6: if c1' is too big, signal the other merger
        double target_R = *(a->r_i);
        double new_c1_size = stats->output_size();
        assert(target_R >= MIN_R);
        bool signal_c2 = (new_c1_size / ltable->max_c0_size > target_R) ||
                (a->max_size && new_c1_size > a->max_size );
        if( signal_c2  )
        {
            DEBUG("mmt:\tsignaling C2 for merge\n");
            DEBUG("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable->max_c0_size, a->max_size, target_R);

            // XXX need to report backpressure here!
            while(ltable->get_tree_c1_mergeable()) {
                pthread_cond_wait(&ltable->c1_needed, &ltable->header_mut);
            }

            xid = Tbegin();

            // we just set c1 = c1'.  Want to move c1 -> c1 mergeable, clean out c1.

          // 7: and perhaps c1_mergeable
          ltable->set_tree_c1_mergeable(c1_prime); // c1_prime == c1.

          // 8: c1 = new empty.
          ltable->set_tree_c1(new diskTreeComponent(xid, ltable->internal_region_size, ltable->datapage_region_size, ltable->datapage_size, stats));

          pthread_cond_signal(&ltable->c1_ready);

          ltable->update_persistent_header(xid);
          Tcommit(xid);

        }

//        DEBUG("mmt:\tUpdated C1's position on disk to %lld\n",ltable->get_tree_c1()->get_root_rec().page);
        // 13

        pthread_mutex_unlock(&ltable->header_mut);

        stats->finished_merge();
//        stats->pretty_print(stdout);

        //TODO: get the freeing outside of the lock
    }

    return 0;

}


void *diskMergeThread(void*arg)
{
    int xid;

    merger_args * a = (merger_args*)(arg);

    logtable<datatuple> * ltable = a->ltable;
    assert(ltable->get_tree_c2());


    int merge_count =0;
    mergeStats * stats = a->ltable->merge_mgr->newMergeStats(2);
    
    while(true)
    {

        // 2: wait for input
        stats->new_merge();
        pthread_mutex_lock(&ltable->header_mut);
        int done = 0;
        // get a new input for merge
        while(!ltable->get_tree_c1_mergeable())
        {
            pthread_cond_signal(&ltable->c1_needed);

            if(!ltable->is_still_running()){
                done = 1;
                break;
            }
            
            DEBUG("dmt:\twaiting for block ready cond\n");
            
            pthread_cond_wait(&ltable->c1_ready, &ltable->header_mut);

            DEBUG("dmt:\tblock ready\n");
        }        
        if(done==1)
        {
            pthread_mutex_unlock(&ltable->header_mut);
            break;
        }

        stats->starting_merge();

        // 3: begin
        xid = Tbegin();

        // 4: do the merge.
        //create the iterators
        diskTreeComponent::iterator *itrA = ltable->get_tree_c2()->open_iterator();
        diskTreeComponent::iterator *itrB = ltable->get_tree_c1_mergeable()->open_iterator();

        //create a new tree
        diskTreeComponent * c2_prime = new diskTreeComponent(xid, ltable->internal_region_size, ltable->datapage_region_size, ltable->datapage_size, stats);

        pthread_mutex_unlock(&ltable->header_mut);

        //do the merge
        DEBUG("dmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, NULL, itrA, itrB, ltable, c2_prime, stats, true);

        delete itrA;
        delete itrB;

        //5: force write the new region to disk
        c2_prime->force(xid);

        // (skip 6, 7, 8, 8.5, 9))

        pthread_mutex_lock(&ltable->header_mut);
        //12
        ltable->get_tree_c2()->dealloc(xid);
        delete ltable->get_tree_c2();
        //11.5
        ltable->get_tree_c1_mergeable()->dealloc(xid);
        //11
        delete ltable->get_tree_c1_mergeable();
        ltable->set_tree_c1_mergeable(0);

        //writes complete
        //now atomically replace the old c2 with new c2
        //pthread_mutex_lock(a->block_ready_mut);

        merge_count++;        
        //update the current optimal R value
        *(a->r_i) = std::max(MIN_R, sqrt( (stats->output_size() * 1.0) / (ltable->max_c0_size) ) );
        
        DEBUG("dmt:\tmerge_count %lld\t#written bytes: %lld\n optimal r %.2f", stats.merge_count, stats.output_size(), *(a->r_i));
        // 10: C2 is never to big
        ltable->set_tree_c2(c2_prime);

        DEBUG("dmt:\tUpdated C2's position on disk to %lld\n",(long long)-1);
        // 13
        ltable->update_persistent_header(xid);
        Tcommit(xid);

        pthread_mutex_unlock(&ltable->header_mut);

        stats->finished_merge();
//        stats->pretty_print(stdout);

    }
    return 0;
}

template <class ITA, class ITB>
void merge_iterators(int xid,
                        diskTreeComponent * forceMe,
                        ITA *itrA, //iterator on c1 or c2
                        ITB *itrB, //iterator on c0 or c1, respectively
                        logtable<datatuple> *ltable,
                        diskTreeComponent *scratch_tree, mergeStats *stats,
                        bool dropDeletes  // should be true iff this is biggest component
                        )
{
    datatuple *t1 = itrA->next_callerFrees();
    stats->read_tuple_from_large_component(t1);
    datatuple *t2 = 0;

    int i = 0;

    while( (t2=itrB->next_callerFrees()) != 0)
    {
      stats->read_tuple_from_small_component(t2);

        DEBUG("tuple\t%lld: keylen %d datalen %d\n",
               ntuples, *(t2->keylen),*(t2->datalen) );

        while(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) < 0) // t1 is less than t2
        {
            //insert t1
            scratch_tree->insertTuple(xid, t1);
            i+=t1->byte_length(); if(i > 1000000) { if(forceMe) forceMe->force(xid); i = 0; }
            stats->wrote_tuple(t1);
            datatuple::freetuple(t1);
            //advance itrA
            t1 = itrA->next_callerFrees();
            stats->read_tuple_from_large_component(t1);
        }

        if(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) == 0)
        {
            datatuple *mtuple = ltable->gettuplemerger()->merge(t1,t2);
            stats->merged_tuples(mtuple, t2, t1); // this looks backwards, but is right.

            //insert merged tuple, drop deletes
            if(dropDeletes && !mtuple->isDelete()) {
              scratch_tree->insertTuple(xid, mtuple);
            }
            datatuple::freetuple(t1);
            t1 = itrA->next_callerFrees();  //advance itrA
            if(t1) {
              stats->read_tuple_from_large_component(t1);
            }
            datatuple::freetuple(mtuple);
        }
        else
        {
            //insert t2
            scratch_tree->insertTuple(xid, t2);
            // cannot free any tuples here; they may still be read through a lookup
        }

        i+= t2->byte_length(); if(i > 1000000) { if(forceMe) forceMe->force(xid); i = 0; }
        stats->wrote_tuple(t2);

        datatuple::freetuple(t2);
    }

    while(t1 != 0) {// t1 is less than t2
      scratch_tree->insertTuple(xid, t1);
      i += t1->byte_length(); if(i > 1000000) { if(forceMe) forceMe->force(xid); i = 0; }
      stats->wrote_tuple(t1);
      datatuple::freetuple(t1);

      //advance itrA
      t1 = itrA->next_callerFrees();
      stats->read_tuple_from_large_component(t1);
    }
    DEBUG("dpages: %d\tnpages: %d\tntuples: %d\n", dpages, npages, ntuples);

    scratch_tree->writes_done();
}
