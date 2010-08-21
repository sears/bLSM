
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
    
    mergedata.push_back(std::make_pair(ltable, mdata));
    return mergedata.size()-1;
    
}

merge_scheduler::~merge_scheduler()
{
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

    //initialize rb-tree
    ltable->set_tree_c0(new memTreeComponent<datatuple>::rbtree_t);

    //disk merger args
#ifdef NO_SNOWSHOVEL
    ltable->set_max_c0_size(MAX_C0_SIZE);
#else
    ltable->set_max_c0_size(MAX_C0_SIZE*2); // XXX blatant hack.
#endif
    diskTreeComponent ** block1_scratch = new diskTreeComponent*;
    *block1_scratch=0;

    DEBUG("Tree C1 is %lld\n", (long long)ltable->get_tree_c1()->get_root_rec().page);
    DEBUG("Tree C2 is %lld\n", (long long)ltable->get_tree_c2()->get_root_rec().page);

    void * (*diskmerger)(void*) = diskMergeThread;
    void * (*memmerger)(void*) = memMergeThread;

    pthread_create(&mdata->diskmerge_thread, 0, diskmerger, ltable);
    pthread_create(&mdata->memmerge_thread, 0, memmerger, ltable);
    
}

template <class ITA, class ITB>
void merge_iterators(int xid, diskTreeComponent * forceMe,
                    ITA *itrA,
                    ITB *itrB,
                    logtable<datatuple> *ltable,
                    diskTreeComponent *scratch_tree,
                    mergeStats * stats,
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

    logtable<datatuple> * ltable = (logtable<datatuple>*)arg;
    assert(ltable->get_tree_c1());
    
    int merge_count =0;
    mergeStats * stats = ltable->merge_mgr->get_merge_stats(1);
    
    while(true) // 1
    {
        rwlc_writelock(ltable->header_mut);
        ltable->merge_mgr->new_merge(1);
        int done = 0;
        // 2: wait for c0_mergable
#ifdef NO_SNOWSHOVEL
        while(!ltable->get_tree_c0_mergeable())
        {            
            pthread_cond_signal(&ltable->c0_needed);

            if(!ltable->is_still_running()){
                done = 1;
                break;
            }
            
            DEBUG("mmt:\twaiting for block ready cond\n");
            
            rwlc_cond_wait(&ltable->c0_ready, ltable->header_mut);

            DEBUG("mmt:\tblock ready\n");
            
        }
#else
        // the merge iterator will wait until c0 is big enough for us to proceed.
        if(!ltable->is_still_running()) {
          done = 1;
        }
#endif

        if(done==1)
        {
            pthread_cond_signal(&ltable->c1_ready);  // no block is ready.  this allows the other thread to wake up, and see that we're shutting down.
            rwlc_unlock(ltable->header_mut);
            break;
        }

        stats->starting_merge();

        // 3: Begin transaction
        xid = Tbegin();

        // 4: Merge

        //create the iterators
        diskTreeComponent::iterator *itrA = ltable->get_tree_c1()->open_iterator();
#ifdef NO_SNOWSHOVEL
        memTreeComponent<datatuple>::iterator *itrB =
            new memTreeComponent<datatuple>::iterator(ltable->get_tree_c0_mergeable());
#else
//        memTreeComponent<datatuple>::revalidatingIterator *itrB =
//            new memTreeComponent<datatuple>::revalidatingIterator(ltable->get_tree_c0(), &ltable->rb_mut);
//        memTreeComponent<datatuple>::batchedRevalidatingIterator *itrB =
//            new memTreeComponent<datatuple>::batchedRevalidatingIterator(ltable->get_tree_c0(), &ltable->tree_bytes, ltable->max_c0_size, &ltable->flushing, 100, &ltable->rb_mut);
#endif
        
        //create a new tree
        diskTreeComponent * c1_prime = new diskTreeComponent(xid,  ltable->internal_region_size, ltable->datapage_region_size, ltable->datapage_size, stats);

        ltable->set_tree_c1_prime(c1_prime);

        rwlc_unlock(ltable->header_mut);
#ifndef NO_SNOWSHOVEL
        // needs to be past the rwlc_unlock...
        memTreeComponent<datatuple>::batchedRevalidatingIterator *itrB =
            new memTreeComponent<datatuple>::batchedRevalidatingIterator(ltable->get_tree_c0(), &ltable->tree_bytes, ltable->max_c0_size, &ltable->flushing, 100, &ltable->rb_mut);
#endif
        //: do the merge
        DEBUG("mmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, c1_prime, itrA, itrB, ltable, c1_prime, stats, false);

        delete itrA;
        delete itrB;

        // 5: force c1'

        rwlc_writelock(ltable->header_mut);

        //force write the new tree to disk
        c1_prime->force(xid);

        merge_count++;        
        DEBUG("mmt:\tmerge_count %lld #bytes written %lld\n", stats.merge_count, stats.output_size());

        // Immediately clean out c0 mergeable so that writers may continue.

        // first, we need to move the c1' into c1.

        // 12: delete old c1
        ltable->get_tree_c1()->dealloc(xid);
        delete ltable->get_tree_c1();

        // 10: c1 = c1'
        ltable->set_tree_c1(c1_prime);
        ltable->set_tree_c1_prime(0);

#ifdef NO_SNOWSHOVEL
        // 11.5: delete old c0_mergeable
        memTreeComponent<datatuple>::tearDownTree(ltable->get_tree_c0_mergeable());
        // 11: c0_mergeable = NULL
        ltable->set_tree_c0_mergeable(NULL);
#endif
        ltable->set_c0_is_merging(false);
        double new_c1_size = stats->output_size();
        pthread_cond_signal(&ltable->c0_needed);

        ltable->update_persistent_header(xid, 1);
        Tcommit(xid);

        ltable->merge_mgr->finished_merge(1);

        //TODO: this is simplistic for now
        //6: if c1' is too big, signal the other merger

        // update c0 effective size.
        double frac = 1.0/(double)merge_count;
        ltable->num_c0_mergers = merge_count;
        ltable->mean_c0_effective_size =
          (int64_t) (
           ((double)ltable->mean_c0_effective_size)*(1-frac) +
           ((double)stats->bytes_in_small*frac));
        ltable->merge_mgr->get_merge_stats(0)->target_size = ltable->mean_c0_effective_size;
        double target_R = *ltable->R();

        printf("Merge done. R = %f MemSize = %lld Mean = %lld, This = %lld, Count = %d factor %3.3fcur%3.3favg\n", target_R, (long long)ltable->max_c0_size, (long long int)ltable->mean_c0_effective_size, stats->bytes_in_small, merge_count, ((double)stats->bytes_in_small) / (double)ltable->max_c0_size, ((double)ltable->mean_c0_effective_size) / (double)ltable->max_c0_size);

        assert(target_R >= MIN_R);
        bool signal_c2 = (new_c1_size / ltable->mean_c0_effective_size > target_R);
        DEBUG("\nc1 size %f R %f\n", new_c1_size, target_R);
        if( signal_c2  )
        {
            DEBUG("mmt:\tsignaling C2 for merge\n");
            DEBUG("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable->max_c0_size, a->max_size, target_R);

            // XXX need to report backpressure here!
            while(ltable->get_tree_c1_mergeable()) {
                rwlc_cond_wait(&ltable->c1_needed, ltable->header_mut);
            }

            xid = Tbegin();

            // we just set c1 = c1'.  Want to move c1 -> c1 mergeable, clean out c1.

          // 7: and perhaps c1_mergeable
          ltable->set_tree_c1_mergeable(ltable->get_tree_c1()); // c1_prime == c1.
          stats->handed_off_tree();

          // 8: c1 = new empty.
          ltable->set_tree_c1(new diskTreeComponent(xid, ltable->internal_region_size, ltable->datapage_region_size, ltable->datapage_size, stats));

          pthread_cond_signal(&ltable->c1_ready);
          pageid_t old_bytes_out = stats->bytes_out;
          stats->bytes_out = 0; // XXX HACK
          ltable->update_persistent_header(xid, 1);
          stats->bytes_out = old_bytes_out;
          Tcommit(xid);

        }

//        DEBUG("mmt:\tUpdated C1's position on disk to %lld\n",ltable->get_tree_c1()->get_root_rec().page);
        // 13

        rwlc_unlock(ltable->header_mut);
//        stats->pretty_print(stdout);

        //TODO: get the freeing outside of the lock
    }

    return 0;

}


void *diskMergeThread(void*arg)
{
    int xid;

    logtable<datatuple> * ltable = (logtable<datatuple>*)arg;
    assert(ltable->get_tree_c2());


    int merge_count =0;
    mergeStats * stats = ltable->merge_mgr->get_merge_stats(2);
    
    while(true)
    {

        // 2: wait for input
        rwlc_writelock(ltable->header_mut);
        ltable->merge_mgr->new_merge(2);
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
            
            rwlc_cond_wait(&ltable->c1_ready, ltable->header_mut);

            DEBUG("dmt:\tblock ready\n");
        }        
        if(done==1)
        {
            rwlc_unlock(ltable->header_mut);
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

        rwlc_unlock(ltable->header_mut);

        //do the merge
        DEBUG("dmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, c2_prime, itrA, itrB, ltable, c2_prime, stats, true);

        delete itrA;
        delete itrB;

        //5: force write the new region to disk
        c2_prime->force(xid);

        // (skip 6, 7, 8, 8.5, 9))

        rwlc_writelock(ltable->header_mut);
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
        *(ltable->R()) = std::max(MIN_R, sqrt( ((double)stats->output_size()) / ((double)ltable->mean_c0_effective_size) ) );
        
        DEBUG("\nR = %f\n", *(ltable->R()));

        DEBUG("dmt:\tmerge_count %lld\t#written bytes: %lld\n optimal r %.2f", stats.merge_count, stats.output_size(), *(a->r_i));
        // 10: C2 is never too big
        ltable->set_tree_c2(c2_prime);
        stats->handed_off_tree();

        DEBUG("dmt:\tUpdated C2's position on disk to %lld\n",(long long)-1);
        // 13
        ltable->update_persistent_header(xid, 2);
        Tcommit(xid);

        ltable->merge_mgr->finished_merge(2);

        rwlc_unlock(ltable->header_mut);
//        stats->pretty_print(stdout);

    }
    return 0;
}

static void periodically_force(int xid, int *i, diskTreeComponent * forceMe, stasis_log_t * log) {
  if(*i > mergeManager::FORCE_INTERVAL) {
    if(forceMe) forceMe->force(xid);
    log->force_tail(log, LOG_FORCE_WAL);
    *i = 0;
  }
}

static int garbage_collect(logtable<datatuple> * ltable, datatuple ** garbage, int garbage_len, int next_garbage, bool force = false) {
  if(next_garbage == garbage_len || force) {
    pthread_mutex_lock(&ltable->rb_mut);
    for(int i = 0; i < next_garbage; i++) {
      datatuple * t2tmp = NULL;
      {
      memTreeComponent<datatuple>::rbtree_t::iterator rbitr = ltable->get_tree_c0()->find(garbage[i]);
        if(rbitr != ltable->get_tree_c0()->end()) {
          t2tmp = *rbitr;
          if((t2tmp->datalen() == garbage[i]->datalen()) &&
             !memcmp(t2tmp->data(), garbage[i]->data(), garbage[i]->datalen())) {
            // they match, delete t2tmp
          } else {
            t2tmp = NULL;
          }
        }
      } // close rbitr before touching the tree.
      if(t2tmp) {
        ltable->get_tree_c0()->erase(garbage[i]);
        ltable->tree_bytes -= garbage[i]->byte_length();
        datatuple::freetuple(t2tmp);
      }
      datatuple::freetuple(garbage[i]);
    }
    pthread_mutex_unlock(&ltable->rb_mut);
    return 0;
  } else {
    return next_garbage;
  }
}

template <class ITA, class ITB>
void merge_iterators(int xid,
                        diskTreeComponent * forceMe,
                        ITA *itrA, //iterator on c1 or c2
                        ITB *itrB, //iterator on c0 or c1, respectively
                        logtable<datatuple> *ltable,
                        diskTreeComponent *scratch_tree, mergeStats * stats,
                        bool dropDeletes  // should be true iff this is biggest component
                        )
{
  stasis_log_t * log = (stasis_log_t*)stasis_log();

    datatuple *t1 = itrA->next_callerFrees();
    ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);
    datatuple *t2 = 0;

    int garbage_len = 100;
    int next_garbage = 0;
    datatuple ** garbage = (datatuple**)malloc(sizeof(garbage[0]) * garbage_len);

    int i = 0;

    while( (t2=itrB->next_callerFrees()) != 0)
    {
      ltable->merge_mgr->read_tuple_from_small_component(stats->merge_level, t2);

        DEBUG("tuple\t%lld: keylen %d datalen %d\n",
               ntuples, *(t2->keylen),*(t2->datalen) );

        while(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) < 0) // t1 is less than t2
        {
            //insert t1
            scratch_tree->insertTuple(xid, t1);
            i+=t1->byte_length();
            ltable->merge_mgr->wrote_tuple(stats->merge_level, t1);
            datatuple::freetuple(t1);
            //advance itrA
            t1 = itrA->next_callerFrees();
            if(t1) {
              ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);
            }
            periodically_force(xid, &i, forceMe, log);
        }

        if(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) == 0)
        {
            datatuple *mtuple = ltable->gettuplemerger()->merge(t1,t2);
            stats->merged_tuples(mtuple, t2, t1); // this looks backwards, but is right.

            //insert merged tuple, drop deletes
            if(dropDeletes && !mtuple->isDelete()) {
              scratch_tree->insertTuple(xid, mtuple);
              i+=mtuple->byte_length();
            }
            datatuple::freetuple(t1);
            ltable->merge_mgr->wrote_tuple(stats->merge_level, mtuple);
            t1 = itrA->next_callerFrees();  //advance itrA
            if(t1) {
              ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);
            }
            datatuple::freetuple(mtuple);
            periodically_force(xid, &i, forceMe, log);
        }
        else
        {
            //insert t2
            scratch_tree->insertTuple(xid, t2);
            i+=t2->byte_length();

            ltable->merge_mgr->wrote_tuple(stats->merge_level, t2);
            periodically_force(xid, &i, forceMe, log);
            // cannot free any tuples here; they may still be read through a lookup
        }
#ifndef NO_SNOWSHOVEL
        if(stats->merge_level == 1) {
          next_garbage = garbage_collect(ltable, garbage, garbage_len, next_garbage);
          garbage[next_garbage] = t2;
          next_garbage++;
        }
#if 0
        pthread_mutex_lock(&ltable->rb_mut);
        if(stats->merge_level == 1) {
          datatuple * t2tmp = NULL;
          {
          memTreeComponent<datatuple>::rbtree_t::iterator rbitr = ltable->get_tree_c0()->find(t2);
            if(rbitr != ltable->get_tree_c0()->end()) {
              t2tmp = *rbitr;
              if((t2tmp->datalen() == t2->datalen()) &&
                 !memcmp(t2tmp->data(), t2->data(), t2->datalen())) {
              }
            }
          }
          if(t2tmp) {
            ltable->get_tree_c0()->erase(t2);
            ltable->tree_bytes -= t2->byte_length();
            datatuple::freetuple(t2tmp);
          }
        }
        pthread_mutex_unlock(&ltable->rb_mut);
#endif
        if(stats->merge_level != 1) {
          datatuple::freetuple(t2);
        }
#else
        datatuple::freetuple(t2);
#endif

    }

    while(t1 != 0) {// t1 is less than t2
      scratch_tree->insertTuple(xid, t1);
      ltable->merge_mgr->wrote_tuple(stats->merge_level, t1);
      i += t1->byte_length();
      datatuple::freetuple(t1);

      //advance itrA
      t1 = itrA->next_callerFrees();
      ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);
      periodically_force(xid, &i, forceMe, log);
    }
    DEBUG("dpages: %d\tnpages: %d\tntuples: %d\n", dpages, npages, ntuples);

    next_garbage = garbage_collect(ltable, garbage, garbage_len, next_garbage, true);
    free(garbage);

    scratch_tree->writes_done();
}
