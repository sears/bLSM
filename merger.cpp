#include <math.h>
#include "merger.h"

#include <stasis/transactional.h>
#undef try
#undef end

static void* memMerge_thr(void* arg) {
  return ((merge_scheduler*)arg)->memMergeThread();
}
static void* diskMerge_thr(void* arg) {
  return ((merge_scheduler*)arg)->diskMergeThread();
}

merge_scheduler::merge_scheduler(logtable<datatuple> *ltable) : ltable_(ltable), MIN_R(3.0) { }
merge_scheduler::~merge_scheduler() { }

void merge_scheduler::shutdown() {
  ltable_->stop();
  pthread_join(mem_merge_thread_,  0);
  pthread_join(disk_merge_thread_, 0);
}

void merge_scheduler::start() {
  pthread_create(&mem_merge_thread_,  0, memMerge_thr,  this);
  pthread_create(&disk_merge_thread_, 0, diskMerge_thr, this);
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
void * merge_scheduler::memMergeThread() {

    int xid;

    assert(ltable_->get_tree_c1());
    
    int merge_count =0;
    mergeStats * stats = ltable_->merge_mgr->get_merge_stats(1);
    
    while(true) // 1
    {
        rwlc_writelock(ltable_->header_mut);
        ltable_->merge_mgr->new_merge(1);
        int done = 0;
        // 2: wait for c0_mergable
        // the merge iterator will wait until c0 is big enough for us to proceed.
        if(!ltable_->is_still_running()) {
          done = 1;
        }
        if(done==1)
        {
            pthread_cond_signal(&ltable_->c1_ready);  // no block is ready.  this allows the other thread to wake up, and see that we're shutting down.
            rwlc_unlock(ltable_->header_mut);
            break;
        }

        stats->starting_merge();

        // 3: Begin transaction
        xid = Tbegin();

        // 4: Merge

        //create the iterators
        diskTreeComponent::iterator *itrA = ltable_->get_tree_c1()->open_iterator();
        const int64_t min_bloom_target = ltable_->max_c0_size;

        //create a new tree
        diskTreeComponent * c1_prime = new diskTreeComponent(xid,  ltable_->internal_region_size, ltable_->datapage_region_size, ltable_->datapage_size, stats, (stats->target_size < min_bloom_target ? min_bloom_target : stats->target_size) / 100);

        ltable_->set_tree_c1_prime(c1_prime);

        rwlc_unlock(ltable_->header_mut);

        // needs to be past the rwlc_unlock...
        memTreeComponent<datatuple>::batchedRevalidatingIterator *itrB =
            new memTreeComponent<datatuple>::batchedRevalidatingIterator(ltable_->get_tree_c0(), &ltable_->tree_bytes, ltable_->max_c0_size, &ltable_->flushing, 100, &ltable_->rb_mut);

        //: do the merge
        DEBUG("mmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, c1_prime, itrA, itrB, ltable_, c1_prime, stats, false);

        delete itrA;
        delete itrB;

        // 5: force c1'

        rwlc_writelock(ltable_->header_mut);

        //force write the new tree to disk
        c1_prime->force(xid);

        merge_count++;        
        DEBUG("mmt:\tmerge_count %lld #bytes written %lld\n", stats.stats_merge_count, stats.output_size());

        // Immediately clean out c0 mergeable so that writers may continue.

        // first, we need to move the c1' into c1.

        // 12: delete old c1
        ltable_->get_tree_c1()->dealloc(xid);
        delete ltable_->get_tree_c1();

        // 10: c1 = c1'
        ltable_->set_tree_c1(c1_prime);
        ltable_->set_tree_c1_prime(0);

        ltable_->set_c0_is_merging(false);
        double new_c1_size = stats->output_size();
        pthread_cond_signal(&ltable_->c0_needed);

        ltable_->update_persistent_header(xid);
        Tcommit(xid);

        //TODO: this is simplistic for now
        //6: if c1' is too big, signal the other merger

        if(stats->bytes_in_small) {
          // update c0 effective size.
          double frac = 1.0/(double)merge_count;
          ltable_->num_c0_mergers = merge_count;
          ltable_->mean_c0_run_length=
            (int64_t) (
             ((double)ltable_->mean_c0_run_length)*(1-frac) +
             ((double)stats->bytes_in_small*frac));
          //ltable_->merge_mgr->get_merge_stats(0)->target_size = ltable_->mean_c0_run_length;
        }

        printf("Merge done. R = %f MemSize = %lld Mean = %lld, This = %lld, Count = %d factor %3.3fcur%3.3favg\n", *ltable_->R(), (long long)ltable_->max_c0_size, (long long int)ltable_->mean_c0_run_length, stats->bytes_in_small, merge_count, ((double)stats->bytes_in_small) / (double)ltable_->max_c0_size, ((double)ltable_->mean_c0_run_length) / (double)ltable_->max_c0_size);

        assert(*ltable_->R() >= MIN_R);
        bool signal_c2 = (new_c1_size / ltable_->mean_c0_run_length > *ltable_->R());
        DEBUG("\nc1 size %f R %f\n", new_c1_size, *ltable_->R());
        if( signal_c2  )
        {
            DEBUG("mmt:\tsignaling C2 for merge\n");
            DEBUG("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable_->max_c0_size, a->max_size, target_R);

            // XXX need to report backpressure here!
            while(ltable_->get_tree_c1_mergeable()) {
                rwlc_cond_wait(&ltable_->c1_needed, ltable_->header_mut);
            }

            xid = Tbegin();

            // we just set c1 = c1'.  Want to move c1 -> c1 mergeable, clean out c1.

          // 7: and perhaps c1_mergeable
          ltable_->set_tree_c1_mergeable(ltable_->get_tree_c1()); // c1_prime == c1.
          stats->handed_off_tree();

          // 8: c1 = new empty.
          ltable_->set_tree_c1(new diskTreeComponent(xid, ltable_->internal_region_size, ltable_->datapage_region_size, ltable_->datapage_size, stats));

          pthread_cond_signal(&ltable_->c1_ready);
          ltable_->update_persistent_header(xid);
          Tcommit(xid);

        }

//        DEBUG("mmt:\tUpdated C1's position on disk to %lld\n",ltable_->get_tree_c1()->get_root_rec().page);
        // 13

        rwlc_unlock(ltable_->header_mut);

        ltable_->merge_mgr->finished_merge(1);
//        stats->pretty_print(stdout);

        //TODO: get the freeing outside of the lock
    }

    return 0;

}


void * merge_scheduler::diskMergeThread()
{
    int xid;

    assert(ltable_->get_tree_c2());


    int merge_count =0;
    mergeStats * stats = ltable_->merge_mgr->get_merge_stats(2);
    
    while(true)
    {

        // 2: wait for input
        rwlc_writelock(ltable_->header_mut);
        ltable_->merge_mgr->new_merge(2);
        int done = 0;
        // get a new input for merge
        while(!ltable_->get_tree_c1_mergeable())
        {
            pthread_cond_signal(&ltable_->c1_needed);

            if(!ltable_->is_still_running()){
                done = 1;
                break;
            }
            
            DEBUG("dmt:\twaiting for block ready cond\n");
            
            rwlc_cond_wait(&ltable_->c1_ready, ltable_->header_mut);

            DEBUG("dmt:\tblock ready\n");
        }        
        if(done==1)
        {
            rwlc_unlock(ltable_->header_mut);
            break;
        }

        stats->starting_merge();

        // 3: begin
        xid = Tbegin();

        // 4: do the merge.
        //create the iterators
        diskTreeComponent::iterator *itrA = ltable_->get_tree_c2()->open_iterator();
        diskTreeComponent::iterator *itrB = ltable_->get_tree_c1_mergeable()->open_iterator(ltable_->merge_mgr, 0.05, &ltable_->shutting_down_);

        //create a new tree
        diskTreeComponent * c2_prime = new diskTreeComponent(xid, ltable_->internal_region_size, ltable_->datapage_region_size, ltable_->datapage_size, stats, (uint64_t)(ltable_->max_c0_size * *ltable_->R() + stats->base_size)/ 1000);
//        diskTreeComponent * c2_prime = new diskTreeComponent(xid, ltable_->internal_region_size, ltable_->datapage_region_size, ltable_->datapage_size, stats);

        rwlc_unlock(ltable_->header_mut);

        //do the merge
        DEBUG("dmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, c2_prime, itrA, itrB, ltable_, c2_prime, stats, true);

        delete itrA;
        delete itrB;

        //5: force write the new region to disk
        c2_prime->force(xid);

        // (skip 6, 7, 8, 8.5, 9))

        rwlc_writelock(ltable_->header_mut);
        //12
        ltable_->get_tree_c2()->dealloc(xid);
        delete ltable_->get_tree_c2();
        //11.5
        ltable_->get_tree_c1_mergeable()->dealloc(xid);
        //11
        delete ltable_->get_tree_c1_mergeable();
        ltable_->set_tree_c1_mergeable(0);

        //writes complete
        //now atomically replace the old c2 with new c2
        //pthread_mutex_lock(a->block_ready_mut);

        merge_count++;        
        //update the current optimal R value
        *(ltable_->R()) = std::max(MIN_R, sqrt( ((double)stats->output_size()) / ((double)ltable_->mean_c0_run_length) ) );
        
        DEBUG("\nR = %f\n", *(ltable_->R()));

        DEBUG("dmt:\tmerge_count %lld\t#written bytes: %lld\n optimal r %.2f", stats.stats_merge_count, stats.output_size(), *(a->r_i));
        // 10: C2 is never too big
        ltable_->set_tree_c2(c2_prime);
        stats->handed_off_tree();

        DEBUG("dmt:\tUpdated C2's position on disk to %lld\n",(long long)-1);
        // 13
        ltable_->update_persistent_header(xid);
        Tcommit(xid);

        rwlc_unlock(ltable_->header_mut);
//        stats->pretty_print(stdout);
        ltable_->merge_mgr->finished_merge(2);


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

static int garbage_collect(logtable<datatuple> * ltable_, datatuple ** garbage, int garbage_len, int next_garbage, bool force = false) {
  if(next_garbage == garbage_len || force) {
    pthread_mutex_lock(&ltable_->rb_mut);
    for(int i = 0; i < next_garbage; i++) {
      datatuple * t2tmp = NULL;
      {
      memTreeComponent<datatuple>::rbtree_t::iterator rbitr = ltable_->get_tree_c0()->find(garbage[i]);
        if(rbitr != ltable_->get_tree_c0()->end()) {
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
        ltable_->get_tree_c0()->erase(garbage[i]);
        ltable_->tree_bytes -= garbage[i]->byte_length();
        datatuple::freetuple(t2tmp);
      }
      datatuple::freetuple(garbage[i]);
    }
    pthread_mutex_unlock(&ltable_->rb_mut);
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
            ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);

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
            ltable->merge_mgr->read_tuple_from_large_component(stats->merge_level, t1);
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
        if(stats->merge_level == 1) {
          next_garbage = garbage_collect(ltable, garbage, garbage_len, next_garbage);
          garbage[next_garbage] = t2;
          next_garbage++;
        }
        if(stats->merge_level != 1) {
          datatuple::freetuple(t2);
        }

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
