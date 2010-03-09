
#include <math.h>
#include "merger.h"
#include "logiterators.cpp"
#include "datapage.h"

typedef struct merge_stats_t {
	int merge_level;               // 1 => C0->C1, 2 => C1->C2
	pageid_t merge_count;          // This is the merge_count'th merge
	struct timeval sleep;          // When did we go to sleep waiting for input?
	struct timeval start;          // When did we wake up and start merging?  (at steady state with max throughput, this should be equal to sleep)
	struct timeval done;           // When did we finish merging?
	pageid_t bytes_out;            // How many bytes did we write (including internal tree nodes)?
	pageid_t num_tuples_out;       // How many tuples did we write?
	pageid_t num_datapages_out;    // How many datapages?
	pageid_t bytes_in_small;       // How many bytes from the small input tree (for C0, we ignore tree overheads)?
	pageid_t num_tuples_in_small;  // Tuples from the small input?
	pageid_t bytes_in_large;       // Bytes from the large input?
	pageid_t num_tuples_in_large;  // Tuples from large input?
} merge_stats_t;

void merge_stats_pp(FILE* fd, merge_stats_t &stats) {
	long long sleep_time = stats.start.tv_sec - stats.sleep.tv_sec;
	long long work_time =  stats.done.tv_sec - stats.start.tv_sec;
	long long total_time = sleep_time + work_time;
	double mb_out = ((double)stats.bytes_out)     /(1024.0*1024.0);
	double mb_ins=  ((double)stats.bytes_in_small)     /(1024.0*1024.0);
	double mb_inl = ((double)stats.bytes_in_large)     /(1024.0*1024.0);
	double kt_out = ((double)stats.num_tuples_out)     /(1024.0);
	double kt_ins=  ((double)stats.num_tuples_in_small)     /(1024.0);
	double kt_inl = ((double)stats.num_tuples_in_large)     /(1024.0);
	double mb_hdd = mb_out + mb_inl + (stats.merge_level == 1 ? 0.0 : mb_ins);
	double kt_hdd = kt_out + kt_inl + (stats.merge_level == 1 ? 0.0 : kt_ins);


	fprintf(fd,
			    "=====================================================================\n"
                "Thread %d merge %lld: sleep %lld sec, run %lld sec\n"
				"           megabytes kTuples datapages   MB/s (real)   kTup/s  (real)\n"
			    "Wrote        %7lld %7lld %9lld"     " %6.1f %6.1f" " %8.1f %8.1f"   "\n"
				"Read (small) %7lld %7lld      -   " " %6.1f %6.1f" " %8.1f %8.1f"   "\n"
			    "Read (large) %7lld %7lld      -   " " %6.1f %6.1f" " %8.1f %8.1f"   "\n"
			    "Disk         %7lld %7lld      -   " " %6.1f %6.1f" " %8.1f %8.1f"   "\n"
		        ".....................................................................\n"
			    "avg tuple len: %6.2fkb\n"
				"effective throughput: (mb/s ; nsec/byte): (%.2f; %.2f) active"      "\n"
			    "                                          (%.2f; %.2f) wallclock"   "\n"
				".....................................................................\n"
			,
			    stats.merge_level, stats.merge_count,
			    sleep_time,
			    work_time,
			    (long long)mb_out, (long long)kt_out, stats.num_datapages_out, mb_out / (double)work_time, mb_out / (double)total_time, kt_out / (double)work_time,  kt_out / (double)total_time,
			    (long long)mb_ins, (long long)kt_ins,                          mb_ins / (double)work_time, mb_ins / (double)total_time, kt_ins / (double)work_time,  kt_ins / (double)total_time,
			    (long long)mb_inl, (long long)kt_inl,                          mb_inl / (double)work_time, mb_inl / (double)total_time, kt_inl / (double)work_time,  kt_inl / (double)total_time,
			    (long long)mb_hdd, (long long)kt_hdd,                          mb_hdd / (double)work_time, mb_hdd / (double)total_time, kt_hdd / (double)work_time,  kt_hdd / (double)total_time,
			    mb_out / kt_out,
			    mb_ins / work_time, 1000.0 * work_time / mb_ins, mb_ins / total_time, 1000.0 * total_time / mb_ins
				);
}

double merge_stats_nsec_to_merge_in_bytes(merge_stats_t); // how many nsec did we burn on each byte from the small tree (want this to be equal for the two mergers)

inline DataPage<datatuple>*
insertTuple(int xid, DataPage<datatuple> *dp, datatuple *t,
            logtable *ltable,
            diskTreeComponent::internalNodes * ltree, merge_stats_t*);

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
    ltable->set_tree_c0(new memTreeComponent<datatuple>::rbtree_t);

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

template <class ITA, class ITB>
void merge_iterators(int xid,
                    ITA *itrA,
                    ITB *itrB,
                    logtable *ltable,
                    diskTreeComponent::internalNodes *scratch_tree,
                    merge_stats_t *stats,
                    bool dropDeletes);


/**
 *  Merge algorithm: Outsider's view
 *<pre>
  1: while(1)
  2:    wait for c0_mergable
  3:    begin
  4:    merge c0_mergable and c1 into c1'  # Blocks; tree must be consistent at this point
  5:    force c1'		           # Blocks
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

    logtable * ltable = a->ltable;
    assert(ltable->get_tree_c1());
    
    int merge_count =0;
    
    while(true) // 1
    {
    	merge_stats_t stats;
    	stats.merge_level = 1;
    	stats.merge_count = merge_count;
    	gettimeofday(&stats.sleep,0);
        writelock(ltable->header_lock,0);
        int done = 0;
        // 2: wait for c0_mergable
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
            
            DEBUG("mmt:\twaiting for block ready cond\n");
            unlock(ltable->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);
            
            writelock(ltable->header_lock,0);
            DEBUG("mmt:\tblock ready\n");
            
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

    	gettimeofday(&stats.start, 0);

        // 3: Begin transaction
        xid = Tbegin();

        // 4: Merge

        //create the iterators
        diskTreeIterator<datatuple> *itrA = new diskTreeIterator<datatuple>(ltable->get_tree_c1()->get_root_rec()); // XXX don't want get_root_rec() to be here.
        memTreeComponent<datatuple>::iterator *itrB =
            new memTreeComponent<datatuple>::iterator(ltable->get_tree_c0_mergeable());

        
        //create a new tree
        diskTreeComponent::internalNodes * c1_prime = new diskTreeComponent::internalNodes(xid); // XXX should not hardcode region size)

        //pthread_mutex_unlock(a->block_ready_mut);
        unlock(ltable->header_lock);

        //: do the merge
        DEBUG("mmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, c1_prime, &stats, false);

        delete itrA;
        delete itrB;

        // 5: force c1'

        //force write the new region to disk
        diskTreeComponent::internalNodes::force_region_rid(xid, c1_prime->get_tree_state());
        //force write the new datapages
        c1_prime->get_alloc()->force_regions(xid);

        merge_count++;        
        DEBUG("mmt:\tmerge_count %lld #bytes written %lld\n", stats.merge_count, stats.bytes_out);

        writelock(ltable->header_lock,0);


        //TODO: this is simplistic for now
        //6: if c1' is too big, signal the other merger
        double target_R = *(a->r_i);
        double new_c1_size = stats.bytes_out;
        assert(target_R >= MIN_R);
        bool signal_c2 = (new_c1_size / ltable->max_c0_size > target_R) ||
                (a->max_size && new_c1_size > a->max_size );
        if( signal_c2  )
        {
        	DEBUG("mmt:\tsignaling C2 for merge\n");
            DEBUG("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable->max_c0_size, a->max_size, target_R);

            // XXX need to report backpressure here!  Also, shouldn't be inside a transaction while waiting on backpressure.
            while(ltable->get_tree_c1_mergeable()) {
                pthread_mutex_lock(a->block_ready_mut);
                unlock(ltable->header_lock);
                
                pthread_cond_wait(a->out_block_needed_cond, a->block_ready_mut);
                pthread_mutex_unlock(a->block_ready_mut);
                writelock(ltable->header_lock,0);
            }
        }

        // 12: delete old c1
        diskTreeComponent::internalNodes::dealloc_region_rid(xid, ltable->get_tree_c1()->get_tree_state());
        ltable->get_tree_c1()->get_alloc()->dealloc_regions(xid);
        delete ltable->get_tree_c1();

        // 11.5: delete old c0_mergeable
        memTreeComponent<datatuple>::tearDownTree(ltable->get_tree_c0_mergeable());
        // 11: c0_mergeable = NULL
        ltable->set_tree_c0_mergeable(NULL);

        if( signal_c2 ) {

        	// 7: and perhaps c1_mergeable
			ltable->set_tree_c1_mergeable(c1_prime);

			// 8: c1 = new empty.
            ltable->set_tree_c1(new diskTreeComponent::internalNodes(xid));

            pthread_cond_signal(a->out_block_ready_cond);

        } else {
        	// 10: c1 = c1'
        	ltable->set_tree_c1(c1_prime);
        }

        DEBUG("mmt:\tUpdated C1's position on disk to %lld\n",ltable->get_tree_c1()->get_root_rec().page);
        // 13
        ltable->update_persistent_header(xid);
        Tcommit(xid);

        unlock(ltable->header_lock);
        
        gettimeofday(&stats.done, 0);
        merge_stats_pp(stdout, stats);

        //TODO: get the freeing outside of the lock
    }

    return 0;

}


void *diskMergeThread(void*arg)
{
    int xid;// = Tbegin();

    merger_args * a = (merger_args*)(arg);

    logtable * ltable = a->ltable;
    assert(ltable->get_tree_c2());


    int merge_count =0;
    
    while(true)
    {
    	merge_stats_t stats;
    	stats.merge_level = 2;
    	stats.merge_count = merge_count;
    	gettimeofday(&stats.sleep,0);
    	// 2: wait for input
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
            
            DEBUG("dmt:\twaiting for block ready cond\n");
            unlock(ltable->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);

            DEBUG("dmt:\tblock ready\n");
            writelock(ltable->header_lock,0);
        }        
        *a->in_block_needed = false;
        if(done==1)
        {
            pthread_cond_signal(a->out_block_ready_cond);
            unlock(ltable->header_lock);
            break;
        }

    	gettimeofday(&stats.start, 0);

    	// 3: begin
        xid = Tbegin();

        // 4: do the merge.
        //create the iterators
        diskTreeIterator<datatuple> *itrA = new diskTreeIterator<datatuple>(ltable->get_tree_c2()->get_root_rec());
        diskTreeIterator<datatuple> *itrB =
            new diskTreeIterator<datatuple>(ltable->get_tree_c1_mergeable()->get_root_rec());

        //create a new tree
        //TODO: maybe you want larger regions for the second tree?
        diskTreeComponent::internalNodes * c2_prime = new diskTreeComponent::internalNodes(xid);

        unlock(ltable->header_lock);
        
        //do the merge        
        DEBUG("dmt:\tMerging:\n");

        merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, c2_prime, &stats, true);
      
        delete itrA;
        delete itrB;        

        //5: force write the new region to disk
        diskTreeComponent::internalNodes::force_region_rid(xid, c2_prime->get_tree_state());
        c2_prime->get_alloc()->force_regions(xid);

        // (skip 6, 7, 8, 8.5, 9))

        writelock(ltable->header_lock,0);
        //12
        diskTreeComponent::internalNodes::dealloc_region_rid(xid, ltable->get_tree_c2()->get_tree_state());
        ltable->get_tree_c2()->get_alloc()->dealloc_regions(xid);
        delete ltable->get_tree_c2();
        //11.5
        diskTreeComponent::internalNodes::dealloc_region_rid(xid, ltable->get_tree_c1_mergeable()->get_tree_state());
        ltable->get_tree_c1_mergeable()->get_alloc()->dealloc_regions(xid);
        //11
        delete ltable->get_tree_c1_mergeable();
        ltable->set_tree_c1_mergeable(0);

        //writes complete
        //now atomically replace the old c2 with new c2
        //pthread_mutex_lock(a->block_ready_mut);

        merge_count++;        
        //update the current optimal R value
        *(a->r_i) = std::max(MIN_R, sqrt( (stats.bytes_out * 1.0) / (ltable->max_c0_size) ) );
        
        DEBUG("dmt:\tmerge_count %lld\t#written bytes: %lld\n optimal r %.2f", stats.merge_count, stats.bytes_out, *(a->r_i));
        // 10: C2 is never to big
        ltable->set_tree_c2(c2_prime);

        DEBUG("dmt:\tUpdated C2's position on disk to %lld\n",(long long)-1);
        // 13
        ltable->update_persistent_header(xid);
        Tcommit(xid);
        
        unlock(ltable->header_lock);

        gettimeofday(&stats.done, 0);
        merge_stats_pp(stdout, stats);

    }
    return 0;
}



template <class ITA, class ITB>
void merge_iterators(int xid,
                        ITA *itrA, //iterator on c1 or c2
                        ITB *itrB, //iterator on c0 or c1, respectively
                        logtable *ltable,
                        diskTreeComponent::internalNodes *scratch_tree, merge_stats_t *stats,
                        bool dropDeletes  // should be true iff this is biggest component
                        )
{
	stats->bytes_out = 0;
	stats->num_tuples_out = 0;
	stats->bytes_in_small = 0;
	stats->num_tuples_in_small = 0;
	stats->bytes_in_large = 0;
	stats->num_tuples_in_large = 0;

    DataPage<datatuple> *dp = 0;

    datatuple *t1 = itrA->next_callerFrees();
    if(t1) {
		stats->num_tuples_in_large++;
		stats->bytes_in_large += t1->byte_length();
    }
    datatuple *t2 = 0;
    
    while( (t2=itrB->next_callerFrees()) != 0)
    {        
        stats->num_tuples_in_small++;
        stats->bytes_in_small += t2->byte_length();

        DEBUG("tuple\t%lld: keylen %d datalen %d\n",
               ntuples, *(t2->keylen),*(t2->datalen) );        

        while(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) < 0) // t1 is less than t2
        {
            //insert t1
            dp = insertTuple(xid, dp, t1, ltable, scratch_tree, stats);

            datatuple::freetuple(t1);
            stats->num_tuples_out++;
            //advance itrA
            t1 = itrA->next_callerFrees();
            if(t1) {
            	stats->num_tuples_in_large++;
            	stats->bytes_in_large += t1->byte_length();
            }
        }

        if(t1 != 0 && datatuple::compare(t1->key(), t1->keylen(), t2->key(), t2->keylen()) == 0)
        {
            datatuple *mtuple = ltable->gettuplemerger()->merge(t1,t2);
            
            //insert merged tuple, drop deletes
            if(dropDeletes && !mtuple->isDelete())
                dp = insertTuple(xid, dp, mtuple, ltable, scratch_tree, stats);
            
            datatuple::freetuple(t1);
            t1 = itrA->next_callerFrees();  //advance itrA
            if(t1) {
            	stats->num_tuples_in_large++;
            	stats->bytes_in_large += t1->byte_length();
            }
            datatuple::freetuple(mtuple);
        }
        else
        {        
            //insert t2
            dp = insertTuple(xid, dp, t2, ltable, scratch_tree, stats);
            // cannot free any tuples here; they may still be read through a lookup
        }

        datatuple::freetuple(t2);
        stats->num_tuples_out++;
    }

    while(t1 != 0) // t1 is less than t2
        {
            dp = insertTuple(xid, dp, t1, ltable, scratch_tree, stats);

            datatuple::freetuple(t1);
            stats->num_tuples_out++;
            //advance itrA
            t1 = itrA->next_callerFrees();
            if(t1) {
            	stats->num_tuples_in_large++;
            	stats->bytes_in_large += t1->byte_length();
            }
        }

    if(dp!=NULL)
        delete dp;
    DEBUG("dpages: %d\tnpages: %d\tntuples: %d\n", dpages, npages, ntuples);

}


inline DataPage<datatuple>*
insertTuple(int xid, DataPage<datatuple> *dp, datatuple *t,
            logtable *ltable,
            diskTreeComponent::internalNodes * ltree, merge_stats_t * stats)
{
    if(dp==0)
    {
        dp = ltable->insertTuple(xid, t, ltree);
        stats->num_datapages_out++;
    }
    else if(!dp->append(t))
    {
    	stats->bytes_out += (PAGE_SIZE * dp->get_page_count());
        delete dp;
        dp = ltable->insertTuple(xid, t, ltree);
        stats->num_datapages_out++;
    }

    return dp;    
}

