
#include <math.h>
#include "merger.h"
#include "logiterators.cpp"
#include "datapage.h"

inline DataPage<datatuple>*
insertTuple(int xid, DataPage<datatuple> *dp, datatuple *t,
            logtable *ltable,
            logtree * ltree,
            int64_t &dpages, int64_t &npages);

int merge_scheduler::addlogtable(logtable *ltable)
{

    struct logtable_mergedata * mdata = new logtable_mergedata;

    // initialize merge data
    mdata->header_lock = initlock();
    mdata->rbtree_mut = new pthread_mutex_t;
    pthread_mutex_init(mdata->rbtree_mut,0);
    mdata->old_c0 = new rbtree_ptr_t;
    *mdata->old_c0 = 0;
    
    mdata->input_needed = new bool(false);
    
    mdata->input_ready_cond = new pthread_cond_t;
    pthread_cond_init(mdata->input_ready_cond,0);
    
    mdata->input_needed_cond = new pthread_cond_t;
    pthread_cond_init(mdata->input_needed_cond,0);

    mdata->input_size = new int64_t(100);

    mdata->diskmerge_args = new merger_args<logtree>;
    mdata->memmerge_args = new merger_args<rbtree_t>;
    
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
        deletelock(mdata->header_lock);
        delete mdata->rbtree_mut;        
        delete mdata->old_c0;
        delete mdata->input_needed;
        delete mdata->input_ready_cond;
        delete mdata->input_needed_cond;
        delete mdata->input_size;

        //delete the merge thread structure variables
        delete (recordid*) mdata->memmerge_args->pageAllocState;
        delete (recordid*) mdata->memmerge_args->oldAllocState;
        delete mdata->memmerge_args->still_open;

        delete (recordid*) mdata->diskmerge_args->pageAllocState;
        delete (recordid*) mdata->diskmerge_args->oldAllocState;

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

        delete mdata->diskmerge_args->my_tree_size;
        
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
        *(mdata->memmerge_args->still_open)=false;
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
    bool *system_running = new bool(true);
    
    //wait to merge the next block until we have merged block FUDGE times.
    static const int FUDGE = 1;
    static double R = MIN_R;
    int64_t * block1_size = new int64_t;
    *block1_size = FUDGE * ((int)R) * (*(mdata->input_size));

    //initialize rb-tree
    ltable->set_tree_c0(new rbtree_t);

    //disk merger args
    recordid * ridp = new recordid;
    *ridp = ltable->get_tree_c2()->get_tree_state(); //h.bigTreeAllocState;
    recordid * oldridp = new recordid;
    *oldridp = NULLRID;

    ltable->max_c0_size = MAX_C0_SIZE;

    logtree ** block1_scratch = new logtree*;
    *block1_scratch=0;

    //recordid * allocer_scratch = new recordid;
    
    struct merger_args<logtree> diskmerge_args= {
        ltable, 
            1,  //worker id 
            logtree::alloc_region_rid, //pageAlloc
                ridp,   // pageAllocState
            oldridp, // oldAllocState
            mdata->rbtree_mut, //block_ready_mutex
            block1_needed_cond, //in_block_needed_cond
            block1_needed,      //in_block_needed
            block2_needed_cond, //out_block_needed_cond
            block2_needed,      //out_block_needed
            block1_ready_cond,  //in_block_ready_cond
            block2_ready_cond,  //out_block_ready_cond
        system_running,    //still_open i.e. system running
            block1_size, //mytree_size ?
            0, //out_tree_size,  biggest component computes its size directly.
        0, //max_tree_size No max size for biggest component
        &R, //r_i
            block1_scratch, //in-tree
            0, //out_tree
            ltable->get_tree_c2()->get_root_rec(), // my_tree
            ltable->get_table_rec() //tree
                };

    *mdata->diskmerge_args = diskmerge_args;

    DEBUG("Tree C2 is %lld\n", (long long)ltable->get_tree_c2()->get_root_rec().page);


    //memory merger args
    ridp = new recordid;
    *ridp = ltable->get_tree_c1()->get_tree_state(); 
    oldridp = new recordid;
    *oldridp = NULLRID;

    DEBUG("Tree C1 is %lld\n", (long long)ltable->get_tree_c1()->get_root_rec().page);

    struct merger_args<rbtree_t> memmerge_args =
        {
            ltable,
            2,
            logtree::alloc_region_rid, //pageAlloc
            ridp,   // pageAllocState
            oldridp, // oldAllocState
            mdata->rbtree_mut, //block_ready_mutex
            mdata->input_needed_cond,
            mdata->input_needed,
            block1_needed_cond,
            block1_needed,
            mdata->input_ready_cond,
            block1_ready_cond,
            system_running,
            mdata->input_size,
            block1_size,
            (int64_t)(R * R * MAX_C0_SIZE),
            &R,
            mdata->old_c0,
            block1_scratch,
            ltable->get_tree_c1()->get_root_rec(),
            ltable->get_table_rec() //tree
        };
    
    *mdata->memmerge_args = memmerge_args;

    void * (*diskmerger)(void*) = diskMergeThread;
    void * (*memmerger)(void*) = memMergeThread;

    pthread_create(&mdata->diskmerge_thread, 0, diskmerger, mdata->diskmerge_args);
    pthread_create(&mdata->memmerge_thread, 0, memmerger, mdata->memmerge_args);
    
}

//      deallocate/free their region
//      create new data region for new data pages
void* memMergeThread(void*arg)
{

    int xid;// = Tbegin();

    merger_args<rbtree_t> * a = (merger_args<rbtree_t>*)(arg);    
    assert(a->my_tree.size != -1);

    logtable * ltable = a->ltable;
    
    int merge_count =0;
//    pthread_mutex_lock(a->block_ready_mut);
    
    while(true)
    {
        writelock(ltable->mergedata->header_lock,0);
        int done = 0;
        // get a new input for merge
        while(!*(a->in_tree))
        {            
            pthread_mutex_lock(a->block_ready_mut);
            *a->in_block_needed = true;
            //pthread_cond_signal(a->in_block_needed_cond);
            pthread_cond_broadcast(a->in_block_needed_cond);

            if(!*(a->still_open)){
                done = 1;
                pthread_mutex_unlock(a->block_ready_mut);
                break;
            }
            
            printf("mmt:\twaiting for block ready cond\n");
            unlock(ltable->mergedata->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);
            
            writelock(ltable->mergedata->header_lock,0);
            printf("mmt:\tblock ready\n");
            
        }        
        *a->in_block_needed = false;

        if(done==1)
        {
            pthread_mutex_lock(a->block_ready_mut);
            pthread_cond_signal(a->out_block_ready_cond);
            pthread_mutex_unlock(a->block_ready_mut);
            unlock(ltable->mergedata->header_lock);
            break;
        }

        if((*a->in_tree)->size()==0) //input empty, this can only happen during shutdown
        {
            delete *a->in_tree;
            *a->in_tree = 0;
            unlock(ltable->mergedata->header_lock);
            continue;
        }
      
        int64_t mergedPages=0;
        
        assert(a->my_tree.size != -1);
        
        //create the iterators
        treeIterator<datatuple> *itrA = new treeIterator<datatuple>(a->my_tree);
        memTreeIterator<rbtree_t, datatuple> *itrB =
        		new memTreeIterator<rbtree_t, datatuple>(*a->in_tree);

        //Tcommit(xid);
        xid = Tbegin();
        
        //create a new tree
        logtree * scratch_tree = new logtree(new DataPage<datatuple>::RegionAllocator(xid, ltable->get_dpstate1() /*rid of old header*/, 10000)); // XXX should not hardcode region size)
        recordid scratch_root = scratch_tree->create(xid);

        //save the old dp state values
		DataPage<datatuple>::RegionAllocator *old_alloc = ltable->get_tree_c1()->get_alloc();
		old_alloc->done(); // XXX do this earlier

        //pthread_mutex_unlock(a->block_ready_mut);
        unlock(ltable->mergedata->header_lock);

        //: do the merge
        printf("mmt:\tMerging:\n");

        int64_t npages = 0;
        mergedPages = merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, scratch_tree, npages, false);

        delete itrA;
        delete itrB;

        //force write the new region to disk
        recordid scratch_alloc_state = scratch_tree->get_tree_state();        
        logtree::force_region_rid(xid, &scratch_alloc_state);
        //force write the new datapages
        scratch_tree->get_alloc()->force_regions(xid);

        //writes complete
        //now atomically replace the old c1 with new c1
        //pthread_mutex_lock(a->block_ready_mut);

        writelock(ltable->mergedata->header_lock,0);
        merge_count++;        
        *a->my_tree_size = mergedPages;      
        printf("mmt:\tmerge_count %d #pages written %lld\n", merge_count, npages);      

        delete ltable->get_tree_c1();
        ltable->set_tree_c1(scratch_tree);

        logtable::table_header h;
        void * oldAllocState = a->pageAllocState;
        Tread(xid, a->tree, &h);
        
        h.c1_root = scratch_root;
        h.c1_state = scratch_alloc_state;
        //note we already updated the dpstate before the merge
        printf("mmt:\tUpdated C1's position on disk to %lld\n",scratch_root.page);      
        Tset(xid, a->tree, &h);
        
        //Tcommit(xid);
        //xid = Tbegin();
        
        // free old my_tree here
        //TODO: check
        logtree::free_region_rid(xid, a->my_tree, logtree::dealloc_region_rid, oldAllocState);
        
        //free the old data pages
        old_alloc->dealloc_regions(xid);

        Tcommit(xid);
        //xid = Tbegin();

        
        //TODO: this is simplistic for now
        //signal the other merger if necessary
        double target_R = *(a->r_i);
        double new_c1_size = npages * PAGE_SIZE;
        assert(target_R >= MIN_R);
        if( (new_c1_size / ltable->max_c0_size > target_R) ||
            (a->max_size && new_c1_size > a->max_size ) )
        {
            printf("mmt:\tsignaling C2 for merge\n");
            printf("mmt:\tnew_c1_size %.2f\tMAX_C0_SIZE %lld\ta->max_size %lld\t targetr %.2f \n", new_c1_size,
                   ltable->max_c0_size, a->max_size, target_R);
            
            // XXX need to report backpressure here!
            while(*a->out_tree) {
                pthread_mutex_lock(a->block_ready_mut);
                unlock(ltable->mergedata->header_lock);
                
                pthread_cond_wait(a->out_block_needed_cond, a->block_ready_mut);
                pthread_mutex_unlock(a->block_ready_mut);
                writelock(ltable->mergedata->header_lock,0);
            }


            *a->out_tree = scratch_tree;
            xid = Tbegin();

            pthread_cond_signal(a->out_block_ready_cond);


            logtree *empty_tree = new logtree(new DataPage<datatuple>::RegionAllocator(xid, ltable->get_dpstate1() /*rid of old header*/, 10000)); // XXX should not hardcode region size);
            empty_tree->create(xid);
            
            *(recordid*)(a->pageAllocState) = empty_tree->get_tree_state();

            a->my_tree = empty_tree->get_root_rec();

            ltable->set_tree_c1(empty_tree);

            logtable::table_header h;
            Tread(xid, a->tree, &h);            
            h.c1_root = empty_tree->get_root_rec(); //update root
            h.c1_state = empty_tree->get_tree_state(); //update index alloc state
            printf("mmt:\tUpdated C1's position on disk to %lld\n",empty_tree->get_root_rec().page);      
            Tset(xid, a->tree, &h);
            Tcommit(xid);
            //xid = Tbegin();

        }
        else //not signaling the C2 for merge yet
        {
            printf("mmt:\tnot signaling C2 for merge\n");
            *(recordid*)a->pageAllocState = scratch_alloc_state;      
            a->my_tree = scratch_root;
        }

        rbtree_ptr_t deltree = *a->in_tree;
        *a->in_tree = 0;

        
        //Tcommit(xid);
        unlock(ltable->mergedata->header_lock);
        
        //TODO: get the freeing outside of the lock
        //// ----------- Free in_tree
        logtable::tearDownTree(deltree);
        //deltree = 0;       


    }

    //pthread_mutex_unlock(a->block_ready_mut);
    
    return 0;

}


void *diskMergeThread(void*arg)
{
    int xid;// = Tbegin();

    merger_args<logtree> * a = (merger_args<logtree>*)(arg);    
    assert(a->my_tree.size != -1);

    logtable * ltable = a->ltable;
    
    int merge_count =0;
    //pthread_mutex_lock(a->block_ready_mut);
    
    while(true)
    {
        writelock(ltable->mergedata->header_lock,0);
        int done = 0;
        // get a new input for merge
        while(!*(a->in_tree))
        {
            pthread_mutex_lock(a->block_ready_mut);
            *a->in_block_needed = true;
            pthread_cond_signal(a->in_block_needed_cond);

            if(!*(a->still_open)){
                done = 1;
                pthread_mutex_unlock(a->block_ready_mut);
                break;
            }
            
            printf("dmt:\twaiting for block ready cond\n");
            unlock(ltable->mergedata->header_lock);
            
            pthread_cond_wait(a->in_block_ready_cond, a->block_ready_mut);
            pthread_mutex_unlock(a->block_ready_mut);

            printf("dmt:\tblock ready\n");
            writelock(ltable->mergedata->header_lock,0);
        }        
        *a->in_block_needed = false;
        if(done==1)
        {
            pthread_cond_signal(a->out_block_ready_cond);
            unlock(ltable->mergedata->header_lock);
            break;
        }
        
        int64_t mergedPages=0;
        
        assert(a->my_tree.size != -1);
        
        //create the iterators
        treeIterator<datatuple> *itrA = new treeIterator<datatuple>(a->my_tree);
        treeIterator<datatuple> *itrB =
            new treeIterator<datatuple>((*a->in_tree)->get_root_rec());
        
        xid = Tbegin();
        
        //create a new tree
        //TODO: maybe you want larger regions for the second tree?
        logtree * scratch_tree = new logtree(new DataPage<datatuple>::RegionAllocator(xid, ltable->get_dpstate2() /*rid of old header*/, 10000)); // XXX should not hardcode region size
        recordid scratch_root = scratch_tree->create(xid);

        //save the old dp state values
        DataPage<datatuple>::RegionAllocator *old_alloc1 = ltable->get_tree_c1()->get_alloc();
        DataPage<datatuple>::RegionAllocator *old_alloc2 = ltable->get_tree_c2()->get_alloc();

        unlock(ltable->mergedata->header_lock);
        
        
        //do the merge        
        printf("dmt:\tMerging:\n");

        int64_t npages = 0;
        mergedPages = merge_iterators<typeof(*itrA),typeof(*itrB)>(xid, itrA, itrB, ltable, scratch_tree, npages, true);
      
        delete itrA;
        delete itrB;        
        
        //force write the new region to disk
        recordid scratch_alloc_state = scratch_tree->get_tree_state();
        logtree::force_region_rid(xid, &scratch_alloc_state);
        //force write the new datapages
        scratch_tree->get_alloc()->force_regions(xid);

        //writes complete
        //now atomically replace the old c2 with new c2
        //pthread_mutex_lock(a->block_ready_mut);
        writelock(ltable->mergedata->header_lock,0);
        
        merge_count++;        
        *a->my_tree_size = mergedPages;
        //update the current optimal R value
        *(a->r_i) = std::max(MIN_R, sqrt( (npages * 1.0) / (ltable->max_c0_size/PAGE_SIZE) ) );
        
        printf("dmt:\tmerge_count %d\t#written pages: %lld\n optimal r %.2f", merge_count, npages, *(a->r_i));

        delete ltable->get_tree_c2();
        ltable->set_tree_c2(scratch_tree);

        logtable::table_header h;
        void * oldAllocState = a->pageAllocState;
        Tread(xid, a->tree, &h);
        
        h.c2_root = scratch_root;
        h.c2_state = scratch_alloc_state;
        //note we already updated the dpstate before the merge
        printf("dmt:\tUpdated C2's position on disk to %lld\n",scratch_root.page);
        Tset(xid, a->tree, &h);
        
        // free old my_tree here
        //TODO: check
        logtree::free_region_rid(xid, a->my_tree, logtree::dealloc_region_rid, oldAllocState);
        //TlsmFree(xid,a->my_tree->r_,logtree::dealloc_region_rid,oldAllocState);
        
        //free the old data pages
        old_alloc2->dealloc_regions(xid);
        
        *(recordid*)a->pageAllocState = scratch_alloc_state;      
        a->my_tree = scratch_root;
        
        //// ----------- Free in_tree
        //TODO: check
        logtree::free_region_rid(xid, (*a->in_tree)->get_root_rec(),
                                 logtree::dealloc_region_rid,
                                 &((*a->in_tree)->get_tree_state()));
        old_alloc1->dealloc_regions(xid);  // XXX make sure that both of these are 'unlinked' before this happens
        //TlsmFree(xid,a->my_tree->r_,logtree::dealloc_region_rid,oldAllocState);
        
        //TODO: check
        //free the old data pages
//        DataPage<datatuple>::dealloc_region_rid(xid, a->in_tree_allocer);//TODO:

        Tcommit(xid);
        
        //xid = Tbegin();
        //Tcommit(xid);
        delete *a->in_tree;        
        *a->in_tree = 0;

        unlock(ltable->mergedata->header_lock);
        
    }

    //pthread_mutex_unlock(a->block_ready_mut);
    
    return 0;


}

template <class ITA, class ITB>
int64_t merge_iterators(int xid,
                        ITA *itrA, //iterator on c1 or c2
                        ITB *itrB, //iterator on c0 or c1, respectively
                        logtable *ltable,
                        logtree *scratch_tree,
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

        while(t1 != 0 && datatuple::compare(t1->key(), t2->key()) < 0) // t1 is less than t2
        {
            //insert t1
            dp = insertTuple(xid, dp, t1, ltable, scratch_tree,
                             dpages, npages);

            datatuple::freetuple(t1);
            ntuples++;      
            //advance itrA
            t1 = itrA->getnext();
        }

        if(t1 != 0 && datatuple::compare(t1->key(), t2->key()) == 0)
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
            logtree * ltree,
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




