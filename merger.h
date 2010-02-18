#ifndef _MERGER_H_
#define _MERGER_H_

#include <vector>
#include <utility>

#include "logstore.h"
#include "logiterators.h"

//TODO: 400 bytes overhead per tuple, this is nuts, check if this is true...
static const int RB_TREE_OVERHEAD = 400;
static const double MIN_R = 3.0;

struct merger_args
{
    logtable * ltable;
    int worker_id;

    pthread_mutex_t * block_ready_mut;

    pthread_cond_t * in_block_needed_cond;
    bool * in_block_needed;

    pthread_cond_t * out_block_needed_cond;
    bool * out_block_needed;

    pthread_cond_t * in_block_ready_cond;
    pthread_cond_t * out_block_ready_cond;

    int64_t max_size;
    double * r_i;
};



struct logtable_mergedata
{
    //merge threads
    pthread_t diskmerge_thread;
    pthread_t memmerge_thread;

    rwl *header_lock;
    
    pthread_mutex_t * rbtree_mut;

    bool *input_needed; // memmerge-input needed
    
    pthread_cond_t * input_ready_cond;
    pthread_cond_t * input_needed_cond;
    int64_t * input_size;

    //merge args 1
    struct merger_args *diskmerge_args;
    //merge args 2
    struct merger_args *memmerge_args;
    
};


class merge_scheduler
{
    std::vector<std::pair<logtable *, logtable_mergedata*> > mergedata; 

public:
    //static pageid_t C0_MEM_SIZE; 
    ~merge_scheduler();
    
    int addlogtable(logtable * ltable);
    void startlogtable(int index, int64_t MAX_C0_SIZE = 100*1024*1024);

    struct logtable_mergedata *getMergeData(int index){return mergedata[index].second;}

    void shutdown();
};

template <class ITA, class ITB>
int64_t merge_iterators(int xid,
                    ITA *itrA,
                    ITB *itrB,
                    logtable *ltable,
                    logtree *scratch_tree,
                    int64_t &npages,
                    bool dropDeletes);

void* memMergeThread(void* arg);
void* diskMergeThread(void* arg);

#endif
