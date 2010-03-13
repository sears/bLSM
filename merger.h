#ifndef _MERGER_H_
#define _MERGER_H_

#include <vector>
#include <utility>

#include <stasis/common.h>
#undef try
#undef end

//TODO: 400 bytes overhead per tuple, this is nuts, check if this is true...
static const int RB_TREE_OVERHEAD = 400;
static const double MIN_R = 3.0;
class logtable;

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

    pageid_t internal_region_size;
    pageid_t datapage_region_size;
    pageid_t datapage_size;

    int64_t max_size;
    double * r_i;
};


struct logtable_mergedata
{
    //merge threads
    pthread_t diskmerge_thread;
    pthread_t memmerge_thread;

    pthread_mutex_t * rbtree_mut;

    bool *input_needed; // memmerge-input needed
    
    pthread_cond_t * input_ready_cond;
    pthread_cond_t * input_needed_cond;
    int64_t * input_size;

    pageid_t internal_region_size;
    pageid_t datapage_region_size;
    pageid_t datapage_size;

    //merge args 1
    struct merger_args *diskmerge_args;
    //merge args 2
    struct merger_args *memmerge_args;
    
};

#include "logstore.h"            // XXX hacky include workaround.

class merge_scheduler
{
    std::vector<std::pair<logtable *, logtable_mergedata*> > mergedata; 

public:
    ~merge_scheduler();
    
    int addlogtable(logtable * ltable);
    void startlogtable(int index, int64_t MAX_C0_SIZE = 100*1024*1024);

    struct logtable_mergedata *getMergeData(int index){return mergedata[index].second;}

    void shutdown();
};

void* memMergeThread(void* arg);
void* diskMergeThread(void* arg);

#endif
