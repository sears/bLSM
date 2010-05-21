#ifndef _MERGER_H_
#define _MERGER_H_

#include "logstore.h"
#include "datatuple.h"

#include <stasis/common.h>
#undef try
#undef end

//TODO: 400 bytes overhead per tuple, this is nuts, check if this is true...
static const int RB_TREE_OVERHEAD = 400;
static const double MIN_R = 3.0;

struct merger_args
{
    logtable<datatuple> * ltable;
    int64_t max_size;
};

struct logtable_mergedata
{
    //merge threads
    pthread_t diskmerge_thread;
    pthread_t memmerge_thread;

    //merge args 1
    struct merger_args *diskmerge_args;
    //merge args 2
    struct merger_args *memmerge_args;
    
};

class merge_scheduler
{
    std::vector<std::pair<logtable<datatuple> *, logtable_mergedata*> > mergedata;

public:
    ~merge_scheduler();

    int addlogtable(logtable<datatuple> * ltable);
    void startlogtable(int index, int64_t MAX_C0_SIZE = 100*1024*1024);

    struct logtable_mergedata *getMergeData(int index){return mergedata[index].second;}

    void shutdown();
};

void* memMergeThread(void* arg);
void* diskMergeThread(void* arg);

#endif
