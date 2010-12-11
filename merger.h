#ifndef _MERGER_H_
#define _MERGER_H_

#include "logstore.h"
#include "datatuple.h"

#include <stasis/common.h>
#undef try
#undef end

class merge_scheduler {
public:
  merge_scheduler(logtable<datatuple> * ltable);
  ~merge_scheduler();

  void start();
  void shutdown();

  void * memMergeThread();
  void * diskMergeThread();

private:
  pthread_t mem_merge_thread_;
  pthread_t disk_merge_thread_;
  logtable<datatuple> * ltable_;
  const double MIN_R;
};

#endif
