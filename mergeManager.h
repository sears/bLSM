/*
 * mergeManager.h
 *
 *  Created on: May 19, 2010
 *      Author: sears
 */

#ifndef MERGEMANAGER_H_
#define MERGEMANAGER_H_

#include <stasis/common.h>
#undef try
#undef end
#include <sys/time.h>
#include <stdio.h>

class mergeStats;

class mergeManager {
private:
  double tv_to_double(struct timeval * tv) {
    return (double)tv->tv_sec + ((double)tv->tv_usec)/1000000.0;
  }
  double ts_to_double(struct timespec * ts) {
    return (double)ts->tv_sec + ((double)ts->tv_nsec)/1000000000.0;
  }
  void double_to_ts(struct timespec *ts, double time) {
    ts->tv_sec = (time_t)(time);
    ts->tv_nsec = (long)((time - (double)ts->tv_sec) * 1000000000.0);
  }
  uint64_t long_tv(struct timeval& tv) {
    return (1000000ULL * (uint64_t)tv.tv_sec) + ((uint64_t)tv.tv_usec);
  }
public:
  mergeManager(void *ltable);

  ~mergeManager();

  void new_merge(mergeStats * s);
  void set_c0_size(int64_t size);
  void tick(mergeStats * s, bool done = false);
  void pretty_print(FILE * out);
  mergeStats* newMergeStats(int mergeLevel);

private:
  pthread_mutex_t mut;
  void*    ltable;
  pageid_t c0_queueSize;
  pageid_t c1_queueSize;  // How many bytes must c0-c1 consume before trying to swap over to an empty c1? ( = current target c1 size)
  pageid_t c2_queueSize;  // How many bytes must c1-c2 consume before there is room for a new empty c1?  ( = previous c1 size)
  pageid_t c0_totalConsumed;
  pageid_t c0_totalCollapsed;
  pageid_t c0_totalWorktime;
  pageid_t c1_totalConsumed;  // What is long-term effective throughput of merger #1? (Excluding blocked times)
  pageid_t c1_totalCollapsed;
  pageid_t c1_totalWorktime;
  pageid_t c2_totalConsumed;  // What is long term effective throughput of merger #2? (Excluding blocked times)
  pageid_t c2_totalCollapsed;
  pageid_t c2_totalWorktime;
  double throttle_seconds;
  double elapsed_seconds;
  double last_throttle_seconds;
  double last_elapsed_seconds;
  mergeStats * c0;
  mergeStats * c1;
  mergeStats * c2;
  struct timespec last_throttle;
  pthread_mutex_t throttle_mut;
  pthread_mutex_t dummy_throttle_mut;
  pthread_cond_t dummy_throttle_cond;

};
#endif /* MERGEMANAGER_H_ */
