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
#include <datatuple.h>

template<class TUPLE>
class logtable;

class mergeStats;

class mergeManager {
public:
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
  mergeManager(logtable<datatuple> *ltable);

  ~mergeManager();

  void new_merge(mergeStats * s);
  void set_c0_size(int64_t size);
  void tick(mergeStats * s, bool block);
  void pretty_print(FILE * out);
  mergeStats* get_merge_stats(int mergeLevel);

private:
  pthread_mutex_t mut;
  logtable<datatuple>*    ltable;
  double throttle_seconds;
//  double elapsed_seconds;
  double last_throttle_seconds;
//  double last_elapsed_seconds;
  mergeStats * c0;
  mergeStats * c1;
  mergeStats * c2;
  pthread_mutex_t throttle_mut;
  pthread_mutex_t dummy_throttle_mut;
  pthread_cond_t dummy_throttle_cond;
  pthread_cond_t throttle_wokeup_cond;
  bool sleeping[3];
  int print_skipped;

};
#endif /* MERGEMANAGER_H_ */
