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
  static const int UPDATE_PROGRESS_DELTA = 10 * 1024 * 1024;
  const double UPDATE_PROGRESS_PERIOD; // in seconds, defined in constructor.
  static const int FORCE_INTERVAL = 25 * 1024 * 1024;
  static double tv_to_double(struct timeval * tv) {
    return (double)tv->tv_sec + ((double)tv->tv_usec)/1000000.0;
  }
  static double ts_to_double(struct timespec * ts) {
    return (double)ts->tv_sec + ((double)ts->tv_nsec)/1000000000.0;
  }
  static void double_to_ts(struct timespec *ts, double time) {
    ts->tv_sec = (time_t)(time);
    ts->tv_nsec = (long)((time - (double)ts->tv_sec) * 1000000000.0);
  }
  uint64_t long_tv(struct timeval& tv) {
    return (1000000ULL * (uint64_t)tv.tv_sec) + ((uint64_t)tv.tv_usec);
  }
  mergeManager(logtable<datatuple> *ltable);
  mergeManager(logtable<datatuple> *ltable, int xid, recordid rid);
  void marshal(int xid, recordid rid);
  recordid talloc(int xid);
  ~mergeManager();

  void new_merge(int mergelevel);
  void set_c0_size(int64_t size);
  void update_progress(mergeStats *s, int delta);
  double c1_c2_progress_delta();

  void tick(mergeStats * s);
  mergeStats* get_merge_stats(int mergeLevel);
  void read_tuple_from_small_component(int merge_level, datatuple * tup);
  void read_tuple_from_large_component(int merge_level, datatuple * tup) {
    if(tup)
      read_tuple_from_large_component(merge_level, 1, tup->byte_length());
  }
  void read_tuple_from_large_component(int merge_level, int tuple_count, pageid_t byte_len);

  void wrote_tuple(int merge_level, datatuple * tup);
  void finished_merge(int merge_level);
  void pretty_print(FILE * out);
  void *pretty_print_thread();

private:
  double c1_c2_delta;
  void init_helper(void);
  struct marshalled_header {
    recordid c0;
    recordid c1;
    recordid c2;
  };
  logtable<datatuple>*    ltable;
  mergeStats * c0;
  mergeStats * c1;
  mergeStats * c2;

  // The following fields are used to shut down the pretty print thread.
  bool still_running;
  pthread_cond_t pp_cond;
  pthread_t pp_thread;
};
#endif /* MERGEMANAGER_H_ */
