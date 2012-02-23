/*
 * mergeManager.h
 *
 * Copyright 2010-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Created on: May 19, 2010
 *      Author: sears
 */

#ifndef MERGEMANAGER_H_
#define MERGEMANAGER_H_

#include <stasis/common.h>
#include <sys/time.h>
#include <stdio.h>
#include <dataTuple.h>

class bLSM;
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
  mergeManager(bLSM *ltable);
  mergeManager(bLSM *ltable, int xid, recordid rid);
  void marshal(int xid, recordid rid);
  recordid talloc(int xid);
  ~mergeManager();

  void finished_merge(int merge_level);
  void new_merge(int mergelevel);
  void set_c0_size(int64_t size);
  void update_progress(mergeStats *s, int delta);
  double c1_c2_progress_delta();

  void tick(mergeStats * s);
  mergeStats* get_merge_stats(int mergeLevel);
  void read_tuple_from_small_component(int merge_level, dataTuple * tup);
  void read_tuple_from_large_component(int merge_level, dataTuple * tup) {
    if(tup)
      read_tuple_from_large_component(merge_level, 1, tup->byte_length());
  }
  void read_tuple_from_large_component(int merge_level, int tuple_count, pageid_t byte_len);

  void wrote_tuple(int merge_level, dataTuple * tup);
  void pretty_print(FILE * out);
  void *pretty_print_thread();
  void *update_progress_thread();

private:
  /**
   * How far apart are the c0-c1 and c1-c2 mergers?
   *
   * This is c1->out_progress - c2->in_progress.  We want the downstream merger
   * to be slightly ahead of the upstream one so that we can mask latency blips
   * due to tearing down the downstream merger and starting the new one.
   * Therefore, this should always be slightly negative.
   *
   * TODO remove c1_c2_delta, which is derived, but difficult (from a synchronization perspective) to compute?
   */
  double c1_c2_delta;
  /** Helper method for the constructors */
  void init_helper(void);
  /**
   * Serialization format for Stasis merge statistics header.
   *
   * The small amount of state maintained by mergeManager consists of derived
   * and runtime-only fields.  This struct reflects that, and only contains
   * pointers to marshaled versions of the per-tree component statistics.
   */
  struct marshalled_header {
    recordid c0; // Probably redundant, but included for symmetry.
    recordid c1;
    recordid c2;
  };
  /**
   * A pointer to the logtable that we manage statistics for.  Most usages of
   * this are layering violations; the main exception is in pretty_print.
   *
   * TODO: remove mergeManager->ltable?
   */
  bLSM*    ltable;
  mergeStats * c0;   /// Per-tree component statistics for c0 and c0_mergeable (the latter should always be null...)
  mergeStats * c1;   /// Per-tree component statistics for c1 and c1_mergeable.
  mergeStats * c2;   /// Per-tree component statistics for c2.

  // The following fields are used to shut down the pretty print thread.
  bool still_running;
  pthread_cond_t pp_cond;
  pthread_t pp_thread;
  pthread_cond_t update_progress_cond;
  pthread_t update_progress_pthread;
};
#endif /* MERGEMANAGER_H_ */
