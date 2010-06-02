/*
 * mergeStats.h
 *
 *  Created on: Apr 27, 2010
 *      Author: sears
 */

#ifndef MERGESTATS_H_
#define MERGESTATS_H_

#include <stasis/common.h>
#undef try
#undef end

#include <sys/time.h>
#include <stdio.h>
#include "datatuple.h"
#include "datapage.h"

#include <mergeManager.h> // XXX for double_to_ts, etc... create a util class.

class mergeStats {
  public:
    mergeStats(int merge_level, int64_t target_size) :
      merge_level(merge_level),
      merge_count(0),
      base_size(0),
      target_size(target_size),
      current_size(0),
      mergeable_size(0),
      bytes_out_with_overhead(0),
      bytes_out(0),
      num_tuples_out(0),
      num_datapages_out(0),
      bytes_in_small(0),
      bytes_in_small_delta(0),
      num_tuples_in_small(0),
      bytes_in_large(0),
      num_tuples_in_large(0),
      just_handed_off(false),
      lifetime_elapsed(0),
      lifetime_consumed(0),
      window_elapsed(0.001),
      window_consumed(0),
      print_skipped(0),
      active(false) {
      gettimeofday(&sleep,0);
      gettimeofday(&last,0);
      mergeManager::double_to_ts(&last_tick, mergeManager::tv_to_double(&last));
    }
    void new_merge2() {
      if(just_handed_off) {
        bytes_out = 0;
        just_handed_off = false;
      }
      base_size = bytes_out;
      current_size = base_size;
      merge_count++;
      bytes_out_with_overhead = 0;
      bytes_out = 0;
      num_tuples_out = 0;
      num_datapages_out = 0;
      bytes_in_small = 0;
      bytes_in_small_delta = 0;
      num_tuples_in_small = 0;
      bytes_in_large = 0;
      num_tuples_in_large = 0;
      gettimeofday(&sleep,0);
    }
    void starting_merge() {
      active = true;
      gettimeofday(&start, 0);
      gettimeofday(&last, 0);
      mergeManager::double_to_ts(&last_tick, mergeManager::tv_to_double(&last));

    }
    void handed_off_tree() {
      if(merge_level == 2) {
      } else {
        mergeable_size = current_size;
        just_handed_off = true;
      }
    }
    void read_tuple_from_large_component(datatuple * tup) {
      if(tup) {
        num_tuples_in_large++;
        bytes_in_large += tup->byte_length();
      }
    }
    void merged_tuples(datatuple * merged, datatuple * small, datatuple * large) {
    }
    void wrote_datapage(DataPage<datatuple> *dp) {
      num_datapages_out++;
      bytes_out_with_overhead += (PAGE_SIZE * dp->get_page_count());
    }
    pageid_t output_size() {
      return bytes_out;
    }
    const int merge_level;               // 1 => C0->C1, 2 => C1->C2
  protected:
    pageid_t merge_count;          // This is the merge_count'th merge
    struct timeval sleep;          // When did we go to sleep waiting for input?
    struct timeval start;          // When did we wake up and start merging?  (at steady state with max throughput, this should be equal to sleep)
    struct timeval done;           // When did we finish merging?
    struct timeval last;

    double float_tv(struct timeval& tv) {
      return ((double)tv.tv_sec) + ((double)tv.tv_usec) / 1000000.0;
    }
    friend class mergeManager;

    struct timespec last_tick;

    pageid_t base_size;
    pageid_t target_size;
    pageid_t current_size;
    pageid_t mergeable_size;  // protected by mutex.

    pageid_t bytes_out_with_overhead;// How many bytes did we write (including internal tree nodes)?
    pageid_t bytes_out;            // How many bytes worth of tuples did we write?
    pageid_t num_tuples_out;       // How many tuples did we write?
    pageid_t num_datapages_out;    // How many datapages?
    pageid_t bytes_in_small;       // How many bytes from the small input tree (for C0, we ignore tree overheads)?
    pageid_t bytes_in_small_delta; // How many bytes from the small input tree during this tick (for C0, we ignore tree overheads)?
    pageid_t num_tuples_in_small;  // Tuples from the small input?
    pageid_t bytes_in_large;       // Bytes from the large input?
    pageid_t num_tuples_in_large;  // Tuples from large input?

    bool just_handed_off;

    double lifetime_elapsed;
    double lifetime_consumed;
    double window_elapsed;
    double window_consumed;

    int print_skipped;  // used by pretty print in mergeManager.

    bool active;
  public:

    void pretty_print(FILE* fd) {
      double sleep_time = float_tv(start) - float_tv(sleep);
      double work_time  = float_tv(done)  - float_tv(start);
      double total_time = sleep_time + work_time;
      double mb_out = ((double)bytes_out_with_overhead)     /(1024.0*1024.0);
      double mb_ins = ((double)bytes_in_small)     /(1024.0*1024.0);
      double mb_inl = ((double)bytes_in_large)     /(1024.0*1024.0);
      double kt_out = ((double)num_tuples_out)     /(1024.0);
      double kt_ins=  ((double)num_tuples_in_small)     /(1024.0);
      double kt_inl = ((double)num_tuples_in_large)     /(1024.0);
      double mb_hdd = mb_out + mb_inl + (merge_level == 1 ? 0.0 : mb_ins);
      double kt_hdd = kt_out + kt_inl + (merge_level == 1 ? 0.0 : kt_ins);


      fprintf(fd,
          "=====================================================================\n"
          "Thread %d merge %lld: sleep %6.2f sec, run %6.2f sec\n"
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
          merge_level, merge_count,
          sleep_time,
          work_time,
          (long long)mb_out, (long long)kt_out, num_datapages_out, mb_out / work_time, mb_out / total_time, kt_out / work_time,  kt_out / total_time,
          (long long)mb_ins, (long long)kt_ins,                    mb_ins / work_time, mb_ins / total_time, kt_ins / work_time,  kt_ins / total_time,
          (long long)mb_inl, (long long)kt_inl,                    mb_inl / work_time, mb_inl / total_time, kt_inl / work_time,  kt_inl / total_time,
          (long long)mb_hdd, (long long)kt_hdd,                    mb_hdd / work_time, mb_hdd / total_time, kt_hdd / work_time,  kt_hdd / total_time,
          mb_out / kt_out,
          mb_ins / work_time, 1000.0 * work_time / mb_ins, mb_ins / total_time, 1000.0 * total_time / mb_ins
          );
    }
  };

#endif /* MERGESTATS_H_ */
