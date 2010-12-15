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

#define EXTENDED_STATS 1

#include <sys/time.h>
#include <stdio.h>
#include "datatuple.h"
#include "datapage.h"

#include <mergeManager.h> // XXX for double_to_ts, etc... create a util class.

#include <stasis/transactional.h>
#undef try
#undef end

class mergeStats {
  public:
    mergeStats(int merge_level, int64_t target_size) :
      merge_level(merge_level),
      base_size(0),
      mergeable_size(0),
      target_size(target_size),
      bytes_out(0),
      bytes_in_small(0),
      bytes_in_large(0),
      just_handed_off(false),
      delta(0),
      need_tick(0),
      in_progress(0),
      out_progress(0),
      active(false)
#if EXTENDED_STATS
      ,
      stats_merge_count(0),
      stats_bytes_out_with_overhead(0),
      stats_num_tuples_out(0),
      stats_num_datapages_out(0),
      stats_bytes_in_small_delta(0),
      stats_num_tuples_in_small(0),
      stats_num_tuples_in_large(0),
      stats_lifetime_elapsed(0),
      stats_lifetime_consumed(0),
      stats_bps(10.0*1024.0*1024.0)
#endif // EXTENDED_STATS
    {
#if EXTENDED_STATS
      gettimeofday(&stats_sleep,0);
      struct timeval last;
      gettimeofday(&last,0);
      mergeManager::double_to_ts(&stats_last_tick, mergeManager::tv_to_double(&last));
#endif
    }
    mergeStats(int xid, recordid rid) {
      marshalled_header h;
      Tread(xid, rid, &h);
      merge_level    = h.merge_level;
      base_size      = h.base_size;
      mergeable_size = h.mergeable_size;
      target_size    = h.target_size;
      bytes_out      = base_size;
      bytes_in_small = 0;
      bytes_in_large = 0;
      just_handed_off= false;
      delta          = 0;
      need_tick      = 0;
      in_progress    = 0;
      out_progress   = ((double)base_size) / (double)target_size;
      active         = false;
    }
    recordid talloc(int xid) {
      return Talloc(xid, sizeof(marshalled_header));
    }
    void marshal(int xid, recordid rid) {
      marshalled_header h;
      h.merge_level = merge_level;
      h.base_size = base_size;
      h.mergeable_size = mergeable_size;
      h.target_size = h.target_size;
      Tset(xid, rid, &h);
    }
    ~mergeStats() { }
    void new_merge2() {
      if(just_handed_off) {
        bytes_out = 0;
        out_progress = 0;
        just_handed_off = false;
      }
      base_size = bytes_out;
      bytes_out = 0;
      bytes_in_small = 0;
      bytes_in_large = 0;
      in_progress = 0;
#if EXTENDED_STATS
      stats_merge_count++;
      stats_bytes_out_with_overhead = 0;
      stats_num_tuples_out = 0;
      stats_num_datapages_out = 0;
      stats_bytes_in_small_delta = 0;
      stats_num_tuples_in_small = 0;
      stats_num_tuples_in_large = 0;
      gettimeofday(&stats_sleep,0);
#endif
    }
    void starting_merge() {
      active = true;
#if EXTENDED_STATS
      gettimeofday(&stats_start, 0);
      struct timeval last;
      gettimeofday(&last, 0);
      mergeManager::double_to_ts(&stats_last_tick, mergeManager::tv_to_double(&last));
#endif
    }
    pageid_t get_current_size() {
      if(merge_level == 0) {
        return base_size + bytes_in_small - bytes_in_large - bytes_out;
      } else {
        // s->bytes_out has strange semantics.  It's how many bytes our input has written into this tree.
        return base_size + bytes_out - bytes_in_large;
      }
    }
    void handed_off_tree() {
      if(merge_level == 2) {
      } else {
        mergeable_size = get_current_size();
        just_handed_off = true;
      }
    }
    void merged_tuples(datatuple * merged, datatuple * small, datatuple * large) {
    }
    void wrote_datapage(DataPage<datatuple> *dp) {
#if EXTENDED_STATS
      stats_num_datapages_out++;
      stats_bytes_out_with_overhead += (PAGE_SIZE * dp->get_page_count());
#endif
    }
    pageid_t output_size() {
      return bytes_out;
    }
  protected:

    double float_tv(struct timeval& tv) {
      return ((double)tv.tv_sec) + ((double)tv.tv_usec) / 1000000.0;
    }
    friend class mergeManager;

  protected:
    struct marshalled_header {
      int merge_level;
      pageid_t base_size;
      pageid_t mergeable_size;
      pageid_t target_size; // Needed?
    };
  public: // XXX eliminate protected fields.
    int merge_level;               // 1 => C0->C1, 2 => C1->C2
    pageid_t base_size; // size of table at beginning of merge.  for c0, size of table at beginning of current c0-c1 merge round, plus data written since then.  (this minus c1->bytes_in_small is the current size)
  protected:
    pageid_t mergeable_size;  // protected by mutex.
  public:
    pageid_t target_size;
  protected:
    pageid_t bytes_out;            // How many bytes worth of tuples did we write?
  public:
    pageid_t bytes_in_small;       // How many bytes from the small input tree (for C0, we ignore tree overheads)?
  protected:
    pageid_t bytes_in_large;       // Bytes from the large input?

    bool just_handed_off;

    int delta;
    int need_tick;
    double in_progress;
    double out_progress;

    bool active;
#if EXTENDED_STATS
    pageid_t stats_merge_count;          // This is the stats_merge_count'th merge
    struct timeval stats_sleep;          // When did we go to sleep waiting for input?
    struct timeval stats_start;          // When did we wake up and start merging?  (at steady state with max throughput, this should be equal to stats_sleep)
    struct timeval stats_done;           // When did we finish merging?
    struct timespec stats_last_tick;
    pageid_t stats_bytes_out_with_overhead;// How many bytes did we write (including internal tree nodes)?
    pageid_t stats_num_tuples_out;       // How many tuples did we write?
    pageid_t stats_num_datapages_out;    // How many datapages?
    pageid_t stats_bytes_in_small_delta; // How many bytes from the small input tree during this tick (for C0, we ignore tree overheads)?
    pageid_t stats_num_tuples_in_small;  // Tuples from the small input?
    pageid_t stats_num_tuples_in_large;  // Tuples from large input?
    double stats_lifetime_elapsed;
    double stats_lifetime_consumed;
    double stats_bps;
#endif

  public:

    void pretty_print(FILE* fd) {
#if EXTENDED_STATS
      double sleep_time = float_tv(stats_start) - float_tv(stats_sleep);
      double work_time  = float_tv(stats_done)  - float_tv(stats_start);
      double total_time = sleep_time + work_time;
      double mb_out = ((double)stats_bytes_out_with_overhead)     /(1024.0*1024.0);
      double mb_ins = ((double)bytes_in_small)     /(1024.0*1024.0);
      double mb_inl = ((double)bytes_in_large)     /(1024.0*1024.0);
      double kt_out = ((double)stats_num_tuples_out)     /(1024.0);
      double kt_ins=  ((double)stats_num_tuples_in_small)     /(1024.0);
      double kt_inl = ((double)stats_num_tuples_in_large)     /(1024.0);
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
          merge_level, stats_merge_count,
          sleep_time,
          work_time,
          (long long)mb_out, (long long)kt_out, stats_num_datapages_out, mb_out / work_time, mb_out / total_time, kt_out / work_time,  kt_out / total_time,
          (long long)mb_ins, (long long)kt_ins,                    mb_ins / work_time, mb_ins / total_time, kt_ins / work_time,  kt_ins / total_time,
          (long long)mb_inl, (long long)kt_inl,                    mb_inl / work_time, mb_inl / total_time, kt_inl / work_time,  kt_inl / total_time,
          (long long)mb_hdd, (long long)kt_hdd,                    mb_hdd / work_time, mb_hdd / total_time, kt_hdd / work_time,  kt_hdd / total_time,
          mb_out / kt_out,
          mb_ins / work_time, 1000.0 * work_time / mb_ins, mb_ins / total_time, 1000.0 * total_time / mb_ins
          );
#endif
    }
  };

#endif /* MERGESTATS_H_ */
