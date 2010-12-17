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
  private:
    void init_helper(void) {
#if EXTENDED_STATS
      gettimeofday(&stats_sleep,0);
      gettimeofday(&stats_start,0);
      gettimeofday(&stats_done,0);

      struct timeval last;
      gettimeofday(&last,0);
      mergeManager::double_to_ts(&stats_last_tick, mergeManager::tv_to_double(&last));
#endif
    }
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
      stats_lifetime_active(0),
      stats_elapsed(0),
      stats_active(0),
      stats_lifetime_consumed(0),
      stats_bps(10.0*1024.0*1024.0)
#endif // EXTENDED_STATS
    {
      init_helper();
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
#if EXTENDED_STATS
      stats_merge_count = 0;
      stats_bytes_out_with_overhead = 0;
      stats_num_tuples_out = 0;
      stats_num_datapages_out = 0;
      stats_bytes_in_small_delta = 0;
      stats_num_tuples_in_small = 0;
      stats_num_tuples_in_large = 0;
      stats_lifetime_elapsed = 0;
      stats_lifetime_active = 0;
      stats_elapsed = 0;
      stats_active = 0;
      stats_lifetime_consumed = 0;
      stats_bps = 10.0*1024.0*1024.0;
#endif
      init_helper();
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
  public: // XXX eliminate public fields; these are still required because various bits of calculation (bloom filter size, estimated c0 run length, etc...) are managed outside of mergeManager.
    int merge_level;               /// The tree component / merge level that we're tracking.  1 => C0->C1, 2 => C1->C2
    pageid_t base_size;            /// size of existing tree component (c[merge_level]') at beginning of current merge.
  protected:
    pageid_t mergeable_size;       /// The size of c[merge_level]_mergeable, assuming it exists.  Protected by mutex.
  public:
    pageid_t target_size;          /// How big should the c[merge_level] tree component be?
  protected:
    pageid_t bytes_out;            /// For C0, number of bytes consumed by downstream merger.  For merge_level 1 and 2, number of bytes enqueued for the downstream (C1-C2, and nil) mergers.
  public:
    pageid_t bytes_in_small;       /// For C0, number of bytes inserted by application.  For C1, C2, number of bytes read from small tree in C(n-1) - Cn merger.
  protected:
    pageid_t bytes_in_large;       /// Bytes from the large input?  (for C0, bytes deleted due to updates)

    // todo: simplify confusing hand off logic, and remove this field?
    bool just_handed_off;

    // These fields are used to amortize mutex acquisitions.
    int delta;
    int need_tick;

    // todo in_progress and out_progress are derived fields. eliminate them?
    double in_progress;
    double out_progress;

    bool active;                    /// True if this merger is running, or blocked by rate limiting.  False if the upstream input does not exist.
#if EXTENDED_STATS
    pageid_t stats_merge_count;          /// This is the stats_merge_count'th merge
    struct timeval stats_sleep;          /// When did we go to sleep waiting for input?
    struct timeval stats_start;          /// When did we wake up and start merging?  (at steady state with max throughput, this should be equal to stats_sleep)
    struct timeval stats_done;           /// When did we finish merging?
    struct timespec stats_last_tick;
    pageid_t stats_bytes_out_with_overhead;/// How many bytes did we write (including internal tree nodes)?
    pageid_t stats_num_tuples_out;       /// How many tuples did we write?
    pageid_t stats_num_datapages_out;    /// How many datapages?
    pageid_t stats_bytes_in_small_delta; /// How many bytes from the small input tree during this tick (for C0, we ignore tree overheads)?
    pageid_t stats_num_tuples_in_small;  /// Tuples from the small input?
    pageid_t stats_num_tuples_in_large;  /// Tuples from large input?
    double stats_lifetime_elapsed;       /// How long has this tree existed, in seconds?
    double stats_lifetime_active;        /// How long has this tree been running (i.e.; active = true), in seconds?
    double stats_elapsed;                /// How long did this merge take, including idle time (not valid until after merge is complete)?
    double stats_active;                 /// How long did this merge take once it started running?
    double stats_lifetime_consumed;      /// How many bytes has this tree consumed from upstream mergers?
    double stats_bps;                    /// Effective throughput while active.
#endif

  public:

    void pretty_print(FILE* fd) {
#if EXTENDED_STATS
      double sleep_time = stats_elapsed - stats_active;
      double work_time  = stats_active;
      double total_time = stats_elapsed;
      double mb_out = ((double)bytes_out)     /(1024.0*1024.0);
      double phys_mb_out = ((double)stats_bytes_out_with_overhead) / (1024.0 * 1024.0);
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
          "avg tuple len: %6.2fKB w/ disk ovehead: %6.2fKB\n"
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
          mb_out / kt_out, phys_mb_out / kt_out,
          mb_ins / work_time, 1000.0 * work_time / mb_ins, mb_ins / total_time, 1000.0 * total_time / mb_ins
          );
#endif
    }
  };

#endif /* MERGESTATS_H_ */
