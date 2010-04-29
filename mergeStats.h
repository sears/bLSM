/*
 * mergeStats.h
 *
 *  Created on: Apr 27, 2010
 *      Author: sears
 */

#ifndef MERGESTATS_H_
#define MERGESTATS_H_

#include <stasis/common.h>

class mergeManager {
  pageid_t c1_queueSize;  // How many bytes must c0-c1 consume before trying to swap over to an empty c1? ( = current target c1 size)
  pageid_t c2_queueSize;  // How many bytes must c1-c2 consume before there is room for a new empty c1?  ( = previous c1 size)
  pageid_t c1_totalConsumed;  // What is long-term effective throughput of merger #1? (Excluding blocked times)
  pageid_t c1_totalWorktime;
  pageid_t c2_totalConsumed;  // What is long term effective throughput of merger #2? (Excluding blocked times)
  pageid_t c2_totalWorktime;
public:
  mergeManager() :
    c1_queueSize(0),
    c2_queueSize(0),
    c1_totalConsumed(0),
    c1_totalWorktime(0),
    c2_totalConsumed(0),
    c2_totalWorktime(0) { }

  class mergeStats;

  void new_merge(mergeStats * s) {
    if(s->merge_count) {
      if(s->merge_level == 1) {
        c1_queueSize = c1_queueSize > s->bytes_out ? c1_queueSize : s->bytes_out;
        c1_totalConsumed += s->bytes_in_small;
        c1_totalWorktime += (long_tv(s->done) - long_tv(s->start));
      } else if(s->merge_level == 2) {
        c2_queueSize = s->bytes_in_small;
        c2_totalConsumed += s->bytes_in_small;
        c2_totalWorktime += (long_tv(s->done) - long_tv(s->start));
      } else { abort(); }
      pretty_print(stdout);
    }
  }

  uint64_t long_tv(struct timeval& tv) {
    return (1000000ULL * (uint64_t)tv.tv_sec) + ((uint64_t)tv.tv_usec);
  }
  void pretty_print(FILE * out) {
          fprintf(out,
              "C1 queue size %lld C2 queue size %lld C1 MB/s (eff; active) %6.1f  C2 MB/s %6.1f\n",
              c1_queueSize, c2_queueSize,
              ((double)c1_totalConsumed)/((double)c1_totalWorktime),
              ((double)c2_totalConsumed)/((double)c2_totalWorktime)
              );
        }
  class mergeStats {
  public:
    mergeStats(mergeManager* merge_mgr, int merge_level) :
      merge_mgr(merge_mgr),
      merge_level(merge_level),
      merge_count(0),
      bytes_out(0),
      num_tuples_out(0),
      num_datapages_out(0),
      bytes_in_small(0),
      num_tuples_in_small(0),
      bytes_in_large(0),
      num_tuples_in_large(0) {
      gettimeofday(&sleep,0);
    }
    void new_merge() {
      merge_mgr->new_merge(this);
      merge_count++;
      bytes_out = 0;
      num_tuples_out = 0;
      num_datapages_out = 0;
      bytes_in_small = 0;
      num_tuples_in_small = 0;
      bytes_in_large = 0;
      num_tuples_in_large = 0;
      gettimeofday(&sleep,0);
    }
    void starting_merge() {
      gettimeofday(&start, 0);
    }
    void finished_merge() {
      gettimeofday(&done, 0);
    }
    void read_tuple_from_large_component(datatuple * tup) {
      if(tup) {
        num_tuples_in_large++;
        bytes_in_large += tup->byte_length();
      }
    }
    void read_tuple_from_small_component(datatuple * tup) {
      if(tup) {
        num_tuples_in_small++;
        bytes_in_small += tup->byte_length();
      }
    }
    void wrote_tuple(datatuple * tup) {
      num_tuples_out++;
      bytes_out_tuples += tup->byte_length();
    }
    void wrote_datapage(DataPage<datatuple> *dp) {
      num_datapages_out++;
      bytes_out += (PAGE_SIZE * dp->get_page_count());
    }
    // TODO: merger.cpp probably shouldn't compute R from this.
    pageid_t output_size() {
      return bytes_out;
    }
  protected:
    mergeManager* merge_mgr;
    int merge_level;               // 1 => C0->C1, 2 => C1->C2
    pageid_t merge_count;          // This is the merge_count'th merge
    struct timeval sleep;          // When did we go to sleep waiting for input?
    struct timeval start;          // When did we wake up and start merging?  (at steady state with max throughput, this should be equal to sleep)
    struct timeval done;           // When did we finish merging?

    double float_tv(struct timeval& tv) {
      return ((double)tv.tv_sec) + ((double)tv.tv_usec) / 1000000.0;
    }
    friend class mergeManager;

    pageid_t bytes_out;            // How many bytes did we write (including internal tree nodes)?
    pageid_t bytes_out_tuples;     // How many bytes worth of tuples did we write?
    pageid_t num_tuples_out;       // How many tuples did we write?
    pageid_t num_datapages_out;    // How many datapages?
    pageid_t bytes_in_small;       // How many bytes from the small input tree (for C0, we ignore tree overheads)?
    pageid_t num_tuples_in_small;  // Tuples from the small input?
    pageid_t bytes_in_large;       // Bytes from the large input?
    pageid_t num_tuples_in_large;  // Tuples from large input?
  public:

    void pretty_print(FILE* fd) {
      double sleep_time = float_tv(start) - float_tv(sleep);
      double work_time  = float_tv(done)  - float_tv(start);
      double total_time = sleep_time + work_time;
      double mb_out = ((double)bytes_out)     /(1024.0*1024.0);
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


  mergeStats* newMergeStats(int mergeLevel) {
    return new mergeStats(this, mergeLevel);
  }
};
#endif /* MERGESTATS_H_ */
