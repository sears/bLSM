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
public:
  mergeManager() :
    c0_queueSize(0),
    c1_queueSize(0),
    c2_queueSize(0),
    c0_totalConsumed(0),
    c0_totalCollapsed(0),
    c0_totalWorktime(0),
    c1_totalConsumed(0),
    c1_totalCollapsed(0),
    c1_totalWorktime(0),
    c2_totalConsumed(0),
    c2_totalCollapsed(0),
    c2_totalWorktime(0),
    c0(new mergeStats(this, 0)),
    c1(new mergeStats(this, 1)),
    c2(new mergeStats(this, 2)) {
    pthread_mutex_init(&mut, 0);
    pthread_mutex_init(&throttle_mut, 0);
    pthread_mutex_init(&dummy_throttle_mut, 0);
    pthread_cond_init(&dummy_throttle_cond, 0);
    struct timeval tv;
    gettimeofday(&tv, 0);
    double_to_ts(&last_throttle, tv_to_double(&tv));

  }
  ~mergeManager() {
    pthread_mutex_destroy(&mut);
  }
  class mergeStats;

  void new_merge(mergeStats * s) {
    pthread_mutex_lock(&mut);
    if(s->merge_count) {
      if(s->merge_level == 0) {
        // queueSize was set during startup
      } else if(s->merge_level == 1) {
        c1_queueSize = c1_queueSize > s->bytes_out ? c1_queueSize : s->bytes_out;
      } else if(s->merge_level == 2) {
        c2_queueSize = s->bytes_in_small;
      } else { abort(); }
      pretty_print(stdout);
    }
    pthread_mutex_unlock(&mut);
  }
  void set_c0_size(int64_t size) {
    c0_queueSize = size;
  }
  void tick(mergeStats * s, bool done = false) {
    if(s->merge_level == 0) {
      pthread_mutex_lock(&throttle_mut);
      // throttle?
      if(s->bytes_in_small_delta > c0_queueSize / 100) {
        struct timeval now;
        gettimeofday(&now, 0);
        double elapsed_delta = tv_to_double(&now) - ts_to_double(&last_throttle);
        pageid_t bytes_written_delta = (s->bytes_in_small_delta - s->bytes_collapsed_delta);
        double min_throughput = 100 * 1024; // don't throttle below 100 kilobytes / sec
        double c0_badness = (double)((c0_totalConsumed + bytes_written_delta - c1_totalConsumed) - c0_queueSize)/ (double)c0_queueSize;
        if(c0_badness > 0) {
          double target_throughput = min_throughput / (c0_badness * c0_badness);
          //double raw_throughput = ((double)bytes_written_delta)/elapsed_delta;
          double target_elapsed = ((double)bytes_written_delta)/target_throughput;
          printf("Worked %6.1f (target %6.1f)\n", elapsed_delta, target_elapsed);
          if(target_elapsed > elapsed_delta) {
            struct timespec sleep_until;
            double_to_ts(&sleep_until, ts_to_double(&last_throttle) + target_elapsed);
            fprintf(stdout, "Throttling for %6.1f seconds\n", target_elapsed - (double)elapsed_delta);
            pthread_mutex_lock(&dummy_throttle_mut);
            pthread_cond_timedwait(&dummy_throttle_cond, &dummy_throttle_mut, &sleep_until);
            pthread_mutex_unlock(&dummy_throttle_mut);
            memcpy(&last_throttle, &sleep_until, sizeof(sleep_until));
          } else {
            double_to_ts(&last_throttle, tv_to_double(&now));
          }
        } else {
          printf("badness is negative\n");
          double_to_ts(&last_throttle, tv_to_double(&now));
        }
      }
    }
    if(done || s->bytes_in_small_delta > c0_queueSize / 100) {
      struct timeval now;
      gettimeofday(&now,0);
      unsigned long long elapsed = long_tv(now) - long_tv(s->last);

      pthread_mutex_lock(&mut);
      if(s->merge_level == 0) {
        c0_totalConsumed += s->bytes_in_small_delta;
        c0_totalWorktime += elapsed;
        c0_totalCollapsed += s->bytes_collapsed_delta;
      } else if(s->merge_level == 1) {
        c1_totalConsumed += s->bytes_in_small_delta;
        c1_totalWorktime += elapsed;
        c1_totalCollapsed += s->bytes_collapsed_delta;
      } else if(s->merge_level == 2) {
        c2_totalConsumed += s->bytes_in_small_delta;
        c2_totalWorktime += elapsed;
        c2_totalCollapsed += s->bytes_collapsed_delta;
      } else { abort(); }
      pthread_mutex_unlock(&mut);

      s->bytes_in_small_delta = 0;
      s->bytes_collapsed_delta = 0;

      memcpy(&s->last, &now, sizeof(now));
      pretty_print(stdout);
    }
    if(s->merge_level == 0) {
      pthread_mutex_unlock(&throttle_mut);
    }
  }
  uint64_t long_tv(struct timeval& tv) {
    return (1000000ULL * (uint64_t)tv.tv_sec) + ((uint64_t)tv.tv_usec);
  }
  void pretty_print(FILE * out) {
    pageid_t mb = 1024 * 1024;
    fprintf(out,"%s %s %s ", c0->active ? "RUN" : "---", c1->active ? "RUN" : "---", c2->active ? "RUN" : "---");
    fprintf(out, "C0 size %lld collapsed %lld resident %lld ",
                 2*c0_queueSize/mb,
                 c0_totalCollapsed/mb,
                 (c0_totalConsumed - (c0_totalCollapsed + c1_totalConsumed))/mb);
    fprintf(out, "C1 size %lld collapsed %lld resident %lld ",
                 2*c1_queueSize/mb,
                 c1_totalCollapsed/mb,
                 (c1_totalConsumed - (c1_totalCollapsed + c2_totalConsumed))/mb);
    fprintf(out, "C2 size %lld collapsed %lld ",
                 2*c2_queueSize/mb, c2_totalCollapsed/mb);
    fprintf(out, "C1 MB/s (eff; active) %6.1f  C2 MB/s %6.1f\n",
                  ((double)c1_totalConsumed)/((double)c1_totalWorktime),
                  ((double)c2_totalConsumed)/((double)c2_totalWorktime));
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
      bytes_in_small_delta(0),
      bytes_collapsed(0),
      bytes_collapsed_delta(0),
      num_tuples_in_small(0),
      bytes_in_large(0),
      num_tuples_in_large(0),
      active(false) {
      gettimeofday(&sleep,0);
      gettimeofday(&last,0);
    }
    void new_merge() {
      merge_mgr->new_merge(this);
      merge_count++;
      bytes_out = 0;
      num_tuples_out = 0;
      num_datapages_out = 0;
      bytes_in_small = 0;
      bytes_in_small_delta = 0;
      bytes_collapsed = 0;
      bytes_collapsed_delta = 0;
      num_tuples_in_small = 0;
      bytes_in_large = 0;
      num_tuples_in_large = 0;
      gettimeofday(&sleep,0);
    }
    void starting_merge() {
      active = true;
      gettimeofday(&start, 0);
      gettimeofday(&last, 0);
    }
    void finished_merge() {
      active = false;
      merge_mgr->tick(this, true);
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
        bytes_in_small_delta += tup->byte_length();
        bytes_in_small += tup->byte_length();
        merge_mgr->tick(this);
      }
    }
    void merged_tuples(datatuple * merged, datatuple * small, datatuple * large) {
      pageid_t d = (merged->byte_length() - (small->byte_length() + large->byte_length()));
      bytes_collapsed += d;
      bytes_collapsed_delta += d;
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
    struct timeval last;

    double float_tv(struct timeval& tv) {
      return ((double)tv.tv_sec) + ((double)tv.tv_usec) / 1000000.0;
    }
    friend class mergeManager;

    pageid_t bytes_out;            // How many bytes did we write (including internal tree nodes)?
    pageid_t bytes_out_tuples;     // How many bytes worth of tuples did we write?
    pageid_t num_tuples_out;       // How many tuples did we write?
    pageid_t num_datapages_out;    // How many datapages?
    pageid_t bytes_in_small;       // How many bytes from the small input tree (for C0, we ignore tree overheads)?
    pageid_t bytes_in_small_delta; // How many bytes from the small input tree during this tick (for C0, we ignore tree overheads)?
    pageid_t bytes_collapsed;      // How many bytes disappeared due to tuple merges?
    pageid_t bytes_collapsed_delta;
    pageid_t num_tuples_in_small;  // Tuples from the small input?
    pageid_t bytes_in_large;       // Bytes from the large input?
    pageid_t num_tuples_in_large;  // Tuples from large input?
    bool active;
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
    if (mergeLevel == 0) {
      return c0;
    } else if (mergeLevel == 1) {
      return c1;
    } else if(mergeLevel == 2) {
      return c2;
    } else {
      abort();
    }
  }

private:
  pthread_mutex_t mut;
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
  mergeStats * c0;
  mergeStats * c1;
  mergeStats * c2;
  struct timespec last_throttle;
  pthread_mutex_t throttle_mut;
  pthread_mutex_t dummy_throttle_mut;
  pthread_cond_t dummy_throttle_cond;

};
#endif /* MERGESTATS_H_ */
