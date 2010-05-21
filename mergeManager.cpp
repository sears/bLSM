/*
 * mergeManager.cpp
 *
 *  Created on: May 19, 2010
 *      Author: sears
 */

#include "mergeManager.h"
#include "mergeStats.h"
#include "logstore.h"
#include "math.h"
mergeStats* mergeManager:: newMergeStats(int mergeLevel) {
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

mergeManager::~mergeManager() {
  pthread_mutex_destroy(&mut);
  pthread_mutex_destroy(&throttle_mut);
  pthread_mutex_destroy(&dummy_throttle_mut);
  pthread_cond_destroy(&dummy_throttle_cond);
  delete c0;
  delete c1;
  delete c2;
}

void mergeManager::new_merge(mergeStats * s) {
  pthread_mutex_lock(&mut);
  if(s->merge_count) {
    if(s->merge_level == 0) {
      // queueSize was set during startup
    } else if(s->merge_level == 1) {
      c1_queueSize = (pageid_t)(*ltable->R() * (double)c0_queueSize); //c1_queueSize > s->bytes_out ? c1_queueSize : s->bytes_out;
    } else if(s->merge_level == 2) {
    } else { abort(); }
  }
  pthread_mutex_unlock(&mut);
}
void mergeManager::set_c0_size(int64_t size) {
  c0_queueSize = size;
}

/**
 * This function is invoked periodically by the merge threads.  It updates mergeManager's statistics, and applies
 * backpressure as necessary.
 *
 * Here is the backpressure algorithm.
 *
 * We want to maintain these two invariants:
 *   - for each byte consumed by the app->c0 threads, a byte is consumed by the c0->c1 merge thread.
 *   - for each byte consumed by the c0->c1 thread, the c1->c2 thread consumes a byte
 *
 * More concretely (and taking into account varying values of R):
 *   capacity(C_i) - current_size(C_i) >= size(C_i_mergeable) - bytes_consumed_by_next_merger
 *
 * where:
 *   capacity c0 = c0_queue_size
 *   capacity c1 = c1_queue_size
 *
 *   current_size(c_i) = sum(bytes_out_delta) - sum(bytes_in_large_delta)
 *
 * bytes_consumed_by_merger = sum(bytes_in_small_delta)
 */
void mergeManager::tick(mergeStats * s, bool done) {
  pageid_t tick_length_bytes = 1024*1024;
  if(done || (s->bytes_in_small_delta > tick_length_bytes)) {
    pthread_mutex_lock(&mut);
    struct timeval now;
    gettimeofday(&now, 0);
    double elapsed_delta = tv_to_double(&now) - ts_to_double(&s->last_tick);
    double bps = (double)s->bytes_in_small_delta / (double)elapsed_delta;

    pageid_t current_size = s->bytes_out - s->bytes_in_large;

    int64_t overshoot;
    int64_t overshoot_fudge = (int64_t)((double)c0_queueSize * 0.1);
    do{
      overshoot = 0;
      if(s->merge_level == 0) {
        if(done) {
          //       c0->bytes_in_small = 0;
        } else if(c0->mergeable_size) {
          overshoot = (overshoot_fudge + c0->mergeable_size - c1->bytes_in_small) //   amount left to process
                      - (c0_queueSize - current_size);            // - room for more insertions
        }
      } else if (s->merge_level == 1) {
        if(done) {
          c0->mergeable_size = 0;
          c1->bytes_in_small = 0;
        } else if(/*c1_queueSize && */c1->mergeable_size) {
          overshoot = (c1->mergeable_size - c2->bytes_in_small)
                      - (c1_queueSize - current_size);
        }
      } else if (s->merge_level == 2) {
        if(done) {
          c1->mergeable_size = 0;
          c2->bytes_in_small = 0;
        }
        // Never throttle this merge.
      }
      static int num_skipped = 0;
      if(num_skipped == 10) {
        printf("#%d mbps %6.1f overshoot %9lld current_size = %9lld ",s->merge_level, bps / (1024.0*1024.0), overshoot, current_size);
        pretty_print(stdout);
        num_skipped = 0;
      }
      num_skipped ++;
      if(overshoot > 0) {
        // throttle
        // it took "elapsed" seconds to process "tick_length_bytes" mb
        double sleeptime = (double)overshoot / bps;  // 2 is a fudge factor

        struct timespec sleep_until;
        if(sleeptime > 1) { sleeptime = 1; }
        double_to_ts(&sleep_until, sleeptime + tv_to_double(&now));
//        printf("\nMerge thread %d Overshoot: %lld Throttle %6f\n", s->merge_level, overshoot, sleeptime);
//        pthread_mutex_lock(&dummy_throttle_mut);
        pthread_cond_timedwait(&dummy_throttle_cond, &mut, &sleep_until);
//        pthread_mutex_unlock(&dummy_throttle_mut);
        gettimeofday(&now, 0);
      }
    } while(overshoot > 0);
    memcpy(&s->last_tick, &now, sizeof(now));

    s->bytes_in_small_delta = 0;
//    pretty_print(stdout);
    pthread_mutex_unlock(&mut);
  }
}

mergeManager::mergeManager(logtable<datatuple> *ltable):
  ltable(ltable),
  c0_queueSize(0),
  c1_queueSize(0),
//  c2_queueSize(0),
  c0(new mergeStats(this, 0)),
  c1(new mergeStats(this, 1)),
  c2(new mergeStats(this, 2)) {
  pthread_mutex_init(&mut, 0);
  pthread_mutex_init(&throttle_mut, 0);
  pthread_mutex_init(&dummy_throttle_mut, 0);
  pthread_cond_init(&dummy_throttle_cond, 0);
  struct timeval tv;
  gettimeofday(&tv, 0);
  double_to_ts(&c0->last_tick, tv_to_double(&tv));
  double_to_ts(&c1->last_tick, tv_to_double(&tv));
  double_to_ts(&c2->last_tick, tv_to_double(&tv));
}

void mergeManager::pretty_print(FILE * out) {
  pageid_t mb = 1024 * 1024;
  logtable<datatuple> * lt = (logtable<datatuple>*)ltable;
  bool have_c0  = false;
  bool have_c0m = false;
  bool have_c1  = false;
  bool have_c1m = false;
  bool have_c2  = false;
  if(lt) {
    pthread_mutex_lock(&lt->header_mut);
    have_c0  = NULL != lt->get_tree_c0();
    have_c0m = NULL != lt->get_tree_c0_mergeable();
    have_c1  = NULL != lt->get_tree_c1();
    have_c1m = NULL != lt->get_tree_c1_mergeable() ;
    have_c2  = NULL != lt->get_tree_c2();
    pthread_mutex_unlock(&lt->header_mut);
  }
  fprintf(out,"[%s] %s %s [%s] %s %s [%s] %s ",
      c0->active ? "RUN" : "---",
      have_c0 ? "C0" : "..",
      have_c0m ? "C0'" : "...",
      c1->active ? "RUN" : "---",
      have_c1 ? "C1" : "..",
      have_c1m ? "C1'" : "...",
      c2->active ? "RUN" : "---",
      have_c2 ? "C2" : "..");
  fprintf(out, "[size in small/large, out, mergeable] C0 %4lld %4lld %4lld %4lld %4lld ", c0_queueSize/mb, c0->bytes_in_small/mb, c0->bytes_in_large/mb, c0->bytes_out/mb, c0->mergeable_size/mb);
  fprintf(out, "C1 %4lld %4lld %4lld %4lld %4lld ", c1_queueSize/mb, c1->bytes_in_small/mb, c1->bytes_in_large/mb, c1->bytes_out/mb, c1->mergeable_size/mb);
  fprintf(out, "C2 .... %4lld %4lld %4lld %4lld ", c2->bytes_in_small/mb, c2->bytes_in_large/mb, c2->bytes_out/mb, c2->mergeable_size/mb);
//  fprintf(out, "Throttle: %6.1f%% (cur) %6.1f%% (overall) ", 100.0*(last_throttle_seconds/(last_elapsed_seconds)), 100.0*(throttle_seconds/(elapsed_seconds)));
//  fprintf(out, "C0 size %4lld resident %4lld ",
//               2*c0_queueSize/mb,
//               (c0->bytes_out - c0->bytes_in_large)/mb);
//  fprintf(out, "C1 size %4lld resident %4lld\r",
//               2*c1_queueSize/mb,
//               (c1->bytes_out - c1->bytes_in_large)/mb);
//  fprintf(out, "C2 size %4lld\r",
//               2*c2_queueSize/mb);
//  fprintf(out, "C1 MB/s (eff; active) %6.1f  C2 MB/s %6.1f\r",
//                ((double)c1_totalConsumed)/((double)c1_totalWorktime),
//                ((double)c2_totalConsumed)/((double)c2_totalWorktime));
  fflush(out);
  fprintf(out, "\r");
}
