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
      c1_queueSize = c1_queueSize > s->bytes_out ? c1_queueSize : s->bytes_out;
    } else if(s->merge_level == 2) {
      c2_queueSize = s->bytes_in_small;
    } else { abort(); }
    pretty_print(stdout);
  }
  pthread_mutex_unlock(&mut);
}
void mergeManager::set_c0_size(int64_t size) {
  c0_queueSize = size;
}
void mergeManager::tick(mergeStats * s, bool done) {
  if(s->merge_level == 0) {
    pthread_mutex_lock(&throttle_mut);
  }
    // throttle?
  if(s->bytes_in_small_delta > c0_queueSize / 10000) {
    if(s->merge_level == 0) {
      struct timeval now;
      gettimeofday(&now, 0);
      double elapsed_delta = tv_to_double(&now) - ts_to_double(&last_throttle);
      pageid_t bytes_written_delta = (s->bytes_in_small_delta - s->bytes_collapsed_delta);
      double min_throughput = 0.0; // don't throttle below 100 kilobytes / sec
      double max_throughput = 10.0 * 1024.0 * 1024.0;
      double c0_badness = (double)((c0_totalConsumed + bytes_written_delta - c1_totalConsumed)- c0_queueSize) / ((double)c0_queueSize);
      double raw_throughput = ((double)bytes_written_delta)/elapsed_delta;
      if(raw_throughput > max_throughput || c0_badness > 0) {
        //double target_throughput = min_throughput / (c0_badness); // * c0_badness * c0_badness);
        double target_throughput;
        if(c0_badness > 0) {
          target_throughput = (max_throughput - min_throughput) * (1.0-sqrt(sqrt(c0_badness))) + min_throughput;
        } else {
          target_throughput = max_throughput;
        }
        double target_elapsed = ((double)bytes_written_delta)/target_throughput;
        //printf("Worked %6.1f (target %6.1f)\n", elapsed_delta, target_elapsed);
        if(target_elapsed > elapsed_delta) {
          struct timespec sleep_until;
          double_to_ts(&sleep_until, ts_to_double(&last_throttle) + target_elapsed);
          //fprintf(stdout, "Throttling for %6.1f seconds\n", target_elapsed - (double)elapsed_delta);
          last_throttle_seconds = target_elapsed - (double)elapsed_delta;
          last_elapsed_seconds = target_elapsed;
          throttle_seconds += last_throttle_seconds;
          elapsed_seconds += last_elapsed_seconds;
          pthread_mutex_lock(&dummy_throttle_mut);
          pthread_cond_timedwait(&dummy_throttle_cond, &dummy_throttle_mut, &sleep_until);
          pthread_mutex_unlock(&dummy_throttle_mut);
          memcpy(&last_throttle, &sleep_until, sizeof(sleep_until));
        } else {
          double_to_ts(&last_throttle, tv_to_double(&now));
        }
      } else {
        double_to_ts(&last_throttle, tv_to_double(&now));
      }
    }
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

mergeManager::mergeManager(void *ltable):
  ltable(ltable),
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
  fprintf(out, "Throttle: %6.1f%% (cur) %6.1f%% (overall) ", 100.0*(last_throttle_seconds/(last_elapsed_seconds)), 100.0*(throttle_seconds/(elapsed_seconds)));
  fprintf(out, "C0 size %4lld collapsed %4lld resident %4lld ",
               2*c0_queueSize/mb,
               c0_totalCollapsed/mb,
               (c0_totalConsumed - (c0_totalCollapsed + c1_totalConsumed))/mb);
  fprintf(out, "C1 size %4lld collapsed %4lld resident %4lld ",
               2*c1_queueSize/mb,
               c1_totalCollapsed/mb,
               (c1_totalConsumed - (c1_totalCollapsed + c2_totalConsumed))/mb);
  fprintf(out, "C2 size %4lld collapsed %4lld ",
               2*c2_queueSize/mb, c2_totalCollapsed/mb);
  fprintf(out, "C1 MB/s (eff; active) %6.1f  C2 MB/s %6.1f\r",
                ((double)c1_totalConsumed)/((double)c1_totalWorktime),
                ((double)c2_totalConsumed)/((double)c2_totalWorktime));
  fflush(out);
}
