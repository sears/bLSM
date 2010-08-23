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
#include "time.h"
mergeStats* mergeManager:: get_merge_stats(int mergeLevel) {
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
  pthread_mutex_destroy(&throttle_mut);
  pthread_cond_destroy(&throttle_wokeup_cond);
  delete c0;
  delete c1;
  delete c2;
  still_running = false;
  pthread_cond_signal(&pp_cond);
  pthread_join(pp_thread, 0);
  pthread_cond_destroy(&pp_cond);
}

void mergeManager::new_merge(int mergeLevel) {
  mergeStats * s = get_merge_stats(mergeLevel);
  if(s->merge_level == 0) {
    // target_size was set during startup
  } else if(s->merge_level == 1) {
    assert(c0->target_size);
    c1->target_size = (pageid_t)(*ltable->R() * (double)c0->target_size);
    assert(c1->target_size);
  } else if(s->merge_level == 2) {
    // target_size is infinity...
  } else { abort(); }
  s->new_merge2();
}
void mergeManager::set_c0_size(int64_t size) {
  c0->target_size = size;
}

void mergeManager::update_progress(mergeStats * s, int delta) {
  s->delta += delta;
  s->mini_delta += delta;
  {
    if(/*s->merge_level < 2*/ s->merge_level == 1 && s->mergeable_size && delta) {
      int64_t effective_max_delta = (int64_t)(UPDATE_PROGRESS_PERIOD * s->bps);

//      if(s->merge_level == 0) { s->base_size = ltable->tree_bytes; }

      if(s->mini_delta > effective_max_delta) {
        struct timeval now;
        gettimeofday(&now, 0);
        double now_double = tv_to_double(&now);
        double elapsed_delta =  now_double - ts_to_double(&s->last_mini_tick);
        double slp = UPDATE_PROGRESS_PERIOD - elapsed_delta;
        if(slp > 0.001) {
//          struct timespec sleeptime;
//          double_to_ts(&sleeptime, slp);
//          nanosleep(&sleeptime, 0);
//          printf("%d Sleep A %f\n", s->merge_level, slp);
        }
        double_to_ts(&s->last_mini_tick, now_double);
        s->mini_delta = 0;
      }
    }
  }
  if((!delta) || s->delta > UPDATE_PROGRESS_DELTA) {
    if(delta) {
      rwlc_writelock(ltable->header_mut);
      s->delta = 0;
      if(!s->need_tick) { s->need_tick = 1; }
    }
    if(s->merge_level == 2
#ifdef NO_SNOWSHOVEL
        || s->merge_level == 1
#endif
    ) {
      if(s->active) {
        s->in_progress =  ((double)(s->bytes_in_large + s->bytes_in_small)) / (double)(get_merge_stats(s->merge_level-1)->mergeable_size + s->base_size);
      } else {
        s->in_progress = 0;
      }
    } else if(s->merge_level == 1) { // C0-C1 merge (c0 is continuously growing...)
      if(s->active) {
        s->in_progress = ((double)(s->bytes_in_large+s->bytes_in_small)) / (double)(s->base_size+ltable->mean_c0_effective_size);
        if(s->in_progress > 0.95) { s->in_progress = 0.95; }
        assert(s->in_progress > -0.01 && s->in_progress < 1.02);
      } else {
        s->in_progress = 0;
      }
    }
    if(s->merge_level != 2) {
      if(s->mergeable_size) {
        s->out_progress = ((double)s->current_size + (double)s->base_size) / (double)s->target_size;
      } else {
        s->out_progress = 0.0;
      }
    }

#ifdef NO_SNOWSHOVEL
    s->current_size = s->base_size + s->bytes_out - s->bytes_in_large;
    s->out_progress = ((double)s->current_size) / (double)s->target_size;
#else
    if(delta) {
      if(s->merge_level == 0) {
        s->current_size = ltable->tree_bytes; // we need to track the number of bytes consumed by the merger; this data is not present in s, so fall back on ltable's aggregate.
      } else {
        s->current_size = s->base_size + s->bytes_out - s->bytes_in_large;
      }
      s->out_progress = ((double)s->current_size) / (double)s->target_size;
    }
#endif
    struct timeval now;
    gettimeofday(&now, 0);
    double elapsed_delta = tv_to_double(&now) - ts_to_double(&s->last_tick);
    if(elapsed_delta < 0.0000001) { elapsed_delta = 0.0000001; }
    s->lifetime_elapsed += elapsed_delta;
    s->lifetime_consumed += s->bytes_in_small_delta;
    double tau = 60.0; // number of seconds to look back for window computation.  (this is the expected mean residence time in an exponential decay model, so the units are not so intuitive...)
    double decay = exp((0.0-elapsed_delta)/tau);

    double_to_ts(&s->last_tick, tv_to_double(&now));

    double window_bps = ((double)s->bytes_in_small_delta) / (double)elapsed_delta;

    s->bps = (1.0-decay) * window_bps + decay * s->bps;

    s->bytes_in_small_delta = 0;

    if(delta) rwlc_unlock(ltable->header_mut);

  }
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
void mergeManager::tick(mergeStats * s, bool block, bool force) {
#define PRINT_SKIP 10000
  if(force || s->need_tick) {

    if(block) {
      if(s->merge_level == 0) {
//        pthread_mutex_lock(&ltable->tick_mut);
        rwlc_readlock(ltable->header_mut);

        while(sleeping[s->merge_level]) {
          abort();
          rwlc_unlock(ltable->header_mut);
//          pthread_cond_wait(&throttle_wokeup_cond, &ltable->tick_mut);
          rwlc_writelock(ltable->header_mut);
        }
      } else {
        rwlc_readlock(ltable->header_mut);
        while(sleeping[s->merge_level]) {
          abort();  // if we're asleep, didn't this thread make us sleep???
          rwlc_cond_wait(&throttle_wokeup_cond, ltable->header_mut);
        }
      }
#ifdef NO_SNOWSHOVEL
      bool snowshovel = false;
#else
      bool snowshovel = true;
#endif
      if((!snowshovel) || s->merge_level == 1) { // apply backpressure based on merge progress.
      int64_t overshoot = 0;
      int64_t overshoot2 = 0;
      int64_t raw_overshoot = 0;

      /* model the effect of linux + stasis' write caches; at the end
         of this merge, we need to force up to FORCE_INTERVAL bytes
         after we think we're done writing the next component. */
      double skew = 0.0;

      int64_t overshoot_fudge = (int64_t)((s->out_progress-skew) * ((double)FORCE_INTERVAL)/(1.0-skew));
      /* model the effect of amortizing this computation: we could
         become this much more overshot if we don't act now. */
      int64_t overshoot_fudge2 = UPDATE_PROGRESS_DELTA; //(int64_t)(((double)UPDATE_PROGRESS_PERIOD) * s->bps / 1000.0);
      /* multiply by 2 for good measure.  These are 'soft' walls, and
         still let writes trickle through.  Once we've exausted the
         fudge factors, we'll hit a hard wall, and stop writes
         entirely, so it's better to start thottling too early than
         too late. */
      overshoot_fudge *= 2;
      overshoot_fudge2 *= 4;
      int spin = 0;
      double total_sleep = 0.0;
      do{
        overshoot = 0;
        overshoot2 = 0;
        raw_overshoot = 0;
        double bps;
        // This needs to be here (and not in update_progress), since the other guy's in_progress changes while we sleep.
        if(s->merge_level == 0) {
          abort();
          if(!(c1->active && c0->mergeable_size)) { overshoot_fudge = 0; overshoot_fudge2 = 0; }
          raw_overshoot = (int64_t)(((double)c0->target_size) * (c0->out_progress - c1->in_progress));
          overshoot = raw_overshoot + overshoot_fudge;
          overshoot2 = raw_overshoot + overshoot_fudge2;
          bps = c1->bps;
        } else if (s->merge_level == 1) {
          if(!(c2->active && c1->mergeable_size)) { overshoot_fudge = 0; overshoot_fudge2 = 0; }
          raw_overshoot = (int64_t)(((double)c1->target_size) * (c1->out_progress - c2->in_progress));
          overshoot = raw_overshoot + overshoot_fudge;
          overshoot2 = raw_overshoot + overshoot_fudge2;
          bps = c2->bps;
          if(!c1->mergeable_size) { overshoot = 0; overshoot2 = 0; }
        }

//#define PP_THREAD_INFO
  #ifdef PP_THREAD_INFO
        printf("\nMerge thread %d %6f %6f Overshoot: raw=%lld, d=%lld eff=%lld Throttle min(1, %6f) spin %d, total_sleep %6.3f\n", s->merge_level, c0_out_progress, c0_c1_in_progress, raw_overshoot, overshoot_fudge, overshoot, -1.0, spin, total_sleep);
  #endif
        bool one_threshold = (overshoot > 0 || overshoot2 > 0) || (raw_overshoot > 0);
        bool two_threshold = (overshoot > 0 || overshoot2 > 0) && (raw_overshoot > 0);

        if(one_threshold && (two_threshold || total_sleep < 0.01)) {
          // throttle
          // it took "elapsed" seconds to process "tick_length_bytes" mb
          double sleeptime = 2.0 * fmax((double)overshoot,(double)overshoot2) / bps;
          double max_c0_sleep = 0.1;
          double min_c0_sleep = 0.01;
          double max_c1_sleep = 0.5;
          double min_c1_sleep = 0.1;
          double max_sleep = s->merge_level == 0 ? max_c0_sleep : max_c1_sleep;
          double min_sleep = s->merge_level == 0 ? min_c0_sleep : min_c1_sleep;

          if(sleeptime < min_sleep) { sleeptime = min_sleep; }
          if(sleeptime > max_sleep) { sleeptime = max_sleep; }

          spin ++;
          total_sleep += sleeptime;

          if((spin > 40) || (total_sleep > (max_sleep * 20.0))) {
            printf("\nMerge thread %d c0->out=%f c1->in=%f c1->out=%f c2->in=%f\n", s->merge_level, c0->out_progress, c1->in_progress, c1->out_progress, c2->in_progress);
            printf("\nMerge thread %d Overshoot: raw=%lld, d=%lld eff=%lld eff2=%lld Throttle min(1, %6f) spin %d, total_sleep %6.3f\n", s->merge_level, (long long)raw_overshoot, (long long)overshoot_fudge, (long long)overshoot, (long long)overshoot2, sleeptime, spin, total_sleep);
          }

          sleeping[s->merge_level] = true;
          if(s->merge_level == 0) abort();
          rwlc_unlock(ltable->header_mut);
          struct timespec ts;
          double_to_ts(&ts, sleeptime);
          nanosleep(&ts, 0);
          //          printf("%d Sleep B %f\n", s->merge_level, sleeptime);

          //          rwlc_writelock(ltable->header_mut);
          rwlc_readlock(ltable->header_mut);
          sleeping[s->merge_level] = false;
          pthread_cond_broadcast(&throttle_wokeup_cond);
          if(s->merge_level == 0) { update_progress(c1, 0); }
          if(s->merge_level == 1) { update_progress(c2, 0); }
        } else {
          if(overshoot > 0 || overshoot2 > 0) {
            s->need_tick ++;
            if(s->need_tick > 500) { printf("need tick %d\n", s->need_tick); }
          } else {
            s->need_tick = 0;
          }
          break;
        }
      } while(1);
     } else if(s->merge_level == 0) {
      while(/*s->current_size*/ltable->tree_bytes > ltable->max_c0_size) {
        rwlc_unlock(ltable->header_mut);
        printf("\nMEMORY OVERRUN!!!! SLEEP!!!!\n");
        struct timespec ts;
        double_to_ts(&ts, 0.1);
        nanosleep(&ts, 0);
        rwlc_readlock(ltable->header_mut);
      }
      double delta = ((double)ltable->tree_bytes)/(0.9*(double)ltable->max_c0_size); // 0 <= delta <= 1.1111  // - (0.9 * (double)ltable->max_c0_size);
      delta -= 1.0;
      if(delta > 0.00005) {
        double slp = 0.001 + 10.0 * delta; //delta / (double)(ltable->max_c0_size);
        DEBUG("\nsleeping %0.6f tree_megabytes %0.3f\n", slp, ((double)ltable->tree_bytes)/(1024.0*1024.0));
        struct timespec sleeptime;
        double_to_ts(&sleeptime, slp);
        rwlc_unlock(ltable->header_mut);
        DEBUG("%d Sleep C %f\n", s->merge_level, slp);

        nanosleep(&sleeptime, 0);
        rwlc_readlock(ltable->header_mut);
      }
    }
      rwlc_unlock(ltable->header_mut);
    }
  }
}

void mergeManager::read_tuple_from_small_component(int merge_level, datatuple * tup) {
  if(tup) {
    mergeStats * s = get_merge_stats(merge_level);
    (s->num_tuples_in_small)++;
    (s->bytes_in_small_delta) += tup->byte_length();
    (s->bytes_in_small) += tup->byte_length();
    update_progress(s, tup->byte_length());
    tick(s, true);
  }
}
void mergeManager::read_tuple_from_large_component(int merge_level, datatuple * tup) {
  if(tup) {
    mergeStats * s = get_merge_stats(merge_level);
    s->num_tuples_in_large++;
    s->bytes_in_large += tup->byte_length();
//    tick(s, false); // would be no-op; we just reduced current_size.
    update_progress(s, tup->byte_length());
    tick(s,false);
  }
}

void mergeManager::wrote_tuple(int merge_level, datatuple * tup) {
  mergeStats * s = get_merge_stats(merge_level);
  (s->num_tuples_out)++;
  (s->bytes_out) += tup->byte_length();

  // XXX this just updates stat's current size, and (perhaps) does a pretty print.  It should not need a mutex.
  //  update_progress(s, tup->byte_length());
  //  tick(s, false);
}

void mergeManager::finished_merge(int merge_level) {
  update_progress(get_merge_stats(merge_level), 0);
  tick(get_merge_stats(merge_level), false, true);  // XXX what does this do???
  get_merge_stats(merge_level)->active = false;
  if(merge_level != 0) {
    get_merge_stats(merge_level - 1)->mergeable_size = 0;
    update_progress(get_merge_stats(merge_level-1), 0);
  }
  gettimeofday(&get_merge_stats(merge_level)->done, 0);
  update_progress(get_merge_stats(merge_level), 0);
}

void * mergeManager::pretty_print_thread() {
  pthread_mutex_t dummy_mut;
  pthread_mutex_init(&dummy_mut, 0);

  while(still_running) {
    struct timeval tv;
    gettimeofday(&tv, 0);
    struct timespec ts;
    double_to_ts(&ts, tv_to_double(&tv)+1);
    pthread_cond_timedwait(&pp_cond, &dummy_mut, &ts);
    if(ltable) {
      rwlc_readlock(ltable->header_mut);
      pretty_print(stdout);
      rwlc_unlock(ltable->header_mut);
    }
  }
  printf("\n");
  return 0;
}
void * merge_manager_pretty_print_thread(void * arg) {
  mergeManager * m = (mergeManager*)arg;
  return m->pretty_print_thread();
}

mergeManager::mergeManager(logtable<datatuple> *ltable):
  UPDATE_PROGRESS_PERIOD(0.005),
  ltable(ltable),
  c0(new mergeStats(0, ltable ? ltable->max_c0_size : 10000000)),
  c1(new mergeStats(1, (int64_t)(ltable ? ((double)(ltable->max_c0_size) * *ltable->R()) : 100000000.0) )),
  c2(new mergeStats(2, 0)) {
  pthread_mutex_init(&throttle_mut, 0);
  pthread_cond_init(&throttle_wokeup_cond, 0);
  struct timeval tv;
  gettimeofday(&tv, 0);
  sleeping[0] = false;
  sleeping[1] = false;
  sleeping[2] = false;
  double_to_ts(&c0->last_tick, tv_to_double(&tv));
  double_to_ts(&c1->last_tick, tv_to_double(&tv));
  double_to_ts(&c2->last_tick, tv_to_double(&tv));
  still_running = true;
  pthread_cond_init(&pp_cond, 0);
  pthread_create(&pp_thread, 0, merge_manager_pretty_print_thread, (void*)this);
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
    have_c0  = NULL != lt->get_tree_c0();
    have_c0m = NULL != lt->get_tree_c0_mergeable();
    have_c1  = NULL != lt->get_tree_c1();
    have_c1m = NULL != lt->get_tree_c1_mergeable() ;
    have_c2  = NULL != lt->get_tree_c2();
  }

  fprintf(out,"[merge progress MB/s window (lifetime)]: app [%s %6lldMB ~ %3.0f%% %6.1fsec %4.1f (%4.1f)] %s %s [%s %3.0f%% ~ %3.0f%% %4.1f (%4.1f)] %s %s [%s %3.0f%% %4.1f (%4.1f)] %s ",
      c0->active ? "RUN" : "---", (long long)(c0->lifetime_consumed / mb), 100.0 * c0->out_progress, c0->lifetime_elapsed, c0->bps/((double)mb), c0->lifetime_consumed/(((double)mb)*c0->lifetime_elapsed),
      have_c0 ? "C0" : "..",
      have_c0m ? "C0'" : "...",
      c1->active ? "RUN" : "---", 100.0 * c1->in_progress, 100.0 * c1->out_progress, c1->bps/((double)mb), c1->lifetime_consumed/(((double)mb)*c1->lifetime_elapsed),
      have_c1 ? "C1" : "..",
      have_c1m ? "C1'" : "...",
      c2->active ? "RUN" : "---", 100.0 * c2->in_progress, c2->bps/((double)mb), c2->lifetime_consumed/(((double)mb)*c2->lifetime_elapsed),
      have_c2 ? "C2" : "..");
#define PP_SIZES
#ifdef PP_SIZES
  fprintf(out, "[target cur base in_small in_large, out, mergeable] C0 %4lld %4lld %4lld %4lld %4lld %4lld %4lld ",
          c0->target_size/mb, c0->current_size/mb, c0->base_size/mb, c0->bytes_in_small/mb,
          c0->bytes_in_large/mb, c0->bytes_out/mb, c0->mergeable_size/mb);

  fprintf(out, "C1 %4lld %4lld %4lld %4lld %4lld %4lld %4lld ",
          c1->target_size/mb, c1->current_size/mb, c1->base_size/mb, c1->bytes_in_small/mb,
          c1->bytes_in_large/mb, c1->bytes_out/mb, c1->mergeable_size/mb);

  fprintf(out, "C2 ---- %4lld %4lld %4lld %4lld %4lld %4lld ",
          /*----*/            c2->current_size/mb, c2->base_size/mb, c2->bytes_in_small/mb,
          c2->bytes_in_large/mb, c2->bytes_out/mb, c2->mergeable_size/mb);
#endif
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
#ifdef NO_SNOWSHOVEL
  assert((!c1->active) || (c1->in_progress >= -0.01 && c1->in_progress < 1.02));
#endif
  assert((!c2->active) || (c2->in_progress >= -0.01 && c2->in_progress < 1.02));

  fprintf(out, "\r");
}
