/*
 * mergeManager.cpp
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

#include "mergeManager.h"
#include "mergeStats.h"
#include "bLSM.h"
#include "math.h"
#include "time.h"
#include <stasis/transactional.h>

#define LEGACY_BACKPRESSURE

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
  still_running = false;
  pthread_cond_signal(&pp_cond);
  pthread_join(pp_thread, 0);
  pthread_join(update_progress_pthread, 0);
  pthread_cond_destroy(&pp_cond);
  delete c0;
  delete c1;
  delete c2;
}

void mergeManager::new_merge(int mergeLevel) {
  mergeStats * s = get_merge_stats(mergeLevel);
  if(s->merge_level == 0) {
    // target_size was set during startup
  } else if(s->merge_level == 1) {
    assert(c0->target_size);
    c1->target_size = (pageid_t)(*ltable->R() * (double)ltable->mean_c0_run_length);
    assert(c1->target_size);
    s->new_merge2();
  } else if(s->merge_level == 2) {
    // target_size is infinity...
    s->new_merge2();
  } else { abort(); }
#ifdef EXTENDED_STATS
  gettimeofday(&s->stats_start,0);
  double elapsed = (tv_to_double(&s->stats_start) - tv_to_double(&s->stats_sleep));
  s->stats_lifetime_elapsed += elapsed;
  (s->stats_elapsed) = elapsed;
  (s->stats_active)  = 0;

#endif
}
void mergeManager::set_c0_size(int64_t size) {
  assert(size);
  c0->target_size = size;
}
void mergeManager::update_progress(mergeStats * s, int delta) {
  s->delta += delta;

  if((!delta) || s->delta > UPDATE_PROGRESS_DELTA) {
    rwlc_writelock(ltable->header_mut);
    if(delta) {
      s->delta = 0;
      if(!s->need_tick) { s->need_tick = 1; }
    }
    if(s->merge_level == 2) {
      if(s->active) {
        s->in_progress =  ((double)(s->bytes_in_large + s->bytes_in_small)) / (double)(get_merge_stats(s->merge_level-1)->mergeable_size + s->base_size);
      } else {
        s->in_progress = 0;
      }
    } else if(s->merge_level == 1) { // C0-C1 merge (c0 is continuously growing...)
      if(s->active) {
	//        s->in_progress = ((double)(s->bytes_in_large+s->bytes_in_small)) / (double)(s->base_size+fmax(ltable->mean_c0_run_length,(double)s->bytes_in_small));
	s->in_progress = ((double)(s->bytes_in_large+s->bytes_in_small)) / (double)(s->base_size+fmax(c0->target_size,(double)s->bytes_in_small));
        //s->in_progress = ((double)(s->bytes_in_large+s->bytes_in_small)) / (((double)s->base_size)+(double)s->bytes_in_small);
      } else {
        s->in_progress = 0;
      }
    }

    if(s->target_size) {
      if(s->merge_level == 0) {
        s->out_progress = ((double)s->get_current_size()) / (double)ltable->mean_c0_run_length;
      } else {
        // To see what's going on in the following code, consider a
        // system with R = 3 and |C0| = 1.  (R is the number of rounds that a
        // C1-C2 merge takes during a bulk load, and also the ratios |C1|/|C0|
        // and |C2|/|C1|).

        // |Cn| is the size of a given tree component, normalized to the amount
        // of data that can be inserted into C0 (because of red-black tree
        // overheads, the effective capacity of C0 is a function of the average
        // tuple size and the physical memory allotted to C0).

        // Here is a trace of the costs of each round of merges during
        // a bulk load (where no data is overwritten):

        // Round | App writes | I/O performed by C0-C1  | I/O performed by C1-C2
        // 0     | 1          | 1                       |
        // 1     | 1          | 3 = 2 * 1 + 1           |   0 (idle)
        // 2     | 1          | 7 = 2 * 3 + 1           |
        // 3     | 1          | 1                       |
        // 4     | 1          | 3                       |   R (just a copy)
        // 5     | 1          | 7                       |
        // 6     | 1          | 1                       |
        // 7     | 1          | 3                       |   3 * R = 2 * R + R
        // 8     | 1          | 7                       |
        // 9     | 1          | 1                       |
        // 10    | 1          | 3                       |   2 ( 3 * R ) + R
        // 11    | 1          | 7                       |

        // i     | 1          | t(0) = 1                | u0 = u1 =...= uR = 0
        //       |            | i < R: t(i) = 1+2*t(i-1)| u(i) = 2 * u(i-R) + R
        //       |            | i >=R: t(i) = t(i%R)    |

        // Note that, at runtime, we can directly compute u(i) for the current
        // C1-C2 merge:

        // u_j <= (1 + \alpha) * (|c2| + |c1_mergeable|)                 [eq 1]

        // Where, \alpha is a constant between 0 and 1 that depends on the
        // number of and deletes in c1_mergeable.  For now, we assume it is 1.

        // Now, for each C1-C2 round, we want to split the total disk
        // work evenly amongst the C0-C1 rounds.  Thus, the amount
        // consumed by C1-C2 + C0-C1 should be equal (if possible) for
        // each pass over C0:

        // t(Rj) + u(Rj) = t(Rj+1) + u(Rj+1) = ... =  t(Rj+R-1) + u(Rj+R-1)

        // Work for set of C0 passes during j'th C1-C2 merge:
        //       work(j) = \sum_{k=0..R-1}{t(Rj+k)}+u_j                   [eq 2]

        // Ideally, we would like the amount of work performed during each
        // application visible round, i, to be the same throughout a given C1-C2
        // merger.  Unfortunately, there's no way to ensure that this will be the
        // case in general, as nothing prevents:

        // f(i) = (R * t[i] > u_j)

        // from being true.  Therefore, we partition the i according to f(i).
        // t(i) is monotonically increasing, so this creates two contiguous sets.

        // Let R' be the first i where f(i) is true, or R if no such i exists.
        //                                                               [eq 3]

        // Then:

        // work'(j) = work(j) - \sum{k=R'...R-1} t(Rj+k)                 [eq 4]

        // We now define u_j(i); the amount of progress we would like the C1-C2
        // merger to make in each application visible round.

        // The later rounds (where f(i) is true) already perform more work than
        // we'd like, so we set:

        // u_j(i) = 0 if f(i)                                           [eq 5a]

        // We evenly divide the remaining work:

        // t(i) + u_j(i) = work'(j) / R' if not f(i)

        // The t(i) are fixed, giving us R' equations in R' unknowns; solving
        // for u_j(i):

        //   u_j(i) = work'(j)/R' - t(i) if not f(i)                    [eq 5b]

        // Note 1:

        // If the tree is big enough, we compute R in a way that guarantees f(i)
        // is false.  We do not do this for small trees because it leads to R<3,
        // which negatively impacts throughput.  Therefore, we set R=3 and deal
        // with periodic transient increases in application-visible throughput.

        // Note 2:

        // If the working set is small, then C1 will not get bigger from one
        // merge to the next.  To cope with this, we compute delta_c1_c2 by
        // figuring out what the percent complete for c2 should be once C1 is
        // full, assuming we're performing a bulk load.  We set delta to the
        // difference between the current progress and the desired progress.  If
        // delta is negative, then the C1-C2 merge will still be ahead of the
        // C0-C1 merge at the end of this round, so we set delta to zero, which
        // effectively puts the C2 merger to sleep.

        // eq 2: Compute t[i] (from table) and initial value of work(j)
        int merge_count = (int)ceil(*ltable->R()-0.1);
        // next, estimate merge_number (i) based on the size of c1.
        // ( i = R * j + merge_number)
        int merge_number = (int)floor(((double)c1->base_size)/(double)ltable->mean_c0_run_length);

        s->out_progress = ((double)merge_number + s->in_progress) / (double) merge_count;

        // eq 1: Compute u_j

        if(c2->active && c1->mergeable_size) {
#ifdef LEGACY_BACKPRESSURE
          c1_c2_delta = c1->out_progress - c2->in_progress;
#else

          pageid_t u__j = (pageid_t)(2.0 * (double)(c2->base_size + c1->mergeable_size));


          double* t = (double*)malloc(sizeof(double) * merge_count);

          t[0] = ltable->mean_c0_run_length;
          double t__j = t[0];
          for(int i = 1; i < merge_count; i++) {
            t[i] = t[i-1] * 2.0 + ltable->mean_c0_run_length;
            t__j += t[i];
          }

          double work_j =  t__j + u__j;

          // eq 3: Compute R'
          int R_prime;
          {
            double frac_work = work_j / (double)merge_count;
            for(R_prime = 0; R_prime < merge_count; R_prime++) {
              if(t[R_prime] > frac_work) break;
            }
          }
          // eq 4: Compute work'
          double work_prime_j = work_j;
          for(int i = R_prime; i < merge_count; i++) {
            work_prime_j -= t[i];  // u_j[i] will be set to zero, so no need to subtract it off.
          }
          // eq 5a,b: Compute the u_j(i)'s for this C1-C2 round:

          double* u_j = (double*)malloc(sizeof(double) * merge_count);

          for(int i = 0; i < R_prime; i++) {
            u_j[i] = work_prime_j / R_prime - t[i];  // [5b]
          }
          for(int i = R_prime; i < merge_count; i++) {
            u_j[i] = 0;                              // [5a]
          }

          // we now have everything we need to know how far along we should expect
          // c1 and c2 to be at the beginning and end of this pass.
          double expected_c1_start_progress = ((double)merge_number) / (double)merge_count;
          double expected_c2_start_progress = 0.0;
          double expected_c1_end_progress = ((double)(merge_number+1)) / (double)merge_count;
          double expected_c2_end_progress = 0.0;
          for(int i = 0; i <= merge_number; i++) {
            if(i < merge_number) {
              expected_c2_start_progress += u_j[i];
            }
            expected_c2_end_progress += u_j[i];
          }

          expected_c2_start_progress /= u__j;
          expected_c2_end_progress /= u__j;

          assert(expected_c1_start_progress > -0.01 && expected_c1_start_progress < 1.01 &&
              expected_c2_start_progress > -0.01 && expected_c2_start_progress < 1.01 &&
              expected_c1_end_progress > -0.01 && expected_c1_end_progress < 1.01 &&
              expected_c2_end_progress > -0.01 && expected_c2_end_progress < 1.01 &&
              expected_c1_start_progress <= expected_c1_end_progress &&
              expected_c2_start_progress <= expected_c2_end_progress);

          double c1_scale_progress = (c1->out_progress - expected_c1_start_progress) / (expected_c1_end_progress - expected_c1_start_progress);
          double c2_scale_progress = (c2->in_progress - expected_c2_start_progress) / (expected_c2_end_progress - expected_c2_start_progress);
          c1_c2_delta = c1_scale_progress - c2_scale_progress;
#endif
        } else {
          c1_c2_delta = -0.02; // Elsewhere, we try to keep this number between -0.05 and -0.01.
        }
        // Appendix to analysis: Computation of t(i) and u(i) for bulk-loads

        // This is not used above (since both can be computed at runtime), but
        // may be of use for future analysis.

        // t(i) is easily computable, but u(i) is less straightforward:
        // u(i) = 2 * u(i-R) + R
        // u(i+R) = 2 * u(i) + R
        // Subtracting:

        // u(i+R) - u(i) = 2u(i-R) - 2u(i)
        // u(i+R) = u(i) + 2u(u-R)
        // u(0) = 0; u(R) = R
        // Characteristic polynomial:
        // r^n = r^(n-1) + 2r^(n-2)
        // divide by r^(n-2):
        // r^2 = r + 2 ; r^2 - r - 2 = 0
        // characteristic roots:
        // r = (1 +/- sqrt(1 + 8)) / 2 = 2 or -1
        // 2^n*C - D = a_n

        // 2^0 * C - D = 0; C = D.
        // 2 * C - D = R ; C = D = R.

        // So, u(n) = 2^n*R - R
        // or:
        // sum{u(i..i+R-1)} = 2*floor(i/3)^R - R.
      }
    }
#if EXTENDED_STATS
    struct timeval now;
    gettimeofday(&now, 0);
    double stats_elapsed_delta = tv_to_double(&now) - ts_to_double(&s->stats_last_tick);
    if(stats_elapsed_delta < 0.0000001) { stats_elapsed_delta = 0.0000001; }
    s->stats_lifetime_active += stats_elapsed_delta;
    s->stats_lifetime_elapsed += stats_elapsed_delta;
    s->stats_active += stats_elapsed_delta;
    s->stats_elapsed += stats_elapsed_delta;
    s->stats_lifetime_consumed += s->stats_bytes_in_small_delta;
    double stats_tau = 60.0; // number of seconds to look back for window computation.  (this is the expected mean residence time in an exponential decay model, so the units are not so intuitive...)
    double stats_decay = exp((0.0-stats_elapsed_delta)/stats_tau);

    double_to_ts(&s->stats_last_tick, tv_to_double(&now));

    double stats_window_bps = ((double)s->stats_bytes_in_small_delta) / (double)stats_elapsed_delta;

    s->stats_bps = (1.0-stats_decay) * stats_window_bps + stats_decay * s->stats_bps;

    s->stats_bytes_in_small_delta = 0;
#endif

    rwlc_unlock(ltable->header_mut);

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
 * bytes_consumed_by_merger = sum(stats_bytes_in_small_delta)
 */
void mergeManager::tick(mergeStats * s) {
  if(s && s->merge_level == 1) { // apply backpressure based on merge progress.
    if(s->need_tick) {
      s->need_tick = 0;
      // Only apply back pressure if next thread is not waiting on us.
      rwlc_readlock(ltable->header_mut);
      if(c1->mergeable_size && c2->active) {
        if(c1_c2_delta > -0.01) {
          DEBUG("Input is too far ahead.  Delta is %f\n", c1_c2_delta);
          double delta = c1_c2_delta;
          rwlc_unlock(ltable->header_mut);
          delta += 0.01; // delta > 0;
          double slp = 0.001 + delta;
          struct timespec sleeptime;
          DEBUG("\ndisk sleeping %0.6f tree_megabytes %0.3f\n", slp, ((double)ltable->tree_bytes)/(1024.0*1024.0));
          double_to_ts(&sleeptime,slp);
          nanosleep(&sleeptime, 0);
          update_progress(s, 0);
          s->need_tick = 1;
        } else {
          rwlc_unlock(ltable->header_mut);
        }
      } else {
        rwlc_unlock(ltable->header_mut);
      }
    }
  } else if((!s) || s->merge_level == 0) {
    // Simple backpressure algorithm based on how full C0 is.

    pageid_t cur_c0_sz;
    if(s) {
      // Is C0 bigger than is allowed?
      while((cur_c0_sz = s->get_current_size()) > ltable->max_c0_size) {  // can't use s->current_size, since this is the thread that maintains that number...
	printf("\nMEMORY OVERRUN!!!! SLEEP!!!!\n");
	struct timespec ts;
	double_to_ts(&ts, 0.1);
	nanosleep(&ts, 0);
      }
      // Linear backpressure model
      s->out_progress = ((double)cur_c0_sz)/((double)ltable->max_c0_size);
    } else {
      cur_c0_sz = c0->get_current_size();
    }
    double delta = ((double)cur_c0_sz)/(0.95*(double)ltable->max_c0_size); // 0 <= delta <= 1.111...
    delta -= 1.0;
    if(delta > 0.00005) {
      double slp = 0.001 + 5.0 * delta; //0.0015 < slp < 1.112111..
      if(!s) {
	//	printf("sleeping!\n");
      }
      DEBUG("\nmem sleeping %0.6f tree_megabytes %0.3f\n", slp, ((double)ltable->tree_bytes)/(1024.0*1024.0));
      struct timespec sleeptime;
      double_to_ts(&sleeptime, slp);
      DEBUG("%d Sleep C %f\n", s->merge_level, slp);
      nanosleep(&sleeptime, 0);
    }
  }
}

void mergeManager::read_tuple_from_small_component(int merge_level, dataTuple * tup) {
  if(tup) {
    mergeStats * s = get_merge_stats(merge_level);
    (s->num_tuples_in_small)++;
#if EXTENDED_STATS
    (s->stats_bytes_in_small_delta) += tup->byte_length();
#endif
    (s->bytes_in_small) += tup->byte_length();
    if(merge_level != 0) {
      update_progress(s, tup->byte_length());
    }
    tick(s);
  }
}
void mergeManager::read_tuple_from_large_component(int merge_level, int tuple_count, pageid_t byte_len) {
  if(tuple_count) {
    mergeStats * s = get_merge_stats(merge_level);
    s->num_tuples_in_large += tuple_count;
    s->bytes_in_large += byte_len;
    if(merge_level != 0) {
      update_progress(s, byte_len);
    }
  }
}

void mergeManager::wrote_tuple(int merge_level, dataTuple * tup) {
  mergeStats * s = get_merge_stats(merge_level);
  (s->num_tuples_out)++;
  (s->bytes_out) += tup->byte_length();
}

void mergeManager::finished_merge(int merge_level) {
  mergeStats *s = get_merge_stats(merge_level);
  update_progress(s, 0);
  s->active = false;
  if(merge_level != 0) {
    get_merge_stats(merge_level - 1)->mergeable_size = 0;
    update_progress(get_merge_stats(merge_level-1), 0);
  }
#if EXTENDED_STATS
  gettimeofday(&s->stats_done, 0);
  double elapsed = tv_to_double(&s->stats_done) - ts_to_double(&s->stats_last_tick);
  (s->stats_lifetime_active) += elapsed;
  (s->stats_lifetime_elapsed) += elapsed;
  (s->stats_elapsed) += elapsed;
  (s->stats_active) += elapsed;
  memcpy(&s->stats_sleep, &s->stats_done, sizeof(s->stats_sleep));
#define VERBOSE
#ifdef VERBOSE
  fprintf(stdout, "\n");
  s->pretty_print(stdout);
#endif
#endif
  update_progress(get_merge_stats(merge_level), 0);
}
void * mergeManager::update_progress_thread() {
  pthread_mutex_t dummy_mut;
  pthread_mutex_init(&dummy_mut, 0);

  while(still_running) {
    struct timeval tv;
    gettimeofday(&tv, 0);
    struct timespec ts;
    double_to_ts(&ts, tv_to_double(&tv)+0.1);
    pthread_cond_timedwait(&update_progress_cond, &dummy_mut, &ts);
    //    printf("Calling update progress\n");
    update_progress(c0,0);
  }
  return 0;
}
void * mergeManager::pretty_print_thread() {
  pthread_mutex_t dummy_mut;
  pthread_mutex_init(&dummy_mut, 0);

  while(still_running) {
    struct timeval tv;
    gettimeofday(&tv, 0);
    struct timespec ts;
    double_to_ts(&ts, tv_to_double(&tv)+1.01);
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
void * merge_manager_update_progress_thread(void * arg) {
  mergeManager * m = (mergeManager*)arg;
  return m->update_progress_thread();
}

double mergeManager::c1_c2_progress_delta() {
  return c1_c2_delta;
}

void mergeManager::init_helper(void) {
  struct timeval tv;
  c1_c2_delta = -0.02; // XXX move this magic number somewhere.  It's also in update_progress.
  gettimeofday(&tv, 0);

#if EXTENDED_STATS
  double_to_ts(&c0->stats_last_tick, tv_to_double(&tv));
  double_to_ts(&c1->stats_last_tick, tv_to_double(&tv));
  double_to_ts(&c2->stats_last_tick, tv_to_double(&tv));
#endif
  still_running = true;
  pthread_cond_init(&pp_cond, 0);
  pthread_create(&pp_thread, 0, merge_manager_pretty_print_thread, (void*)this);
  pthread_create(&update_progress_pthread, 0, merge_manager_update_progress_thread, (void*)this);
}

mergeManager::mergeManager(bLSM *ltable):
  UPDATE_PROGRESS_PERIOD(0.005),
  ltable(ltable) {
  c0 = new mergeStats(0, ltable ? ltable->max_c0_size : 10000000);
  c1 = new mergeStats(1, (int64_t)(ltable ? ((double)(ltable->max_c0_size) * *ltable->R()) : 100000000.0) );
  c2 = new mergeStats(2, 0);
  init_helper();
}
mergeManager::mergeManager(bLSM *ltable, int xid, recordid rid):
  UPDATE_PROGRESS_PERIOD(0.005),
  ltable(ltable) {
  marshalled_header h;
  Tread(xid, rid, &h);
  c0 = new mergeStats(xid, h.c0);
  c1 = new mergeStats(xid, h.c1);
  c2 = new mergeStats(xid, h.c2);
  init_helper();
}
recordid mergeManager::talloc(int xid) {
  marshalled_header h;
  recordid ret = Talloc(xid, sizeof(h));
  h.c0 = c0->talloc(xid);
  h.c1 = c1->talloc(xid);
  h.c2 = c2->talloc(xid);
  Tset(xid, ret, &h);
  return ret;
}
void mergeManager::marshal(int xid, recordid rid) {
  marshalled_header h;
  Tread(xid, rid, &h);
  c0->marshal(xid, h.c0);
  c1->marshal(xid, h.c1);
  c2->marshal(xid, h.c2);
}

void mergeManager::pretty_print(FILE * out) {

#if EXTENDED_STATS
  bLSM * lt = ltable;
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
  pageid_t mb = 1024 * 1024;
  fprintf(out,"[merge progress MB/s window (lifetime)]: app [%s %6lldMB tot %6lldMB cur ~ %3.0f%%/%3.0f%% %6.1fsec %4.1f (%4.1f)] %s %s [%s %3.0f%% ~ %3.0f%% %4.1f (%4.1f)] %s %s [%s %3.0f%% %4.1f (%4.1f)] %s ",
      c0->active ? "RUN" : "---", (long long)(c0->stats_lifetime_consumed / mb), (long long)(c0->get_current_size() / mb), 100.0 * c0->out_progress, 100.0 * ((double)c0->get_current_size())/(double)ltable->max_c0_size, c0->stats_lifetime_elapsed, c0->stats_bps/((double)mb), c0->stats_lifetime_consumed/(((double)mb)*c0->stats_lifetime_elapsed),
      have_c0 ? "C0" : "..",
      have_c0m ? "C0'" : "...",
      c1->active ? "RUN" : "---", 100.0 * c1->in_progress, 100.0 * c1->out_progress, c1->stats_bps/((double)mb), c1->stats_lifetime_consumed/(((double)mb)*c1->stats_lifetime_elapsed),
      have_c1 ? "C1" : "..",
      have_c1m ? "C1'" : "...",
      c2->active ? "RUN" : "---", 100.0 * c2->in_progress, c2->stats_bps/((double)mb), c2->stats_lifetime_consumed/(((double)mb)*c2->stats_lifetime_elapsed),
      have_c2 ? "C2" : "..");
#endif
//#define PP_SIZES
#ifdef PP_SIZES
  {
    pageid_t mb = 1024 * 1024;
    fprintf(out, "[target cur base in_small in_large, out, mergeable] C0 %4lld %4lld %4lld %4lld %4lld %4lld %4lld ",
            c0->target_size/mb, c0->current_size/mb, c0->base_size/mb, c0->bytes_in_small/mb,
            c0->bytes_in_large/mb, c0->bytes_out/mb, c0->mergeable_size/mb);

    fprintf(out, "C1 %4lld %4lld %4lld %4lld %4lld %4lld %4lld ",
            c1->target_size/mb, c1->current_size/mb, c1->base_size/mb, c1->bytes_in_small/mb,
            c1->bytes_in_large/mb, c1->bytes_out/mb, c1->mergeable_size/mb);

    fprintf(out, "C2 ---- %4lld %4lld %4lld %4lld %4lld %4lld ",
            /*----*/            c2->current_size/mb, c2->base_size/mb, c2->bytes_in_small/mb,
            c2->bytes_in_large/mb, c2->bytes_out/mb, c2->mergeable_size/mb);
  }
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
#if 0 // XXX would like to bring this back somehow...
  assert((!c1->active) || (c1->in_progress >= -0.01 && c1->in_progress < 1.02));
  assert((!c2->active) || (c2->in_progress >= -0.01 && c2->in_progress < 1.10));
#endif

  fprintf(out, "\r");
}
