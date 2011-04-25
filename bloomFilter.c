#include <assert.h>
#include <math.h>
#include <stdio.h>
#include "bloomFilter.h"
/**
   Variable names:
     m: number of bloom filter bits
     n: number of bloom filter entries
     k: number of hash functions = ln(2) * (m/n)
     c: m/n
     f: false positive rate =               (1/2)^k ~= 0.6185)^(m/n)  ;
            taking lof_0.6185 of both sides: k log_0.6185(1/2) = m/n ; 
            applying change of base:         k log(1/2) / log(6.128) = m / n
	    (but that's not useful; this is:)

                 f ~= 0.6185 ^ (m/n)
		 log_0.6185(f) = m/n
		 log(f) / log(0.6185) = m / n
		 m = n log f / log 0.6185
     p: probability a given bit is 1 ~= e^(-kn/m)
 */

static uint64_t bloom_filter_calc_num_buckets(uint64_t num_expected_items, double false_positive_rate) {
  //m = n log f / log 0.6185
  return ((uint64_t) ceil(((double)num_expected_items) * log(false_positive_rate) / log(0.6185)));
  // m = - n ln f / ln 2 ^ 2 = - n ln f / 0.4804 = - n log f / (0.4343 * 0.4804) = -n log f / 0.2086
}
static int bloom_filter_calc_num_functions(uint64_t num_expected_items, uint64_t num_buckets) {
  // k = ln(2) * (m/n)
  int ret = floor((log(2) / log(exp(1.0))) * ((double) num_buckets) / (double) num_expected_items);
  if(ret == 0) {
    return 1;
  } else {
    return ret;
  }
}
static double bloom_filter_current_false_positive_rate(uint64_t actual_number_of_items, uint64_t num_buckets) {
  // 0.6185^(m/n)
  return pow(0.6185, ((double)num_buckets)/(double)actual_number_of_items);
}

struct bloom_filter_t {
  uint64_t (*func_a)(const char *, int);
  uint64_t (*func_b)(const char *, int);
  uint64_t num_expected_items;
  double   desired_false_positive_rate;
  uint64_t num_buckets;
  uint8_t   * buckets;
  uint64_t num_functions;
  uint64_t*result_scratch_space;
  uint64_t actual_number_of_items;
};
bloom_filter_t * bloom_filter_create(uint64_t(*func_a)(const char*,int),
					    uint64_t(*func_b)(const char*,int),
					    uint64_t num_expected_items,
					    double false_positive_rate) {
  bloom_filter_t * ret = malloc(sizeof(*ret));
  ret->func_a = func_a;
  ret->func_b = func_b;
  ret->num_expected_items = num_expected_items;
  ret->desired_false_positive_rate = false_positive_rate;
  ret->num_buckets = bloom_filter_calc_num_buckets(ret->num_expected_items, ret->desired_false_positive_rate);
  ret->buckets = calloc((ret->num_buckets / 8) + ((ret->num_buckets % 8 == 0) ? 0 : 1), 1);
  ret->num_functions = bloom_filter_calc_num_functions(ret->num_expected_items, ret->num_buckets);
  ret->result_scratch_space = malloc(sizeof(*ret->result_scratch_space) * ret->num_functions);
  ret->actual_number_of_items = 0;
  return ret;
}
void bloom_filter_destroy(bloom_filter_t* bf) {
  free(bf->buckets);
  free(bf->result_scratch_space);
  free(bf);
}
// XXX this uses %.  It would be better if it used &, but that would potentially double the memory we use.  #define a flag.
static void bloom_filter_calc_functions(bloom_filter_t * bf, uint64_t* results, const char * key, int keylen) {
  uint64_t fa = bf->func_a(key, keylen);
  uint64_t fb = bf->func_b(key, keylen);

  results[0] = (fa + fb) % bf->num_buckets;
  for(int i = 1; i < bf->num_functions; i++) {
    results[i] = (results[i-1] + fb ) % bf->num_buckets;
  }
}

static const uint8_t bloom_filter_bit_masks[] = { 1, 2, 4, 8, 16, 32, 64, 128 };
static void bloom_filter_set_bit(bloom_filter_t *bf, uint64_t bit) {
  uint64_t array_offset = bit >> 3;
  uint8_t  bit_number = bit & 7;

  assert(bit < bf->num_buckets);

  bf->buckets[array_offset] |= bloom_filter_bit_masks[bit_number];

}
/**
   @return 0 if the bit is not set, true otherwise.
 */
static uint8_t bloom_filter_get_bit(bloom_filter_t *bf, uint64_t bit) {
  uint64_t array_offset = bit >> 3;
  uint8_t bit_number = bit & 7;

  assert(bit < bf->num_buckets);

  return bf->buckets[array_offset] & bloom_filter_bit_masks[bit_number];
}
void bloom_filter_insert(bloom_filter_t * bf, const char *key, int len) {
  bloom_filter_calc_functions(bf, bf->result_scratch_space, key, len);
  for(int i = 0; i < bf->num_functions; i++) {
    bloom_filter_set_bit(bf, bf->result_scratch_space[i]);
  }
  bf->actual_number_of_items++;
}
int bloom_filter_lookup(bloom_filter_t * bf, const char * key, int len) {
  int ret = 1;
  uint64_t * scratch = malloc(sizeof(*scratch) * bf->num_functions);
  bloom_filter_calc_functions(bf, scratch, key, len);
  for(int i = 0; i < bf->num_functions; i++) {
    ret  = ret && bloom_filter_get_bit(bf, scratch[i]);
  }
  free(scratch);
  return ret;
}

void bloom_filter_print_stats(bloom_filter_t * bf) {
  printf("Design capacity %lld design false positive %f\n"
         "Current item count %lld current false positive %f\n"
         "Number of buckets %lld (%f MB), number of hash functions %lld\n",
         bf->num_expected_items, bf->desired_false_positive_rate,
         bf->actual_number_of_items,
         bloom_filter_current_false_positive_rate(bf->actual_number_of_items, bf->num_buckets),
         bf->num_buckets, ((double)bf->num_buckets) / (8.0 * 1024.0 * 1024.0), bf->num_functions);
}
