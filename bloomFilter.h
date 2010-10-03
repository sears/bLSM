#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <stasis/common.h>

BEGIN_C_DECLS

typedef struct bloom_filter_t bloom_filter_t;

/**
   @return 0 if there is not enough memory, or some other error occurred; a pointer to the new bloom filter otherwise.
 */
bloom_filter_t * bloom_filter_create(uint64_t(*hash_func_a)(const char*,int), uint64_t(*hash_func_b)(const char*,int),
				     uint64_t num_expected_items, double false_positive_rate);

void bloom_filter_destroy(bloom_filter_t*);

void bloom_filter_insert(bloom_filter_t * bf, const char* key, int len);
/**
   @return 1 if the value might be in the bloom filter, 0 otherwise
 */
int bloom_filter_lookup(bloom_filter_t * bf, const char* key, int len);

void bloom_filter_print_stats(bloom_filter_t * bf);

END_C_DECLS

#endif
