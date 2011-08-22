/*
 * check_bloomFilter.c
 *
 *  Created on: Oct 2, 2010
 *      Author: sears
 */
#include <stasis/util/hashFunctions.h>
#include <bloomFilter.h>
#include <assert.h>
#include <stdio.h>
#include <sys/time.h>
#include <stasis/util/crc32.h>

/*
 * This file can test CRC and FNV-1 based hash functions.  Based on early experiments:
 *
 * CRC32 insert/lookup: 11/13 seconds, 1.1% false positive
 * FNV-1 insert/lookup: 8/9 seconds,   2.8% false positive
 *
 * Expected false positive rate is 1%.
 */

static uint64_t hash_a(const char* a, int len) {
  return stasis_crc32(a,len,0xcafebabe);
}

static uint64_t hash_b(const char* a, int len) {
  return stasis_crc32(a,len,0xdeadbeef);
}
static uint64_t hash_a_fnv(const char* a, int len) {
  return stasis_util_hash_fnv_1_uint32_t((const byte*)a, len);
}
static uint64_t hash_b_fnv(const char* a, int len) {
  return stasis_util_hash_fnv_1_uint64_t((const byte*)a, len);
}

static char * malloc_random_string(int group) {
  char * str = 0;
  int strlen = 0;
  while(!strlen) strlen = 128 + (rand() & 127);
  str = (char*)malloc(strlen + 1);
  str[0] = group;

  for(int i = 1; i < strlen; i++) {
    str[i] = (rand() & 128) + 1;
  }
  str[strlen] = 0;
  return str;
}

int main(int argc, char * argv[]) {
  (void)hash_a; (void)hash_b;
  (void)hash_a_fnv; (void)hash_b_fnv;

  const int num_inserts = 1000000;
  char ** strings = (char**)malloc(num_inserts * sizeof(char*));
  uint64_t sum_strlen = 0;
  struct timeval start, stop;
  gettimeofday(&start, 0);
  printf("seed: %lld\n", (long long)start.tv_sec);
  srand(start.tv_sec);
  for(int i = 0; i < num_inserts; i++) {
    strings[i] = malloc_random_string(1);
    sum_strlen += strlen(strings[i]);
  }
  gettimeofday(&stop,0);
  printf("Generated strings in %d seconds.  Mean string length: %f\n", (int)(stop.tv_sec - start.tv_sec), (double)(sum_strlen)/(double)num_inserts);

  bloom_filter_t * bf = bloom_filter_create(hash_a, hash_b, num_inserts, 0.01);
  bloom_filter_print_stats(bf);
  gettimeofday(&start, 0);
  for(int i = 0; i < num_inserts; i++) {
    bloom_filter_insert(bf,strings[i], strlen(strings[i]));
  }
  gettimeofday(&stop, 0);
  printf("Inserted strings in %d seconds.\n", (int)(stop.tv_sec - start.tv_sec));

  gettimeofday(&start, 0);
  for(int i = 0; i < num_inserts; i++) {
    assert(bloom_filter_lookup(bf, strings[i], strlen(strings[i])));
  }
  gettimeofday(&stop, 0);
  printf("Looked up strings in %d seconds.\n", (int)(stop.tv_sec - start.tv_sec));
  bloom_filter_print_stats(bf);

  uint64_t false_positives = 0;
  gettimeofday(&start, 0);
  for(int i = 0; i < num_inserts; i++) {
    char * str = malloc_random_string(2);
    if(bloom_filter_lookup(bf, str, strlen(str))) {
      false_positives ++;
    }
    assert(bloom_filter_lookup(bf, strings[i], strlen(strings[i])));
    free(str);
  }
  gettimeofday(&stop, 0);
  printf("Generated and looked up non-existant strings in %d seconds\n"
         "false positive rate was %lf\n", (int)(stop.tv_sec - start.tv_sec),
         ((double)false_positives)/(double)num_inserts);

  return 0;
}
