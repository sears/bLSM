/*
 * check_testAndSet.cpp
 *
 *  Created on: Sep 16, 2010
 *      Author: sears
 */

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "datapage.h"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include <stasis/transactional.h>
#undef begin
#undef end

#include "check_util.h"

#define NUM_THREADS 128

unsigned char vals[NUM_THREADS];
logtable<datatuple>* ltbl;

int myucharcmp(const void * ap, const void * bp) {
  unsigned char a = *(unsigned char*)ap;
  unsigned char b = *(unsigned char*)bp;
  return (int)a - (int)b;
}

void * worker(void * idp) {
  unsigned char id = *(unsigned char*)idp;
  bool succ = false;
  while(!succ) {
    unsigned char key = random() % NUM_THREADS;
    printf("id = %d key = %d\n", (int)id, (int)key);
    datatuple * dt = datatuple::create(&key, sizeof(key), &id, sizeof(id));
    datatuple * dtdelete = datatuple::create(&key, sizeof(key));
    succ = ltbl->testAndSetTuple(dt, dtdelete);
    datatuple::freetuple(dt);
    datatuple::freetuple(dtdelete);
    vals[id] = key;
  }
  return 0;
}

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    logtable<datatuple>::init_stasis();
    int xid = Tbegin();

    merge_scheduler mscheduler;
    logtable<datatuple> ltable(1000, 10000, 5);
    ltbl = &ltable;

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    int lindex = mscheduler.addlogtable(&ltable);
    ltable.setMergeData(mscheduler.getMergeData(lindex));

    mscheduler.startlogtable(lindex, 10 * 1024 * 1024);

    pthread_t *threads = (pthread_t*)malloc(NUM_THREADS * sizeof(pthread_t));

    for(int i = 0; i < NUM_THREADS; i++) {
      unsigned char * x = (unsigned char*)malloc(sizeof(unsigned char));
      *x = i;
      int err = pthread_create(&threads[i], 0, worker, x);
      if(err) { errno = err; perror("Couldn't spawn thread"); abort(); }
    }
    for(int i = 0; i < NUM_THREADS; i++) {
      pthread_join(threads[i], 0);
    }

    qsort(vals, NUM_THREADS, sizeof(unsigned char), &myucharcmp);

    for(int i = 0; i < NUM_THREADS; i++) {
      assert(((unsigned char)i) == vals[i]);
    }

    mscheduler.shutdown();
    logtable<datatuple>::deinit_stasis();

    printf("\npass\n");
}

/** @test
 */
int main()
{
    insertProbeIter(5000);



    return 0;
}

