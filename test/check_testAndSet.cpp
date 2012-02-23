/*
 * check_testAndSet.cpp
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
 *  Created on: Sep 16, 2010
 *      Author: sears
 */

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "dataPage.h"
#include "mergeScheduler.h"
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

bLSM * ltable;

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
    dataTuple * dt = dataTuple::create(&key, sizeof(key), &id, sizeof(id));
    dataTuple * dtdelete = dataTuple::create(&key, sizeof(key));
    succ = ltable->testAndSetTuple(dt, dtdelete);
    dataTuple::freetuple(dt);
    dataTuple::freetuple(dtdelete);
    vals[id] = key;
  }
  return 0;
}

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");
    system("rm -rf stasis_log/");

    bLSM::init_stasis();
    int xid = Tbegin();

    ltable = new bLSM(10 * 1024 * 1024, 1000, 10000, 5);

    mergeScheduler mscheduler(ltable);

    recordid table_root = ltable->allocTable(xid);

    Tcommit(xid);

    mscheduler.start();

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
    delete ltable;
    bLSM::deinit_stasis();

    printf("\npass\n");
}

/** @test
 */
int main()
{
    insertProbeIter(5000);



    return 0;
}

