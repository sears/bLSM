/*
 * tcpclient_noop.cpp
 *
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
 *  Created on: Feb 23, 2010
 *      Author: sears
 */

#include "..tcpclient.h"
#include "../network.h"
#include "datatuple.h"

void usage(char * argv[]) {
    fprintf(stderr, "usage %s numthreads threadopcount [host [port]]\n", argv[0]);
}

#include "../servers/native/util/util_main.h"
#include <sys/time.h>
int threadopcount;

int thrargc;
char ** thrargv;

void * worker (void * arg) {
  logstore_handle_t * l = util_open_conn(thrargc-2, thrargv+2);
  for(int i = 0; i < threadopcount; i++) {
    datatuple * ret = logstore_client_op(l, OP_DBG_NOOP);
    if(ret == NULL) {
        perror("No-op failed"); return (void*)-1;
    } else {
        datatuple::freetuple(ret);
    }
  }
  logstore_client_close(l);
  return 0;
}

int main(int argc, char * argv[]) {
    if(argc < 3) {
      usage(argv);
      return 1;
    }
    thrargc = argc;
    thrargv = argv;

    int numthreads = atoi(argv[1]);
    threadopcount = (atoi(argv[2])/numthreads);

    pthread_t * threads = (pthread_t*)malloc(sizeof(*threads) * numthreads);

    struct timeval start, stop;
    gettimeofday(&start, 0);
    for(int i = 0; i < numthreads; i++) {
      pthread_create(&threads[i], 0, worker, 0);
    }
    int had_err = 0;
    for(int i = 0; i < numthreads; i++) {
      void * err;
      pthread_join(threads[i], &err);
      if(err) {
        had_err = 1;
      }
    }
    gettimeofday(&stop,0);
    if(!had_err) {
      double startf = ((double)start.tv_sec) + (double)start.tv_usec / 1000000.0;
      double stopf = ((double)stop.tv_sec) + (double)stop.tv_usec / 1000000.0;
      double elapsed = stopf -startf;
      printf("%5d threads, %6d ops/thread, %6.2f seconds, %7.1f ops/thread-second, %6.1f ops/sec\n",
          numthreads, threadopcount, elapsed, ((double)threadopcount)/elapsed, (((double)numthreads)*(double)threadopcount)/elapsed);
    }
    free(threads);
    return 0;

}

