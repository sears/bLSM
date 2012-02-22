/*
 * simpleServer.h
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
 *  Created on: Aug 11, 2010
 *      Author: sears
 */

#ifndef SIMPLESERVER_H_
#define SIMPLESERVER_H_
#include "logstore.h"

class simpleServer {
public:
  void* worker(int /*handle*/);
  static const int DEFAULT_PORT = 32432;
  static const int DEFAULT_THREADS = 1000;

  simpleServer(blsm * ltable, int max_threads = DEFAULT_THREADS, int port = DEFAULT_PORT);
  bool acceptLoop();
  ~simpleServer();
private:
  blsm* ltable;
  int port;
  int max_threads;
  int * thread_fd;
  pthread_cond_t * thread_cond;
  pthread_mutex_t * thread_mut;
  pthread_t * thread;
};

#endif /* SIMPLESERVER_H_ */
