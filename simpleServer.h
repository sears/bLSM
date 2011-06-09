/*
 * simpleServer.h
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

  simpleServer(logtable * ltable, int max_threads = DEFAULT_THREADS, int port = DEFAULT_PORT);
  bool acceptLoop();
  ~simpleServer();
private:
  logtable* ltable;
  int port;
  int max_threads;
  int * thread_fd;
  pthread_cond_t * thread_cond;
  pthread_mutex_t * thread_mut;
  pthread_t * thread;
};

#endif /* SIMPLESERVER_H_ */
