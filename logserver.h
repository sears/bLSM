/*
 * logserver.h
 *
 * Copyright 2009-2012 Yahoo! Inc.
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
 */
#ifndef _LOGSERVER_H_
#define _LOGSERVER_H_

#include <queue>
#include <vector>

#include "datatuple.h"

#include <stasis/transactional.h>
#include <pthread.h>

#include "logstore.h"

#define STATS_ENABLED 1

#ifdef STATS_ENABLED
#include <sys/time.h>
#include <time.h>
#include <map>
#endif

struct pthread_item;

struct pthread_data {
    std::queue<pthread_item> *idleth_queue;
    std::queue<int> *ready_queue;
    std::queue<int> *work_queue;
    pthread_mutex_t * qlock;

    pthread_cond_t *selcond;
    int* self_pipe;
    
    pthread_cond_t * th_cond;
    pthread_mutex_t * th_mut;
    
    int *workitem; //id of the socket to work

    logtable *ltable;
    bool *sys_alive;

    #ifdef STATS_ENABLED
    int num_reqs;
    struct timeval start_tv, stop_tv;
    double work_time;
    std::map<std::string, int> num_reqsc;
    std::map<std::string, double> work_timec;
    #endif
    
};

struct pthread_item{
    pthread_t * th_handle;
    pthread_data *data;
};

struct serverth_data
{
    int *server_socket;
    int server_port;
    std::queue<pthread_item> *idleth_queue;
    std::queue<int> *ready_queue;

    pthread_cond_t *selcond;
    int * self_pipe;

    pthread_mutex_t *qlock;
};

void * thread_work_fn( void *);

class logserver
{
public:
    logserver(int nthreads, int server_port){
        this->nthreads = nthreads;
        this->server_port = server_port;

        qlock = new pthread_mutex_t;
        pthread_mutex_init(qlock,0);

        ltable = 0;

        #ifdef STATS_ENABLED        
        num_selevents = 0;
        num_selcalls = 0;
        #endif


    }

    ~logserver()
        {
            delete qlock;
        }
    
    void startserver(logtable *ltable);

    void stopserver();
    
private:

    //main loop of server
    //accept connections, assign jobs to threads
    void eventLoop();

    int server_port;
    
    size_t nthreads;

    bool sys_alive;
    
    int serversocket; //server socket file descriptor

    std::queue<int> ready_queue; //connections to go inside select
    std::queue<int> work_queue;  //connections to be processed by worker threads
    std::queue<pthread_item> idleth_queue;
    pthread_mutex_t *qlock;

    pthread_t server_thread;
    serverth_data *sdata;
    pthread_cond_t *selcond; //server loop cond
    int * self_pipe; // write a byte to self_pipe[1] to wake up select().
    std::vector<pthread_item *> th_list; // list of threads

    logtable *ltable;

    #ifdef STATS_ENABLED
    int num_reqs;
    int num_selevents;
    int num_selcalls;
    struct timeval start_tv, stop_tv;
    double tot_threadwork_time;
    double tot_time;
    #endif

    
};


#endif
