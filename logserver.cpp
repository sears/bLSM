/*
 * diskTreeComponent.cpp
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
#include <sstream>

#include "logserver.h"
#include "datatuple.h"
#include "merger.h"

#include "logstore.h"
#include "requestDispatch.h"

#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <errno.h>

#include <unistd.h>
#include <fcntl.h>

void *serverLoop(void *args);
void logserver::startserver(blsm *ltable)
{
    sys_alive = true;
    this->ltable = ltable;

    selcond = new pthread_cond_t;
    pthread_cond_init(selcond, 0);
    
    self_pipe = (int*)malloc(2 * sizeof(int));
    pipe(self_pipe);

    fcntl(self_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(self_pipe[1], F_SETFL, O_NONBLOCK);


    //initialize threads
    for(size_t i=0; i<nthreads; i++)
    {
        struct pthread_item *worker_th = new pthread_item;
        th_list.push_back(worker_th);
        
        worker_th->th_handle = new pthread_t;
        struct pthread_data *worker_data = new pthread_data;
        worker_th->data = worker_data;

        worker_data->idleth_queue = &idleth_queue;        
        worker_data->ready_queue = &ready_queue;
        worker_data->work_queue = &work_queue;

#ifdef STATS_ENABLED
        worker_data->num_reqs = 0;
#endif


        worker_data->qlock = qlock;

        worker_data->selcond = selcond;
        worker_data->self_pipe = self_pipe;
        
        worker_data->th_cond = new pthread_cond_t;
        pthread_cond_init(worker_data->th_cond,0);
        
        worker_data->th_mut = new pthread_mutex_t;
        pthread_mutex_init(worker_data->th_mut,0);

        worker_data->workitem = new int;
        *(worker_data->workitem) = -1;

        worker_data->ltable = ltable;

        worker_data->sys_alive = &sys_alive;
        
        pthread_create(worker_th->th_handle, 0, thread_work_fn, worker_th);

        idleth_queue.push(*worker_th);
    }

    //start server socket
    sdata = new serverth_data;
    sdata->server_socket = &serversocket;
    sdata->server_port = server_port;
    sdata->idleth_queue = &idleth_queue;
    sdata->ready_queue = &ready_queue;
    sdata->selcond = selcond;
    sdata->self_pipe = self_pipe;
    sdata->qlock = qlock;
    
    pthread_create(&server_thread, 0, serverLoop, sdata);

    //start monitoring loop
    eventLoop();

}

void logserver::stopserver()
{
    //close the server socket
    //stops receiving data on the server socket
    shutdown(serversocket, 0);
    
    //wait for all threads to be idle
    while(idleth_queue.size() != nthreads)
        sleep(1);

    #ifdef STATS_ENABLED
    printf("\n\nSTATISTICS\n");
    std::map<std::string, int> num_reqsc;
    std::map<std::string, double> work_timec;
    #endif
    
    //set the system running flag to false
    sys_alive = false;
    for(size_t i=0; i<nthreads; i++)
    {
        pthread_item *idle_th = th_list[i];
        
        //wake up the thread 
        pthread_mutex_lock(idle_th->data->th_mut);        
        pthread_cond_signal(idle_th->data->th_cond);
        pthread_mutex_unlock(idle_th->data->th_mut);
        //wait for it to join
        pthread_join(*(idle_th->th_handle), 0);
        //free the thread variables
        pthread_cond_destroy(idle_th->data->th_cond);

        #ifdef STATS_ENABLED
        if(i == 0)
        {
            tot_threadwork_time = 0;
            num_reqs = 0;
        }

        tot_threadwork_time += idle_th->data->work_time;
        num_reqs += idle_th->data->num_reqs;

        printf("thread %llu: work_time %.3f\t#calls %d\tavg req process time:\t%.3f\n",
               (unsigned long long)i,
               idle_th->data->work_time,
               idle_th->data->num_reqs,
               (( idle_th->data->num_reqs == 0 ) ? 0 : idle_th->data->work_time / idle_th->data->num_reqs)
               );

        for(std::map<std::string, int>::const_iterator itr = idle_th->data->num_reqsc.begin();
            itr != idle_th->data->num_reqsc.end(); itr++)
        {
            std::string ckey = (*itr).first;
            printf("\t%s\t%d\t%.3f\t%.3f\n", ckey.c_str(), (*itr).second, idle_th->data->work_timec[ckey],
                   idle_th->data->work_timec[ckey] / (*itr).second);

            if(num_reqsc.find(ckey) == num_reqsc.end()){
                num_reqsc[ckey] = 0;
                work_timec[ckey] = 0;                
            }
            num_reqsc[ckey] += (*itr).second;
            work_timec[ckey] += idle_th->data->work_timec[ckey];
        }
        #endif
        
        delete idle_th->data->th_cond;
        delete idle_th->data->th_mut;
        delete idle_th->data->workitem;
        delete idle_th->data;
        delete idle_th->th_handle;        
    }

    th_list.clear();

    close(self_pipe[0]);
    close(self_pipe[1]);
    free(self_pipe);

    #ifdef STATS_ENABLED

    printf("\n\nAggregated Stats:\n");
    for(std::map<std::string, int>::const_iterator itr = num_reqsc.begin();
        itr != num_reqsc.end(); itr++)
    {
        std::string ckey = (*itr).first;
        printf("\t%s\t%d\t%.3f\t%.3f\n", ckey.c_str(), (*itr).second, work_timec[ckey],
               work_timec[ckey] / (*itr).second);
    }
    
    tot_time = (stop_tv.tv_sec - start_tv.tv_sec) * 1000 +
               (stop_tv.tv_usec / 1000 - start_tv.tv_usec / 1000);
    
    printf("\ntot time:\t%f\n",tot_time);
    printf("tot work time:\t%f\n", tot_threadwork_time);       
    printf("load avg:\t%f\n", tot_threadwork_time / tot_time);

    printf("tot num reqs\t%d\n", num_reqs);
    if(num_reqs!= 0)
    {
        printf("tot work time / num reqs:\t%.3f\n", tot_threadwork_time / num_reqs);
        printf("tot time / num reqs:\t%.3f\n", tot_time / num_reqs );
    }
    #endif

    return;
}

void logserver::eventLoop()
{

    fd_set readfs;
    
    int maxfd;

    struct timespec   ts;
    std::vector<int> sel_list;
    sel_list.push_back(self_pipe[0]);
         
//    struct timeval no_timeout = { 0, 0 };

    while(true)
    {
        //clear readset
        FD_ZERO(&readfs);
        maxfd = -1;

        ts.tv_nsec = 250000; //nanosec
        ts.tv_sec = 0;
        
        //update select set
        pthread_mutex_lock(qlock);

        assert(sel_list.size() != 0);  // self_pipe[0] should always be there.
        if(sel_list.size() == 1)
        {
			assert(sel_list[0] == self_pipe[0]);
            while(ready_queue.size() == 0)
                pthread_cond_wait(selcond, qlock);
        }
        
        //new connections + processed conns are in ready_queue
        //add them to select list
        while(ready_queue.size() > 0)
        {
            sel_list.push_back(ready_queue.front());
            ready_queue.pop();
        }
        pthread_mutex_unlock(qlock);

        //ready select set
        for(std::vector<int>::const_iterator itr=sel_list.begin();
            itr != sel_list.end(); itr++)
        {
            if(maxfd < *itr)
                maxfd = *itr;
            FD_SET(*itr, &readfs);
        }

        //select events
        int sel_res = select(maxfd+1, &readfs, NULL, NULL, NULL); //&no_timeout);// &Timeout);

        #ifdef STATS_ENABLED
        if(num_selcalls == 0)
            gettimeofday(&start_tv, 0);        
        
        num_selevents += sel_res;
        num_selcalls++;
        #endif

        pthread_mutex_lock(qlock);
        for(size_t i=0; i<sel_list.size(); i++ )
        {
            int currsock = sel_list[i];
            DEBUG("processing request from currsock = %d\n", currsock);
            if (FD_ISSET(currsock, &readfs))
            {
            	if(currsock == self_pipe[0]) {
            		DEBUG("currsock = %d is self_pipe\n", currsock);
            		char gunk;
					int n;
					while(1 == (n = read(self_pipe[0], &gunk, 1))) { }
					if(n == -1) {
						if(errno != EAGAIN) {
							perror("Couldn't read self_pipe!");
							abort();
						}
					} else {
						assert(n == 1);
					}
            	} else {
            		if(idleth_queue.size() > 0) //assign the job to an indle thread
					{
						DEBUG("push currsock = %d onto idleth\n", currsock);  fflush(stdout);
						pthread_item idle_th = idleth_queue.front();
						idleth_queue.pop();

						//wake up the thread to do work
						pthread_mutex_lock(idle_th.data->th_mut);
						//set the job of the idle thread
						assert(currsock != -1);
						*(idle_th.data->workitem) = currsock;
						pthread_cond_signal(idle_th.data->th_cond);
						pthread_mutex_unlock(idle_th.data->th_mut);
					}
					else
					{
						DEBUG("push currsock = %d onto workqueue\n", currsock); fflush(stdout);
						//insert the given element to the work queue
						work_queue.push(currsock);
					}
            	}
                
                //remove from the sel_list
            	if(currsock != self_pipe[0]) {
            		sel_list.erase(sel_list.begin()+i);
            		i--;
            	}
            } else {
            	DEBUG("not set\n");
            }
        }

        pthread_mutex_unlock(qlock);

        #ifdef STATS_ENABLED
        gettimeofday(&stop_tv, 0);
        #endif
        
    }
    
}

void *serverLoop(void *args)
{

    serverth_data *sdata = (serverth_data*)args;
    
    int sockfd; //socket descriptor
    struct sockaddr_in serv_addr;
    struct sockaddr_in cli_addr;
    int newsockfd; //newly created 

    //open a socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        printf("ERROR opening socket\n");
        return 0;
    }
    
    bzero((char *) &serv_addr, sizeof(serv_addr));     
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(sdata->server_port);
    
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
    {
        printf("ERROR on binding.\n");
        return 0;
    }
    
    //start listening on the server socket
    //second arg is the max number of connections waiting in queue
    if(listen(sockfd,SOMAXCONN)==-1)
    {
        printf("ERROR on listen.\n");
         return 0;
    }

    printf("LSM Server listening...\n");

    *(sdata->server_socket) = sockfd;
    int flag, result;
    while(true)
    {
        socklen_t clilen = sizeof(cli_addr);

        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) 
        {
            printf("ERROR on accept.\n");
            return 0; // we probably want to continue instead of return here (when not debugging)
        }

        flag = 1;
        result = setsockopt(newsockfd,            /* socket affected */
                            IPPROTO_TCP,     /* set option at TCP level */
                            TCP_NODELAY,     /* name of option */
                            (char *) &flag,  /* the cast is historical
                                                cruft */
                            sizeof(int));    /* length of option value */
        if (result < 0)
        {
            printf("ERROR on setting socket option TCP_NODELAY.\n");
            return 0; 
        }        

        char clientip[20];
        inet_ntop(AF_INET, (void*) &(cli_addr.sin_addr), clientip, 20);
//        printf("Connection from:\t%s\n", clientip);

        pthread_mutex_lock(sdata->qlock);

        //insert the given element to the ready queue
        sdata->ready_queue->push(newsockfd);

   /*     if(sdata->ready_queue->size() == 1) //signal the event loop
            pthread_cond_signal(sdata->selcond);
        else */ if(sdata->ready_queue->size() == 1) { // signal the event loop
        	pthread_cond_signal(sdata->selcond);
        	char gunk = 42;
        	int ret;
        	if(-1 == (ret = write(sdata->self_pipe[1], &gunk, 1))) {
        		if(errno != EAGAIN) {
        			perror("Couldn't write to pipe!");
        			abort();
        		}
        	} else {
        		assert(ret == 1);
        	}
        }
        pthread_mutex_unlock(sdata->qlock);
    }
}


void * thread_work_fn( void * args)
{
    pthread_item * item = (pthread_item *) args;

    pthread_mutex_lock(item->data->th_mut);
    while(true)
    {        
        while(*(item->data->workitem) == -1)
        {
            if(!*(item->data->sys_alive))
                break;
            pthread_cond_wait(item->data->th_cond, item->data->th_mut); //wait for job
        }

        
        #ifdef STATS_ENABLED
        gettimeofday(& (item->data->start_tv), 0);
        std::ostringstream ostr;
        ostr << *(item->data->workitem) << "_";
        #endif
        
        if(!*(item->data->sys_alive))
        {
            break;
        }

        // XXX move this logserver error handling logic into requestDispatch.cpp

        //step 1: read the opcode
        network_op_t opcode = readopfromsocket(*(item->data->workitem), LOGSTORE_CLIENT_REQUEST);
        if(opcode == LOGSTORE_CONN_CLOSED_ERROR) {
        	opcode = OP_DONE;
        	printf("Broken client closed connection uncleanly\n");
        }

        int err = opcode == OP_DONE || opiserror(opcode); //close the conn on failure

        //step 2: read the first tuple from client
        datatuple *tuple = 0, *tuple2 = 0;
        if(!err) { tuple  = readtuplefromsocket(*(item->data->workitem), &err); }
        //        read the second tuple from client
        if(!err) { tuple2 = readtuplefromsocket(*(item->data->workitem), &err); }

        //step 3: process the tuple
		if(!err) { err = requestDispatch<int>::dispatch_request(opcode, tuple, tuple2, item->data->ltable, *(item->data->workitem)); }

        //free the tuple
        if(tuple)  datatuple::freetuple(tuple);
        if(tuple2) datatuple::freetuple(tuple2);

		pthread_mutex_lock(item->data->qlock);

		// Deal with old work_queue item by freeing it or putting it back in the queue.

		if(err) {
		    if(opcode != OP_DONE) {
		    	char *msg;
		    	if(-1 != asprintf(&msg, "network error. conn closed. (%d, %llu) ",
		    			*(item->data->workitem), (unsigned long long)item->data->work_queue->size())) {
		    		perror(msg);
		    		free(msg);
		    	} else {
		    		printf("error preparing string for perror!");
		    	}
		    } else {
//		    	printf("client done. conn closed. (%d, %d)\n",
//					   *(item->data->workitem), item->data->work_queue->size());
		    }
			close(*(item->data->workitem));

        } else {

			//add conn desc to ready queue
			item->data->ready_queue->push(*(item->data->workitem));

			if(item->data->ready_queue->size() == 1) { //signal the event loop
				pthread_cond_signal(item->data->selcond);
				char gunk = 13;
				int ret =write(item->data->self_pipe[1], &gunk, 1);
				if(ret == -1) {
					if(errno != EAGAIN) {
						perror("Couldn't write to self_pipe!");
						abort();
					}
				} else {
					assert(ret == 1);
				}
			}
        }


		if(item->data->work_queue->size() > 0)
		{
			int new_work = item->data->work_queue->front();
			item->data->work_queue->pop();
			*(item->data->workitem) = new_work;
		}
		else
		{
			//set work to -1
			*(item->data->workitem) = -1;
			//add self to idle queue
			item->data->idleth_queue->push(*item);
		}
		pthread_mutex_unlock(item->data->qlock);

		if(!err) {
#ifdef STATS_ENABLED
			if( item->data->num_reqs == 0 )
				item->data->work_time = 0;
			gettimeofday(& (item->data->stop_tv), 0);
			(item->data->num_reqs)++;
			item->data->work_time += (item->data->stop_tv.tv_sec - item->data->start_tv.tv_sec) * 1000 +
				   (item->data->stop_tv.tv_usec / 1000 - item->data->start_tv.tv_usec / 1000);

			int iopcode = opcode;
			ostr << iopcode;
			std::string clientkey = ostr.str();
			if(item->data->num_reqsc.find(clientkey) == item->data->num_reqsc.end())
			{
				item->data->num_reqsc[clientkey]=0;
				item->data->work_timec[clientkey]=0;
			}

			item->data->num_reqsc[clientkey]++;
			item->data->work_timec[clientkey] += (item->data->stop_tv.tv_sec - item->data->start_tv.tv_sec) * 1000 +
				(item->data->stop_tv.tv_usec / 1000 - item->data->start_tv.tv_usec / 1000);;
#endif

		}
    }
    pthread_mutex_unlock(item->data->th_mut);

    return NULL;
}
