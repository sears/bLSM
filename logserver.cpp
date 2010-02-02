


#include "logserver.h"
#include "datatuple.h"

#include "logstore.h"

#include "network.h"

#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <errno.h>

#undef begin
#undef end
#undef try


void *serverLoop(void *args);

void logserver::startserver(logtable *ltable)
{
    sys_alive = true;
    this->ltable = ltable;

    selcond = new pthread_cond_t;
    pthread_cond_init(selcond, 0);
    
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

        worker_data->qlock = qlock;

        worker_data->selcond = selcond;
        
        worker_data->th_cond = new pthread_cond_t;
        pthread_cond_init(worker_data->th_cond,0);
        
        worker_data->th_mut = new pthread_mutex_t;
        pthread_mutex_init(worker_data->th_mut,0);

        worker_data->workitem = new int;
        *(worker_data->workitem) = -1;

        //worker_data->table_lock = lsmlock;

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

        printf("thread %d: work_time %.3f\t#calls %d\tavg req process time:\t%.3f\n",
               i,
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
    
    //close(serversocket);

    return;
}

void logserver::eventLoop()
{

    fd_set readfs;
    std::vector<int> sel_list;
    
    int maxfd;

    struct timespec   ts;
         
    while(true)
    {
        //clear readset
        FD_ZERO(&readfs);
        maxfd = -1;

        ts.tv_nsec = 250000; //nanosec
        ts.tv_sec = 0;

        //Timeout.tv_usec = 250;  /* microseconds */
        //Timeout.tv_sec  = 0;  /* seconds */
        
        //update select set
        pthread_mutex_lock(qlock);

        //while(ready_queue.size() == 0)
        if(sel_list.size() == 0)
        {
            while(ready_queue.size() == 0)
                pthread_cond_wait(selcond, qlock);
            //pthread_cond_timedwait(selcond, qlock, &ts);
            //printf("awoke\n");
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
        int sel_res = select(maxfd+1, &readfs, NULL, NULL, NULL);// &Timeout);
        //printf("sel_res %d %d\n", sel_res, errno);        
        //fflush(stdout);
        //job assignment to threads
        //printf("sel_list size:\t%d ready_cnt\t%d\n", sel_list.size(), sel_res);

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

            if (FD_ISSET(currsock, &readfs))
            {
                //printf("sock %d ready\n", currsock);
//                pthread_mutex_lock(qlock);

                if(idleth_queue.size() > 0) //assign the job to an indle thread
                {
                    pthread_item idle_th = idleth_queue.front();
                    idleth_queue.pop();
                    
                    //wake up the thread to do work
                    pthread_mutex_lock(idle_th.data->th_mut);
                    //set the job of the idle thread
                    *(idle_th.data->workitem) = currsock;
                    pthread_cond_signal(idle_th.data->th_cond);
                    pthread_mutex_unlock(idle_th.data->th_mut);
                    //printf("%d:\tconn %d assigned.\n", i, currsock);
                }
                else
                {
                    //insert the given element to the work queue
                    work_queue.push(currsock);                    
                    //printf("work queue size:\t%d\n", work_queue.size());
                }

//                pthread_mutex_unlock(qlock);                
                
                //remove from the sel_list
                sel_list.erase(sel_list.begin()+i);
                i--;                
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
    socklen_t clilen = sizeof(cli_addr);
    

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
    //second arg is the max number of coonections waiting in queue
    if(listen(sockfd,SOMAXCONN)==-1)
    {
        printf("ERROR on listen.\n");
         return 0;
    }

    printf("LSM Server listenning...\n");

    *(sdata->server_socket) = sockfd;
    int flag, result;
    while(true)
    {
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
        printf("Connection from:\t%s\n", clientip);

        //printf("Number of idle threads %d\n", idleth_queue.size());

        pthread_mutex_lock(sdata->qlock);

        //insert the given element to the ready queue
        sdata->ready_queue->push(newsockfd);

        if(sdata->ready_queue->size() == 1) //signal the event loop
            pthread_cond_signal(sdata->selcond);
        
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
            //printf("thread quitted.\n");
            break;
        }

        //step 1: read the opcode
        uint8_t opcode;
        ssize_t n = read(*(item->data->workitem), &opcode, sizeof(uint8_t));
        if(n == 0) {
        	opcode = OP_DONE;
        	n = sizeof(uint8_t);
        	printf("Obsolescent client closed connection uncleanly\n");
        }
        assert( n == sizeof(uint8_t));
        assert( opcode < OP_INVALID );

        if( opcode == OP_DONE ) //close the conn on failure
        {
            pthread_mutex_lock(item->data->qlock);            
            printf("client done. conn closed. (%d, %d, %d, %d)\n",
                   n, errno, *(item->data->workitem), item->data->work_queue->size());
            close(*(item->data->workitem));
                
            if(item->data->work_queue->size() > 0)
            {
                int new_work = item->data->work_queue->front();
                item->data->work_queue->pop();
                //printf("work queue size:\t%d\n", item->data->work_queue->size());
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
            continue;            
        }

        
        //step 2: read the tuple from client        
        datatuple tuple;
        tuple.keylen = (uint32_t*)malloc(sizeof(uint32_t));
        tuple.datalen = (uint32_t*)malloc(sizeof(uint32_t));
        
        //read the key length
        n = read(*(item->data->workitem), tuple.keylen, sizeof(uint32_t));
        assert( n == sizeof(uint32_t));
        //read the data length
        n = read(*(item->data->workitem), tuple.datalen, sizeof(uint32_t));
        assert( n == sizeof(uint32_t));
        
        //read the key
        tuple.key = (byte*) malloc(*tuple.keylen);
        readfromsocket(*(item->data->workitem), (char*) tuple.key, *tuple.keylen);
        //read the data
        if(!tuple.isDelete() && opcode != OP_FIND)
        {
            tuple.data = (byte*) malloc(*tuple.datalen);
            readfromsocket(*(item->data->workitem), (char*) tuple.data, *tuple.datalen);
        }
        else
            tuple.data = 0;

        //step 3: process the tuple
        //pthread_mutex_lock(item->data->table_lock);
        //readlock(item->data->table_lock,0);
        
        if(opcode == OP_INSERT)
        {
            //insert/update/delete
            item->data->ltable->insertTuple(tuple);
            //unlock the lsmlock
            //pthread_mutex_unlock(item->data->table_lock);
            //unlock(item->data->table_lock);
            //step 4: send response
            uint8_t rcode = OP_SUCCESS;
            n = write(*(item->data->workitem), &rcode, sizeof(uint8_t));
            assert(n == sizeof(uint8_t));
            
        }
        else if(opcode == OP_FIND)
        {
            //find the tuple
            datatuple *dt = item->data->ltable->findTuple(-1, tuple.key, *tuple.keylen);
            //unlock the lsmlock
            //pthread_mutex_unlock(item->data->table_lock);
            //unlock(item->data->table_lock);

            #ifdef STATS_ENABLED

            if(dt == 0)
                printf("key not found:\t%s\n", datatuple::key_to_str(tuple.key).c_str());
            else if( *dt->datalen != 1024)
                printf("data len for\t%s:\t%d\n", datatuple::key_to_str(tuple.key).c_str(),
                       *dt->datalen);

            if(datatuple::compare(tuple.key, dt->key) != 0)
                printf("key not equal:\t%s\t%s\n", datatuple::key_to_str(tuple.key).c_str(),
                       datatuple::key_to_str(dt->key).c_str());
            
            #endif
            
            if(dt == 0)  //tuple deleted
            {
                dt = (datatuple*) malloc(sizeof(datatuple));
                dt->keylen = (uint32_t*) malloc(2*sizeof(uint32_t) + *tuple.keylen);
                *dt->keylen = *tuple.keylen;
                dt->datalen = dt->keylen + 1;
                dt->key = (datatuple::key_t) (dt->datalen+1);
                memcpy((byte*) dt->key, (byte*) tuple.key, *tuple.keylen);
                dt->setDelete();
            }

            //send the reply code
            uint8_t rcode = OP_SENDING_TUPLE;
            n = write(*(item->data->workitem), &rcode, sizeof(uint8_t));
            assert(n == sizeof(uint8_t));

            //send the tuple
            writetosocket(*(item->data->workitem), (char*) dt->keylen, dt->byte_length());

            //free datatuple
            free(dt->keylen);
            free(dt);
        }

        //close the socket
        //close(*(item->data->workitem));

        //free the tuple
        free(tuple.keylen);
        free(tuple.datalen);
        free(tuple.key);
        free(tuple.data);

        //printf("socket %d: work completed.", *(item->data->workitem));
        
        pthread_mutex_lock(item->data->qlock);
        
        //add conn desc to ready queue
        item->data->ready_queue->push(*(item->data->workitem));
        //printf("ready queue size: %d sock(%d)\n", item->data->ready_queue->size(), *(item->data->workitem));
        if(item->data->ready_queue->size() == 1) //signal the event loop
            pthread_cond_signal(item->data->selcond);

        //printf("work complete, added to ready queue %d (size %d)\n", *(item->data->workitem),
        //       item->data->ready_queue->size());
        
        if(item->data->work_queue->size() > 0)
        {
            int new_work = item->data->work_queue->front();
            item->data->work_queue->pop();
            //printf("work queue size:\t%d\n", item->data->work_queue->size());
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

        #ifdef STATS_ENABLED
        if( item->data->num_reqs == 0 )
            item->data->work_time = 0;
        gettimeofday(& (item->data->stop_tv), 0);
        (item->data->num_reqs)++;
        //item->data->work_time += tv_to_double(item->data->stop_tv) - tv_to_double(item->data->start_tv);
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
    pthread_mutex_unlock(item->data->th_mut);

    return NULL;
}
                       

