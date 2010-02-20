#include "logserver.h"
#include "datatuple.h"
#include "merger.h"

#include "logstore.h"

#include "network.h"

#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <errno.h>

#include <stasis/operations/regions.h>

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

#ifdef STATS_ENABLED
        worker_data->num_reqs = 0;
#endif


        worker_data->qlock = qlock;

        worker_data->selcond = selcond;
        
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
        
        //update select set
        pthread_mutex_lock(qlock);

        if(sel_list.size() == 0)
        {
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
        int sel_res = select(maxfd+1, &readfs, NULL, NULL, NULL);// &Timeout);

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
                }
                else
                {
                    //insert the given element to the work queue
                    work_queue.push(currsock);                    
                }
                
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

        pthread_mutex_lock(sdata->qlock);

        //insert the given element to the ready queue
        sdata->ready_queue->push(newsockfd);

        if(sdata->ready_queue->size() == 1) //signal the event loop
            pthread_cond_signal(sdata->selcond);
        
        pthread_mutex_unlock(sdata->qlock);
    }
}

static void network_disconnect(pthread_item * item, bool iserror) {
    pthread_mutex_lock(item->data->qlock);
    if(iserror) {
    	printf("network error. conn closed. (%d, %d, %d)\n",
    		   errno, *(item->data->workitem), item->data->work_queue->size());
    } else {
    	printf("client done. conn closed. (%d, %d)\n",
			   *(item->data->workitem), item->data->work_queue->size());
    }
	close(*(item->data->workitem));

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

        //step 1: read the opcode
        network_op_t opcode = readopfromsocket(*(item->data->workitem), LOGSTORE_CLIENT_REQUEST);
        if(opcode == LOGSTORE_CONN_CLOSED_ERROR) {
        	opcode = OP_DONE;
        	printf("Obsolescent client closed connection uncleanly\n");
        }

        if( opcode == OP_DONE || (opiserror(opcode))) //close the conn on failure
        {
        	network_disconnect(item, opiserror(opcode));
        	continue;
        }

        int err = 0;
        
        //step 2: read the first tuple from client
        datatuple *tuple, *tuple2;
        if(!err) { tuple  = readtuplefromsocket(*(item->data->workitem), &err); }
        //        read the second tuple from client
        if(!err) { tuple2 = readtuplefromsocket(*(item->data->workitem), &err); }
        //step 3: process the tuple

        if(opcode == OP_INSERT)
        {
            //insert/update/delete
            item->data->ltable->insertTuple(tuple);
            //step 4: send response
            err = writeoptosocket(*(item->data->workitem), LOGSTORE_RESPONSE_SUCCESS);
        }
        else if(opcode == OP_FIND)
        {
            //find the tuple
            datatuple *dt = item->data->ltable->findTuple(-1, tuple->key(), tuple->keylen());

            #ifdef STATS_ENABLED

            if(dt == 0) {
                DEBUG("key not found:\t%s\n", datatuple::key_to_str(tuple.key()).c_str());
            } else if( dt->datalen() != 1024) {
                DEBUG("data len for\t%s:\t%d\n", datatuple::key_to_str(tuple.key()).c_str(),
                       dt->datalen);
                if(datatuple::compare(tuple->key(), dt->key()) != 0) {
                    DEBUG("key not equal:\t%s\t%s\n", datatuple::key_to_str(tuple.key()).c_str(),
                           datatuple::key_to_str(dt->key).c_str());
                }

            }
            #endif
            
            bool dt_needs_free;
            if(dt == 0)  //tuple does not exist.
            {
            	dt = tuple;
            	dt->setDelete();
            	dt_needs_free = false;
            } else {
            	dt_needs_free = true;
            }

            //send the reply code
            int err = writeoptosocket(*(item->data->workitem), LOGSTORE_RESPONSE_SENDING_TUPLES);
            if(!err) {
				//send the tuple
				err = writetupletosocket(*(item->data->workitem), dt);
            }
            if(!err) {
            	writeendofiteratortosocket(*(item->data->workitem));
            }
            //free datatuple
            if(dt_needs_free) {
            	datatuple::freetuple(dt);
            }
        }
        else if(opcode == OP_SCAN)
        {
        	size_t limit = -1;
        	size_t count = 0;
        	if(!err) {  limit = readcountfromsocket(*(item->data->workitem), &err);     }
            if(!err) {  err = writeoptosocket(*(item->data->workitem), LOGSTORE_RESPONSE_SENDING_TUPLES); }

            if(!err) {
        		logtableIterator<datatuple> * itr = new logtableIterator<datatuple>(item->data->ltable, tuple);
        		datatuple * t;
        		while(!err && (t = itr->getnext())) {
        			if(tuple2) {  // are we at the end of range?
        				if(datatuple::compare_obj(t, tuple2) >= 0) {
        					datatuple::freetuple(t);
        					break;
        				}
        			}
        			err = writetupletosocket(*(item->data->workitem), t);
					datatuple::freetuple(t);
					count ++;
					if(count == limit) { break; }  // did we hit limit?
				}
        		delete itr;
        	}
    		if(!err) { writeendofiteratortosocket(*(item->data->workitem)); }
        }
        else if(opcode == OP_DBG_BLOCKMAP)
        {
        	// produce a list of stasis regions
        	int xid = Tbegin();

        	readlock(item->data->ltable->header_lock, 0);

        	// produce a list of regions used by current tree components
        	pageid_t datapage_c1_region_length, datapage_c1_mergeable_region_length = 0, datapage_c2_region_length;
        	pageid_t datapage_c1_region_count,  datapage_c1_mergeable_region_count = 0, datapage_c2_region_count;
        	pageid_t * datapage_c1_regions = item->data->ltable->get_tree_c1()->get_alloc()->list_regions(xid, &datapage_c1_region_length, &datapage_c1_region_count);
        	pageid_t * datapage_c1_mergeable_regions = NULL;
		if(item->data->ltable->get_tree_c1_mergeable()) {
		  datapage_c1_mergeable_regions = item->data->ltable->get_tree_c1_mergeable()->get_alloc()->list_regions(xid, &datapage_c1_mergeable_region_length, &datapage_c1_mergeable_region_count);
		}
        	pageid_t * datapage_c2_regions = item->data->ltable->get_tree_c2()->get_alloc()->list_regions(xid, &datapage_c2_region_length, &datapage_c2_region_count);

        	pageid_t tree_c1_region_length, tree_c1_mergeable_region_length = 0, tree_c2_region_length;
        	pageid_t tree_c1_region_count,  tree_c1_mergeable_region_count = 0, tree_c2_region_count;

        	recordid tree_c1_region_header = item->data->ltable->get_tree_c1()->get_tree_state();
        	recordid tree_c2_region_header = item->data->ltable->get_tree_c2()->get_tree_state();

        	pageid_t * tree_c1_regions = diskTreeComponent::list_region_rid(xid, &tree_c1_region_header, &tree_c1_region_length, &tree_c1_region_count);
        	pageid_t * tree_c1_mergeable_regions = NULL;
		if(item->data->ltable->get_tree_c1_mergeable()) {
		  recordid tree_c1_mergeable_region_header = item->data->ltable->get_tree_c1_mergeable()->get_tree_state();
		  tree_c1_mergeable_regions = diskTreeComponent::list_region_rid(xid, &tree_c1_mergeable_region_header, &tree_c1_mergeable_region_length, &tree_c1_mergeable_region_count);
		}
        	pageid_t * tree_c2_regions = diskTreeComponent::list_region_rid(xid, &tree_c2_region_header, &tree_c2_region_length, &tree_c2_region_count);
        	unlock(item->data->ltable->header_lock);

        	Tcommit(xid);

        	printf("C1 Datapage Regions (each is %lld pages long):\n", datapage_c1_region_length);
        	for(pageid_t i = 0; i < datapage_c1_region_count; i++) {
        		printf("%lld ", datapage_c1_regions[i]);
        	}

        	printf("\nC1 Internal Node Regions (each is %lld pages long):\n", tree_c1_region_length);
        	for(pageid_t i = 0; i < tree_c1_region_count; i++) {
        		printf("%lld ", tree_c1_regions[i]);
        	}

        	printf("\nC2 Datapage Regions (each is %lld pages long):\n", datapage_c2_region_length);
        	for(pageid_t i = 0; i < datapage_c2_region_count; i++) {
        		printf("%lld ", datapage_c2_regions[i]);
        	}

        	printf("\nC2 Internal Node Regions (each is %lld pages long):\n", tree_c2_region_length);
        	for(pageid_t i = 0; i < tree_c2_region_count; i++) {
        		printf("%lld ", tree_c2_regions[i]);
        	}
        	printf("\nStasis Region Map\n");

        	boundary_tag tag;
        	pageid_t pid = ROOT_RECORD.page;
        	TregionReadBoundaryTag(xid, pid, &tag);
		pageid_t max_off = 0;
        	bool done;
        	do {
		  max_off = pid + tag.size;
        		// print tag.
        		printf("\tPage %lld\tSize %lld\tAllocationManager %d\n", (long long)pid, (long long)tag.size, (int)tag.allocation_manager);
        		done = ! TregionNextBoundaryTag(xid, &pid, &tag, 0/*all allocation managers*/);
        	} while(!done);

        	printf("\n");

	        printf("Tree components are using %lld megabytes.  File is using %lld megabytes.",
		       PAGE_SIZE * (tree_c1_region_length * tree_c1_region_count
				    + tree_c1_mergeable_region_length * tree_c1_mergeable_region_count
				    + tree_c2_region_length * tree_c2_region_count
				    + datapage_c1_region_length * datapage_c1_region_count
				    + datapage_c1_mergeable_region_length * datapage_c1_mergeable_region_count
				    + datapage_c2_region_length * datapage_c2_region_count) / (1024 * 1024),
		       (PAGE_SIZE * max_off) / (1024*1024));

        	free(datapage_c1_regions);
		if(datapage_c1_mergeable_regions) free(datapage_c1_mergeable_regions);
        	free(datapage_c2_regions);
        	free(tree_c1_regions);
		if(tree_c1_mergeable_regions) free(tree_c1_mergeable_regions);
        	free(tree_c2_regions);
            err = writeoptosocket(*(item->data->workitem), LOGSTORE_RESPONSE_SUCCESS);

        }

        //free the tuple
        if(tuple)  datatuple::freetuple(tuple);
        if(tuple2) datatuple::freetuple(tuple2);

        if(err) {
			perror("could not respond to client");
			network_disconnect(item, true);
			continue;
		} else {
			pthread_mutex_lock(item->data->qlock);

			//add conn desc to ready queue
			item->data->ready_queue->push(*(item->data->workitem));
			if(item->data->ready_queue->size() == 1) //signal the event loop
				pthread_cond_signal(item->data->selcond);

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
		}
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
    pthread_mutex_unlock(item->data->th_mut);

    return NULL;
}
