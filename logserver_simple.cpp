


#include "logserver.h"
#include "datatuple.h"

#include "logstore.h"

#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#undef begin
#undef end
#undef try


//server codes
uint8_t logserver::OP_SUCCESS = 1;
uint8_t logserver::OP_FAIL = 2;
uint8_t logserver::OP_SENDING_TUPLE = 3;

//client codes
uint8_t logserver::OP_FIND = 4;
uint8_t logserver::OP_INSERT = 5;

uint8_t logserver::OP_INVALID = 32;


void logserver::startserver(logtable *ltable)
{
    sys_alive = true;
    this->ltable = ltable;
    //initialize threads
    for(int i=0; i<nthreads; i++)
    {
        struct pthread_item *worker_th = new pthread_item;
        th_list.push_back(worker_th);
        
        worker_th->th_handle = new pthread_t;
        struct pthread_data *worker_data = new pthread_data;
        worker_th->data = worker_data;

        worker_data->idleth_queue = &idleth_queue;
        
        worker_data->conn_queue = &conn_queue;

        worker_data->qlock = qlock;
        
        worker_data->th_cond = new pthread_cond_t;
        pthread_cond_init(worker_data->th_cond,0);
        
        worker_data->th_mut = new pthread_mutex_t;
        pthread_mutex_init(worker_data->th_mut,0);

        worker_data->workitem = new int;
        *(worker_data->workitem) = -1;

        worker_data->table_lock = lsmlock;

        worker_data->ltable = ltable;

        worker_data->sys_alive = &sys_alive;
        
        pthread_create(worker_th->th_handle, 0, thread_work_fn, worker_th);

        idleth_queue.push(*worker_th);
                
        
    }

    dispatchLoop();

}

void logserver::stopserver()
{
    //close the server socket
    //stops receiving data on the server socket
    shutdown(serversocket, 0);

    //wait for all threads to be idle
    while(idleth_queue.size() != nthreads)
        sleep(1); 

    //set the system running flag to false
    sys_alive = false;
    for(int i=0; i<nthreads; i++)    
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
        delete idle_th->data->th_cond;
        delete idle_th->data->th_mut;
        delete idle_th->data->workitem;
        delete idle_th->data;
        delete idle_th->th_handle;        
    }

    th_list.clear();

    return;
}

void logserver::dispatchLoop()
{
    
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
        return;
    }
    
    bzero((char *) &serv_addr, sizeof(serv_addr));     
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(server_port);
    
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
    {
        printf("ERROR on binding.\n");
        return;
    }
    
    //start listening on the server socket
    //second arg is the max number of coonections waiting in queue
    if(listen(sockfd,SOMAXCONN)==-1)
    {
        printf("ERROR on listen.\n");
         return;
    }

    printf("LSM Server listenning...\n");

    serversocket = sockfd;
    int flag, result;
    while(true)
    {
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) 
        {
            printf("ERROR on accept.\n");
            return; // we probably want to continue instead of return here (when not debugging)
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
            return; 
        }        

        char clientip[20];
        inet_ntop(AF_INET, (void*) &(cli_addr.sin_addr), clientip, 20);
        //printf("Connection from:\t%s\n", clientip);

        //printf("Number of idle threads %d\n", idleth_queue.size());

        pthread_mutex_lock(qlock);

        if(idleth_queue.size() > 0)
        {
            pthread_item idle_th = idleth_queue.front();
            idleth_queue.pop();

            //wake up the thread to do work
            pthread_mutex_lock(idle_th.data->th_mut);
            //set the job of the idle thread
            *(idle_th.data->workitem) = newsockfd;
            pthread_cond_signal(idle_th.data->th_cond);
            pthread_mutex_unlock(idle_th.data->th_mut);            
        }
        else
        {
            //insert the given element to the queue
            conn_queue.push(newsockfd);
            //printf("Number of queued connections:\t%d\n", conn_queue.size());
        }

        pthread_mutex_unlock(qlock);

        /*
        try
        {
            
            pthread_item idle_th = idleth_queue.pop();
            //wake up the thread to do work
            pthread_mutex_lock(idle_th.data->th_mut);
            //set the job of the idle thread
            *(idle_th.data->workitem) = newsockfd;
            pthread_cond_signal(idle_th.data->th_cond);
            pthread_mutex_unlock(idle_th.data->th_mut);
            
        }
        catch(int empty_exception)
        {
            //insert the given element to the queue
            conn_queue.push(newsockfd);
            //printf("Number of queued connections:\t%d\n", conn_queue.size());
        }
        */
    }

    
}

inline void readfromsocket(int sockd, byte *buf, int count)
{

    int n = 0;
    while( n < count )
    {
        n += read( sockd, buf + n, count - n);
    }
    
}

inline void writetosocket(int sockd, byte *buf, int count)
{
    int n = 0;
    while( n < count )
    {
        n += write( sockd, buf + n, count - n);
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

        
        if(!*(item->data->sys_alive))
        {
            //printf("thread quitted.\n");
            break;
        }

        //step 1: read the opcode
        uint8_t opcode;
        ssize_t n = read(*(item->data->workitem), &opcode, sizeof(uint8_t));
        assert( n == sizeof(uint8_t));
        assert( opcode < logserver::OP_INVALID );
        
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
        readfromsocket(*(item->data->workitem), (byte*) tuple.key, *tuple.keylen);
        //read the data
        if(!tuple.isDelete() && opcode != logserver::OP_FIND)
        {
            tuple.data = (byte*) malloc(*tuple.datalen);
            readfromsocket(*(item->data->workitem), (byte*) tuple.data, *tuple.datalen);
        }
        else
            tuple.data = 0;

        //step 3: process the tuple
        //pthread_mutex_lock(item->data->table_lock);
        //readlock(item->data->table_lock,0);
        
        if(opcode == logserver::OP_INSERT)
        {
            //insert/update/delete
            item->data->ltable->insertTuple(tuple);
            //unlock the lsmlock
            //pthread_mutex_unlock(item->data->table_lock);
            //unlock(item->data->table_lock);
            //step 4: send response
            uint8_t rcode = logserver::OP_SUCCESS;
            n = write(*(item->data->workitem), &rcode, sizeof(uint8_t));
            assert(n == sizeof(uint8_t));
            
        }
        else if(opcode == logserver::OP_FIND)
        {
            //find the tuple
            datatuple *dt = item->data->ltable->findTuple(-1, tuple.key, *tuple.keylen);
            //unlock the lsmlock
            //pthread_mutex_unlock(item->data->table_lock);
            //unlock(item->data->table_lock);
            
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
            uint8_t rcode = logserver::OP_SENDING_TUPLE;
            n = write(*(item->data->workitem), &rcode, sizeof(uint8_t));
            assert(n == sizeof(uint8_t));

            //send the tuple
            writetosocket(*(item->data->workitem), (byte*) dt->keylen, dt->byte_length());

            //free datatuple
            free(dt->keylen);
            free(dt);
        }

        //close the socket
        close(*(item->data->workitem));

        //free the tuple
        free(tuple.keylen);
        free(tuple.datalen);
        free(tuple.key);
        free(tuple.data);

        //printf("socket %d: work completed.\n", *(item->data->workitem));

        pthread_mutex_lock(item->data->qlock);

        if(item->data->conn_queue->size() > 0)
        {
            int new_work = item->data->conn_queue->front();
            item->data->conn_queue->pop();
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

        /*
        //check if there is new work this thread can do
        try
        {            
            int new_work = item->data->conn_queue->pop();
            *(item->data->workitem) = new_work; //set new work
            //printf("socket %d: new work found.\n", *(item->data->workitem));
        }
        catch(int empty_exception)
        {
            //printf("socket %d: no new work found.\n", *(item->data->workitem));
            //set work to -1
            *(item->data->workitem) = -1;
            //add self to idle queue
            item->data->idleth_queue->push(*item);

        }
        */

    }
    pthread_mutex_unlock(item->data->th_mut);


}
                       

