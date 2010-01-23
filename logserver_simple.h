#ifndef _LOGSERVER_H_
#define _LOGSERVER_H_


#include <queue>
#include <vector>

//#include "logstore.h"

#include "datatuple.h"



#include <stasis/transactional.h>
#include <pthread.h>

#undef begin
#undef try
#undef end

class logtable;

template<class T>
class ccqueue
{
public:
    ccqueue()
        {
            qmut = new pthread_mutex_t;
            pthread_mutex_init(qmut,0);            
        }
    
    int size()
        {
            pthread_mutex_lock(qmut);
            int qsize = m_queue.size();            
            pthread_mutex_unlock(qmut);
            return qsize;
        }

    //inserts a copy of the given element to the queue
    void push(const T &item)
        {
            pthread_mutex_lock(qmut);
            m_queue.push(item);
            pthread_mutex_unlock(qmut);
            return;
        }

    //returns a copy of the next element
    //deletes the copy in the queue
    //throws an exception with -1 on empty queue
    T pop() throw (int)
        {
            pthread_mutex_lock(qmut);
 
            if(m_queue.size() > 0)
            {
                T item = m_queue.front();
                m_queue.pop();
                pthread_mutex_unlock(qmut);
                return item;
            }
            
            
            pthread_mutex_unlock(qmut);
            throw(-1);
                
            
        }

    

    ~ccqueue()
        {
            delete qmut;
        }
    
private:

    std::queue<T> m_queue;

    pthread_mutex_t *qmut;

};

struct pthread_item;

struct pthread_data {
    std::queue<pthread_item> *idleth_queue;
    std::queue<int> *conn_queue;
    pthread_mutex_t * qlock;

    pthread_cond_t * th_cond;
    pthread_mutex_t * th_mut;
    
    int *workitem; //id of the socket to work

    //pthread_mutex_t * table_lock;
    rwl *table_lock;
    logtable *ltable;
    bool *sys_alive;
};

struct pthread_item{
    pthread_t * th_handle;
    pthread_data *data;
};

struct work_item
{
    int sockd; //socket id
    datatuple in_tuple; //request
    datatuple out_tuple; //response
};
    

void * thread_work_fn( void *);    

class logserver
{
public:
    //server codes
    static uint8_t OP_SUCCESS;
    static uint8_t OP_FAIL;
    static uint8_t OP_SENDING_TUPLE;

    //client codes
    static uint8_t OP_FIND;
    static uint8_t OP_INSERT;

    static uint8_t OP_INVALID;
    
public:
    logserver(int nthreads, int server_port){
        this->nthreads = nthreads;
        this->server_port = server_port;
        //lsmlock = new pthread_mutex_t;
        //pthread_mutex_init(lsmlock,0);

        lsmlock = initlock();

        qlock = new pthread_mutex_t;
        pthread_mutex_init(qlock,0);

        ltable = 0;

    }

    ~logserver()
        {
            //delete lsmlock;
            deletelock(lsmlock);
            delete qlock;
        }
    
    void startserver(logtable *ltable);

    void stopserver();
    
    
public:

private:

    //main loop of server
    //accept connections, assign jobs to threads
    void dispatchLoop();
    

private:

    int server_port;
    
    int nthreads;

    bool sys_alive;
    
    int serversocket; //server socket file descriptor

    //ccqueue<int> conn_queue; //list of active connections (socket list)

    //ccqueue<pthread_item> idleth_queue; //list of idle threads

    std::queue<int> conn_queue;
    std::queue<pthread_item> idleth_queue;
    pthread_mutex_t *qlock;

    std::vector<pthread_item *> th_list; // list of threads

    rwl *lsmlock; //lock for using lsm table

    logtable *ltable;
    
};


#endif
