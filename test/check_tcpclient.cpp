#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include "logstore.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <sys/types.h> 

#include "../tcpclient.h"
#include "../network.h"

#include "check_util.h"

#undef begin
#undef end


static const char * svrname = "localhost";
static int svrport = 32432;

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);

    logstore_handle_t * l = logstore_client_open(svrname, svrport, 100);

    //data generation
    typedef std::vector<std::string> key_v_t;
    const static size_t max_partition_size = 100000;
    int KEY_LEN = 100;
    std::vector<key_v_t*> *key_v_list = new std::vector<key_v_t*>;
    size_t list_size = NUM_ENTRIES / max_partition_size + 1;
    for(size_t i =0; i<list_size; i++)
    {
        key_v_t * key_arr = new key_v_t;
        if(NUM_ENTRIES < max_partition_size*(i+1))
            preprandstr(NUM_ENTRIES-max_partition_size*i, key_arr, KEY_LEN, true);
        else
            preprandstr(max_partition_size, key_arr, KEY_LEN, true);
    
        std::sort(key_arr->begin(), key_arr->end(), &mycmp);
        key_v_list->push_back(key_arr);
        printf("size partition %llu is %llu\n", (unsigned long long)i+1, (unsigned long long)key_arr->size());
    }


    
    key_v_t * key_arr = new key_v_t;
    
    std::vector<key_v_t::iterator*> iters;
    for(size_t i=0; i<list_size; i++)
    {
        iters.push_back(new key_v_t::iterator((*key_v_list)[i]->begin()));
    }

    int lc = 0;
    while(true)
    {
        int list_index = -1;
        for(size_t i=0; i<list_size; i++)
        {
            if(*iters[i] == (*key_v_list)[i]->end())
                continue;
            
            if(list_index == -1 || mycmp(**iters[i], **iters[list_index]))
                list_index = i;
        }

        if(list_index == -1)
            break;
        
        if(key_arr->size() == 0 || mycmp(key_arr->back(), **iters[list_index]))
            key_arr->push_back(**iters[list_index]);

        (*iters[list_index])++;        
        lc++;
        if(lc % max_partition_size == 0)
            printf("%llu/%llu completed.\n", (unsigned long long)lc, (unsigned long long)NUM_ENTRIES);
    }

    for(size_t i=0; i<list_size; i++)
    {
        (*key_v_list)[i]->clear();
        delete (*key_v_list)[i];
        delete iters[i];
    }
    key_v_list->clear();
    delete key_v_list;
    printf("key arr size: %llu\n", (unsigned long long)key_arr->size());

    if(key_arr->size() > NUM_ENTRIES)
        key_arr->erase(key_arr->begin()+NUM_ENTRIES, key_arr->end());
    
    NUM_ENTRIES=key_arr->size();
    
    printf("Stage 1: Writing %llu keys\n", (unsigned long long)NUM_ENTRIES);
    
    struct timeval start_tv, stop_tv, ti_st, ti_end;
    double insert_time = 0;
    int delcount = 0, upcount = 0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    std::vector<int> del_list;
    gettimeofday(&start_tv,0);
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        len_t keylen = (*key_arr)[i].length()+1;

        //prepare the data
        std::string ditem;
        getnextdata(ditem, 8192);
        len_t datalen = ditem.length()+1;

        datatuple* newtuple = datatuple::create((*key_arr)[i].c_str(), keylen,
						ditem.c_str(), datalen);

        datasize += newtuple->byte_length();

        gettimeofday(&ti_st,0);

        //send the data
        datatuple * ret = logstore_client_op(l, OP_INSERT, newtuple);
        assert(ret);
        
        gettimeofday(&ti_end,0);
        insert_time += tv_to_double(ti_end) - tv_to_double(ti_st);

        datatuple::freetuple(newtuple);

        if(i % 10000 == 0 && i > 0)
            printf("%llu / %llu inserted.\n", (unsigned long long)i, (unsigned long long)NUM_ENTRIES);
            
    }
    gettimeofday(&stop_tv,0);
    printf("insert time: %6.1f\n", insert_time);
    printf("insert time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
    printf("#deletions: %d\n#updates: %d\n", delcount, upcount);

    

    printf("Stage 2: Looking up %llu keys:\n", (unsigned long long)NUM_ENTRIES);


    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {
        int ri = i;
        //printf("key index%d\n", i);
        //fflush(stdout);

        //get the key
        len_t keylen = (*key_arr)[ri].length()+1;

        datatuple* searchtuple = datatuple::create((*key_arr)[ri].c_str(), keylen);

        //find the key with the given tuple
        datatuple *dt = logstore_client_op(l, OP_FIND, searchtuple);

        assert(dt!=0);
        assert(!dt->isDelete());
        found_tuples++;
        assert(dt->keylen() == (*key_arr)[ri].length()+1);

        //free dt
        datatuple::freetuple(dt);
        dt = 0;

        datatuple::freetuple(searchtuple);
    }
    printf("found %d\n", found_tuples);

    printf("Stage 3: Initiating scan\n");

    network_op_t ret = logstore_client_op_returns_many(l, OP_SCAN, NULL, NULL, 0); // start = NULL stop = NULL limit = NONE
    assert(ret == LOGSTORE_RESPONSE_SENDING_TUPLES);
    datatuple * tup;
    size_t i = 0;
    while((tup = logstore_client_next_tuple(l))) {
      assert(!tup->isDelete());
      assert(tup->keylen() == (*key_arr)[i].length()+1);
      assert(!memcmp(tup->key(), (*key_arr)[i].c_str(), (*key_arr)[i].length()));
      datatuple::freetuple(tup);
      i++;
    }
    assert(i == NUM_ENTRIES);

    key_arr->clear();
    delete key_arr;

    logstore_client_close(l);

    gettimeofday(&stop_tv,0);
    printf("run time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
}



/** @test
 */
int main(int argc, char* argv[])
{
	if(argc > 1) {
		svrname = argv[1];
	}
	if(argc > 2) {
		svrport = atoi(argv[2]);
	}
    //insertProbeIter(25000);
    insertProbeIter(100000);
    //insertProbeIter(5000);
//    insertProbeIter(100);

    /*
    insertProbeIter(5000);
    insertProbeIter(2500);
    insertProbeIter(1000);
    insertProbeIter(500);
    insertProbeIter(1000);
    insertProbeIter(100);
    insertProbeIter(10);
    */
    
    return 0;
}

