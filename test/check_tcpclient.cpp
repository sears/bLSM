#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <sys/types.h> 

#include "check_util.h"

#undef begin
#undef end

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    std::string servername = "sherpa4";
    int serverport = 32432;

    double delete_freq = .05;
    double update_freq = .15;
    
    //data generation
    typedef std::vector<std::string> key_v_t;
    const static size_t max_partition_size = 100000;
    int KEY_LEN = 100;
    std::vector<key_v_t*> *key_v_list = new std::vector<key_v_t*>;
    int list_size = NUM_ENTRIES / max_partition_size + 1;
    for(int i =0; i<list_size; i++)
    {
        key_v_t * key_arr = new key_v_t;
        if(NUM_ENTRIES < max_partition_size*(i+1))
            preprandstr(NUM_ENTRIES-max_partition_size*i, key_arr, KEY_LEN, true);
        else
            preprandstr(max_partition_size, key_arr, KEY_LEN, true);
    
        std::sort(key_arr->begin(), key_arr->end(), &mycmp);
        key_v_list->push_back(key_arr);
        printf("size partition %d is %d\n", i+1, key_arr->size());
    }


    
    key_v_t * key_arr = new key_v_t;
    
    std::vector<key_v_t::iterator*> iters;
    for(int i=0; i<list_size; i++)
    {
        iters.push_back(new key_v_t::iterator((*key_v_list)[i]->begin()));
    }

    int lc = 0;
    while(true)
    {
        int list_index = -1;
        for(int i=0; i<list_size; i++)
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
            printf("%d/%d completed.\n", lc, NUM_ENTRIES);
    }

    for(int i=0; i<list_size; i++)
    {
        (*key_v_list)[i]->clear();
        delete (*key_v_list)[i];
        delete iters[i];
    }
    key_v_list->clear();
    delete key_v_list;
    
//    preprandstr(NUM_ENTRIES, data_arr, 10*8192);

    printf("key arr size: %d\n", key_arr->size());

    //removeduplicates(key_arr);
    if(key_arr->size() > NUM_ENTRIES)
        key_arr->erase(key_arr->begin()+NUM_ENTRIES, key_arr->end());
    
    NUM_ENTRIES=key_arr->size();
    
    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);
    
    struct timeval start_tv, stop_tv, ti_st, ti_end;
    double insert_time = 0;
    int dpages = 0;
    int npages = 0;
    int delcount = 0, upcount = 0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    std::vector<int> del_list;
    gettimeofday(&start_tv,0);
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple newtuple;        
        uint32_t keylen = (*key_arr)[i].length()+1;
        newtuple.keylen = &keylen;
        
        newtuple.key = (datatuple::key_t) malloc(keylen);
        memcpy((byte*)newtuple.key, (*key_arr)[i].c_str(), keylen);

        //prepare the data
        std::string ditem;
        getnextdata(ditem, 8192);
        uint32_t datalen = ditem.length()+1;
        newtuple.datalen = &datalen;        
        newtuple.data = (datatuple::data_t) malloc(datalen);
        memcpy((byte*)newtuple.data, ditem.c_str(), datalen);

        /*
        printf("key: \t, keylen: %u\ndata:  datalen: %u\n",
               //newtuple.key,
               *newtuple.keylen,
               //newtuple.data,
               *newtuple.datalen);
               */
        
        datasize += newtuple.byte_length();

        gettimeofday(&ti_st,0);        

        //send the data
        sendTuple(servername, serverport, logserver::OP_INSERT, newtuple);
        
        gettimeofday(&ti_end,0);
        insert_time += tv_to_double(ti_end) - tv_to_double(ti_st);

        free(newtuple.key);
        free(newtuple.data);

        if(i % 10000 == 0 && i > 0)
            printf("%d / %d inserted.\n", i, NUM_ENTRIES);
            
    }
    gettimeofday(&stop_tv,0);
    printf("insert time: %6.1f\n", insert_time);
    printf("insert time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
    printf("#deletions: %d\n#updates: %d\n", delcount, upcount);

    

    printf("Stage 2: Looking up %d keys:\n", NUM_ENTRIES);

    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {        
        int ri = i;        
        //printf("key index%d\n", i);
        fflush(stdout);
        
        //get the key
        uint32_t keylen = (*key_arr)[ri].length()+1;        
        datatuple searchtuple;
        searchtuple.keylen = (uint32_t*)malloc(2*sizeof(uint32_t) + keylen);
        *searchtuple.keylen = keylen;

        searchtuple.datalen = searchtuple.keylen + 1;
        *searchtuple.datalen = 0;

        searchtuple.key = (datatuple::key_t)(searchtuple.keylen + 2);
        memcpy((byte*)searchtuple.key, (*key_arr)[ri].c_str(), keylen);

        //find the key with the given tuple
        datatuple *dt = sendTuple(servername, serverport, logserver::OP_FIND,
                                  searchtuple);
        
        assert(dt!=0);
        assert(!dt->isDelete());
        found_tuples++;
        assert(*(dt->keylen) == (*key_arr)[ri].length()+1);

        //free dt
        free(dt->keylen);
        free(dt->datalen);
        free(dt->key);
        free(dt->data);        
        free(dt);
        
        dt = 0;

        free(searchtuple.keylen);        
        
    }
    printf("found %d\n", found_tuples);

    key_arr->clear();
    //data_arr->clear();
    delete key_arr;
    //delete data_arr;
    
    gettimeofday(&stop_tv,0);
    printf("run time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
    
}



/** @test
 */
int main()
{
    //insertProbeIter(25000);
    insertProbeIter(100000);
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

