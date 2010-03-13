#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.h"
#include "logiterators.h"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#undef begin
#undef end

#include "check_util.h"

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    diskTreeComponent::internalNodes::init_stasis();

    //data generation
    std::vector<std::string> * data_arr = new std::vector<std::string>;
    std::vector<std::string> * key_arr = new std::vector<std::string>;
    
    preprandstr(NUM_ENTRIES, data_arr, 10*8192, true);
    preprandstr(NUM_ENTRIES+200, key_arr, 100, true);
    
    std::sort(key_arr->begin(), key_arr->end(), &mycmp);

    removeduplicates(key_arr);
    if(key_arr->size() > NUM_ENTRIES)
        key_arr->erase(key_arr->begin()+NUM_ENTRIES, key_arr->end());
    
    NUM_ENTRIES=key_arr->size();
    
    if(data_arr->size() > NUM_ENTRIES)
        data_arr->erase(data_arr->begin()+NUM_ENTRIES, data_arr->end());

    int xid = Tbegin();

    merge_scheduler mscheduler;    
    logtable ltable(1000, 10000, 5);

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    writelock(ltable.header_lock,0);
    int lindex = mscheduler.addlogtable(&ltable);
    ltable.setMergeData(mscheduler.getMergeData(lindex));

    mscheduler.startlogtable(lindex);
    unlock(ltable.header_lock);
    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);
    
    struct timeval start_tv, stop_tv, ti_st, ti_end;
    double insert_time = 0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    gettimeofday(&start_tv,0);
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple *newtuple = datatuple::create((*key_arr)[i].c_str(), (*key_arr)[i].length()+1,(*data_arr)[i].c_str(), (*data_arr)[i].length()+1);

        /*
        printf("key: \t, keylen: %u\ndata:  datalen: %u\n",
               //newtuple.key,
               *newtuple.keylen,
               //newtuple.data,
               *newtuple.datalen);
               */
        
        datasize += newtuple->byte_length();

        gettimeofday(&ti_st,0);        
        ltable.insertTuple(newtuple);
        gettimeofday(&ti_end,0);
        insert_time += tv_to_double(ti_end) - tv_to_double(ti_st);

        datatuple::freetuple(newtuple);
        
    }
    gettimeofday(&stop_tv,0);
    printf("insert time: %6.1f\n", insert_time);
    printf("insert time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));

    printf("\nTREE STRUCTURE\n");
    //ltable.get_tree_c1()->print_tree(xid);
    printf("datasize: %lld\n", (long long)datasize);
    //sleep(20);

    xid = Tbegin();


    printf("Stage 2: Looking up %d keys:\n", NUM_ENTRIES);

    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {        
        int ri = i;
        //printf("key index%d\n", i);
        fflush(stdout);

        //get the key
        uint32_t keylen = (*key_arr)[ri].length()+1;        
        datatuple::key_t rkey = (datatuple::key_t) malloc(keylen);
        memcpy((byte*)rkey, (*key_arr)[ri].c_str(), keylen);
        //for(int j=0; j<keylen-1; j++)
        //rkey[j] = (*key_arr)[ri][j];
        //rkey[keylen-1]='\0';

        //find the key with the given tuple
        datatuple *dt = ltable.findTuple(xid, rkey, keylen);

        assert(dt!=0);
        //if(dt!=0)
        {
			found_tuples++;
			assert(dt->keylen() == (*key_arr)[ri].length()+1);
			assert(dt->datalen() == (*data_arr)[ri].length()+1);
			datatuple::freetuple(dt);
        }
        dt = 0;
        free(rkey);
    }
    printf("found %d\n", found_tuples);

    key_arr->clear();
    data_arr->clear();
    delete key_arr;
    delete data_arr;
    
    mscheduler.shutdown();
    printf("merge threads finished.\n");
    gettimeofday(&stop_tv,0);
    printf("run time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));


    
    Tcommit(xid);
    diskTreeComponent::internalNodes::deinit_stasis();
}



/** @test
 */
int main()
{
    insertProbeIter(5000);

    
    
    return 0;
}

