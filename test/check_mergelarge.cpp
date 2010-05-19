#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.h"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include <stasis/transactional.h>
#undef begin
#undef end

#include "check_util.h"

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    logtable<datatuple>::init_stasis();

    //data generation
//    std::vector<std::string> * data_arr = new std::vector<std::string>;
    std::vector<std::string> * key_arr = new std::vector<std::string>;
    
//    preprandstr(NUM_ENTRIES, data_arr, 10*8192);
    preprandstr(NUM_ENTRIES+200, key_arr, 100, true);
    
    std::sort(key_arr->begin(), key_arr->end(), &mycmp);

    removeduplicates(key_arr);
    if(key_arr->size() > NUM_ENTRIES)
        key_arr->erase(key_arr->begin()+NUM_ENTRIES, key_arr->end());
    
    NUM_ENTRIES=key_arr->size();
    
    int xid = Tbegin();

    merge_scheduler mscheduler;    
    logtable<datatuple> ltable(1000, 10000, 100);

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    int lindex = mscheduler.addlogtable(&ltable);
    ltable.setMergeData(mscheduler.getMergeData(lindex));

    mscheduler.startlogtable(lindex, 10 * 1024 * 1024);

    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);
    
    struct timeval start_tv, stop_tv, ti_st, ti_end;
    double insert_time = 0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    gettimeofday(&start_tv,0);
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the data
        std::string ditem;
        getnextdata(ditem, 10*8192);

        //prepare the tuple
        datatuple *newtuple = datatuple::create((*key_arr)[i].c_str(), (*key_arr)[i].length()+1, ditem.c_str(), ditem.length()+1);

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
    printf("datasize: %lld\n", datasize);
    
    mscheduler.shutdown();
    printf("merge threads finished.\n");
    gettimeofday(&stop_tv,0);
    printf("run time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
    
    logtable<datatuple>::deinit_stasis();
    
}



/** @test
 */
int main()
{
    insertProbeIter(25000);

    
    
    return 0;
}

