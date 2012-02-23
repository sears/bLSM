/*
 * check_datapage.cpp
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
 *      Author: makdere
 */
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include "blsm.h"
#include "datapage.h"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include <stasis/transactional.h>

#include "check_util.h"

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");
    system("rm -rf stasis_log/");

    bLSM::init_stasis();

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

    bLSM *ltable = new bLSM(10*1024*1024, 1000, 10000, 100);
    mergeScheduler mscheduler(ltable);

    recordid table_root = ltable->allocTable(xid);

    Tcommit(xid);

    mscheduler.start();

    printf("Stage 1: Writing %llu keys\n", (unsigned long long)NUM_ENTRIES);
    
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
        dataTuple *newtuple = dataTuple::create((*key_arr)[i].c_str(), (*key_arr)[i].length()+1, ditem.c_str(), ditem.length()+1);

        datasize += newtuple->byte_length();

        gettimeofday(&ti_st,0);        
        ltable->insertTuple(newtuple);
        gettimeofday(&ti_end,0);
        insert_time += tv_to_double(ti_end) - tv_to_double(ti_st);

        dataTuple::freetuple(newtuple);
    }
    gettimeofday(&stop_tv,0);
    printf("insert time: %6.1f\n", insert_time);
    printf("insert time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));

    printf("\nTREE STRUCTURE\n");
    //ltable.get_tree_c1()->print_tree(xid);
    printf("datasize: %llu\n", (unsigned long long)datasize);
    
    mscheduler.shutdown();
    delete ltable;
    printf("merge threads finished.\n");
    gettimeofday(&stop_tv,0);
    printf("run time: %6.1f\n", (tv_to_double(stop_tv) - tv_to_double(start_tv)));
    bLSM::deinit_stasis();
    
}



/** @test
 */
int main()
{
    insertProbeIter(25000);

    
    
    return 0;
}

