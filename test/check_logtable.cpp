/*
 * check_logtable.cpp
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
#include "bLSM.h"
#include "dataPage.h"
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

    sync();

    bLSM::init_stasis();

    int xid = Tbegin();

    Tcommit(xid);

    xid = Tbegin();

    mergeManager merge_mgr(0);
    mergeStats * stats = merge_mgr.get_merge_stats(1);

    diskTreeComponent *ltable_c1 = new diskTreeComponent(xid, 1000, 10000, 5, stats);

    std::vector<std::string> data_arr;
    std::vector<std::string> key_arr;
    preprandstr(NUM_ENTRIES, data_arr, 5*4096, true);
    preprandstr(NUM_ENTRIES+200, key_arr, 50, true);//well i can handle upto 200
    
    std::sort(key_arr.begin(), key_arr.end(), &mycmp);

    removeduplicates(key_arr);
    if(key_arr.size() > NUM_ENTRIES)
        key_arr.erase(key_arr.begin()+NUM_ENTRIES, key_arr.end());
    
    NUM_ENTRIES=key_arr.size();
    
    if(data_arr.size() > NUM_ENTRIES)
        data_arr.erase(data_arr.begin()+NUM_ENTRIES, data_arr.end());  
    
    printf("Stage 1: Writing %llu keys\n", (unsigned long long)NUM_ENTRIES);

    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the tuple
        dataTuple* newtuple = dataTuple::create(key_arr[i].c_str(), key_arr[i].length()+1, data_arr[i].c_str(), data_arr[i].length()+1);

        ltable_c1->insertTuple(xid, newtuple);
        dataTuple::freetuple(newtuple);
    }
    printf("\nTREE STRUCTURE\n");
    ltable_c1->print_tree(xid);

    printf("Writes complete.\n");

    ltable_c1->writes_done();

    Tcommit(xid);
    xid = Tbegin();

    printf("Stage 2: Sequentially reading %llu tuples\n", (unsigned long long)NUM_ENTRIES);
    
    size_t tuplenum = 0;
    diskTreeComponent::iterator * tree_itr = ltable_c1->open_iterator();


    dataTuple *dt=0;
    while( (dt=tree_itr->next_callerFrees()) != NULL)
    {
        assert(dt->rawkeylen() == key_arr[tuplenum].length()+1);
        assert(dt->datalen() == data_arr[tuplenum].length()+1);
        tuplenum++;
        dataTuple::freetuple(dt);
        dt = 0;
    }
    delete(tree_itr);
    assert(tuplenum == key_arr.size());
    
    printf("Sequential Reads completed.\n");

    int rrsize=key_arr.size() / 3;
    printf("Stage 3: Randomly reading %d tuples by key\n", rrsize);

    for(int i=0; i<rrsize; i++)
    {
        //randomly pick a key
        int ri = rand()%key_arr.size();

		dataTuple *dt = ltable_c1->findTuple(xid, (const dataTuple::key_t) key_arr[ri].c_str(), (size_t)key_arr[ri].length()+1);

        assert(dt!=0);
        assert(dt->rawkeylen() == key_arr[ri].length()+1);
        assert(dt->datalen() == data_arr[ri].length()+1);
        dataTuple::freetuple(dt);
        dt = 0;        
    }

    printf("Random Reads completed.\n");
    Tcommit(xid);
    bLSM::deinit_stasis();
}

/** @test
 */
int main()
{
    insertProbeIter(15000);

    
    
    return 0;
}

