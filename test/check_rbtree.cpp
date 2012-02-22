/*
 * check_rbtree.cpp
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

#include "check_util.h"

void insertProbeIter(size_t NUM_ENTRIES)
{
	unlink("logfile.txt");
	unlink("storefile.txt");
    system("rm -rf stasis_log/");
    //data generation
    std::vector<std::string> data_arr;
    std::vector<std::string> key_arr;
    preprandstr(NUM_ENTRIES, data_arr, 10*8192, true);
    preprandstr(NUM_ENTRIES+200, key_arr, 100, true);
    
    std::sort(key_arr.begin(), key_arr.end(), &mycmp);

    removeduplicates(key_arr);
    if(key_arr.size() > NUM_ENTRIES)
        key_arr.erase(key_arr.begin()+NUM_ENTRIES, key_arr.end());
    
    NUM_ENTRIES=key_arr.size();
    
    if(data_arr.size() > NUM_ENTRIES)
        data_arr.erase(data_arr.begin()+NUM_ENTRIES, data_arr.end());
    
    memTreeComponent::rbtree_t rbtree;

    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple *newtuple = datatuple::create(key_arr[i].c_str(), key_arr[i].length()+1,data_arr[i].c_str(), data_arr[i].length()+1);

        datasize += newtuple->byte_length();

        rbtree.insert(newtuple);
    }

    printf("\nTREE STRUCTURE\n");
    //ltable.get_tree_c1()->print_tree(xid);
    printf("datasize: %lld\n", (long long)datasize);

    printf("Stage 2: Looking up %llu keys:\n", (unsigned long long)NUM_ENTRIES);

    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {        
        //find the key with the given tuple
        int ri = i;

        //prepare a search tuple
        datatuple *search_tuple = datatuple::create(key_arr[ri].c_str(), key_arr[ri].length()+1);
        
        //step 1: look in tree_c0

	memTreeComponent::rbtree_t::iterator rbitr = rbtree.find(search_tuple);
        if(rbitr != rbtree.end())
        {
            datatuple *tuple = *rbitr;

            found_tuples++;
            assert(tuple->rawkeylen() == key_arr[ri].length()+1);
            assert(tuple->datalen() == data_arr[ri].length()+1);
        }
        else
        {
            printf("Not in scratch_tree\n");
        }
        
        datatuple::freetuple(search_tuple);
    }
    printf("found %d\n", found_tuples);    
}



/** @test
 */
int main()
{
    insertProbeIter(250);

    
    
    return 0;
}

