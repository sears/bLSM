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
    
    std::set<datatuple, datatuple> rbtree;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple newtuple;        
        uint32_t keylen = key_arr[i].length()+1;
        newtuple.keylen = (uint32_t*)malloc(sizeof(uint32_t));
        *newtuple.keylen = keylen;
        
        newtuple.key = (datatuple::key_t) malloc(keylen);
        for(size_t j=0; j<keylen-1; j++)
            newtuple.key[j] = key_arr[i][j];
        newtuple.key[keylen-1]='\0';

        //prepare the data
        uint32_t datalen = data_arr[i].length()+1;
        newtuple.datalen = (uint32_t*)malloc(sizeof(uint32_t));
        *newtuple.datalen = datalen;
        
        newtuple.data = (datatuple::data_t) malloc(datalen);
        for(size_t j=0; j<datalen-1; j++)
            newtuple.data[j] = data_arr[i][j];
        newtuple.data[datalen-1]='\0';

        /*
        printf("key: \t, keylen: %u\ndata:  datalen: %u\n",
               //newtuple.key,
               *newtuple.keylen,
               //newtuple.data,
               *newtuple.datalen);
               */
        
        datasize += newtuple.byte_length();

        rbtree.insert(newtuple);
        
        
    }

    printf("\nTREE STRUCTURE\n");
    //ltable.get_tree_c1()->print_tree(xid);
    printf("datasize: %lld\n", (long long)datasize);

    printf("Stage 2: Looking up %d keys:\n", NUM_ENTRIES);

    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {        
        int ri = i;

        //get the key
        uint32_t keylen = key_arr[ri].length()+1;        
        datatuple::key_t rkey = (datatuple::key_t) malloc(keylen);
        for(size_t j=0; j<keylen-1; j++)
            rkey[j] = key_arr[ri][j];
        rkey[keylen-1]='\0';

        //find the key with the given tuple

        //prepare a search tuple
        datatuple search_tuple;
        search_tuple.keylen = (uint32_t*)malloc(sizeof(uint32_t));
        *(search_tuple.keylen) = keylen;
        search_tuple.key = rkey;
        
        
        datatuple *ret_tuple=0; 
        //step 1: look in tree_c0

        rbtree_t::iterator rbitr = rbtree.find(search_tuple);
        if(rbitr != rbtree.end())
        {
            datatuple tuple = *rbitr;
            byte *barr = tuple.to_bytes();
            ret_tuple = datatuple::from_bytes(barr);

            found_tuples++;
            assert(*(ret_tuple->keylen) == key_arr[ri].length()+1);
            assert(*(ret_tuple->datalen) == data_arr[ri].length()+1);
            free(barr);
            free(ret_tuple);        
        }
        else
        {
            printf("Not in scratch_tree\n");
        }
        
        free(search_tuple.keylen);
        free(rkey);
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

