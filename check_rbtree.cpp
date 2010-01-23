#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.cpp"
#include "logiterators.cpp"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#undef begin
#undef end




bool mycmp(const std::string & k1,const std::string & k2)
{            
    //for char* ending with \0
    return strcmp(k1.c_str(),k2.c_str()) < 0;
    
    //for int32_t
    //printf("%d\t%d\n",(*((int32_t*)k1)) ,(*((int32_t*)k2)));
    //return (*((int32_t*)k1)) <= (*((int32_t*)k2));
}

//must be given a sorted array
void removeduplicates(std::vector<std::string> &arr)
{

    for(int i=arr.size()-1; i>0; i--)
    {
        if(! (mycmp(arr[i], arr[i-1]) || mycmp(arr[i-1], arr[i])))        
            arr.erase(arr.begin()+i);
            
    }

}

void preprandstr(int count, std::vector<std::string> &arr, int avg_len=50, bool duplicates_allowed=false)
{    

    for ( int j=0; j<count; j++)
    {
        int str_len = (rand()%(avg_len*2)) + 3;

        char *rc = (char*)malloc(str_len);
        
        for(int i=0; i<str_len-1; i++)        
            rc[i] = rand()%10+48;
        
        rc[str_len-1]='\0';
        std::string str(rc);
        
        //make sure there is no duplicate key
        if(!duplicates_allowed)
        {
            bool dup = false;
            for(int i=0; i<j; i++)        
                if(! (mycmp(arr[i], str) || mycmp(str, arr[i])))
                {
                    dup=true;
                    break;
                }
            if(dup)
            {
                j--;
                continue;
            }
        }

        
        //printf("keylen-%d\t%d\t%s\n", str_len, str.length(),rc);
        free(rc);

        arr.push_back(str);
        
    }

}

void insertProbeIter(int  NUM_ENTRIES)
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
    for(int i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple newtuple;        
        uint32_t keylen = key_arr[i].length()+1;
        newtuple.keylen = (uint32_t*)malloc(sizeof(uint32_t));
        *newtuple.keylen = keylen;
        
        newtuple.key = (datatuple::key_t) malloc(keylen);
        for(int j=0; j<keylen-1; j++)
            newtuple.key[j] = key_arr[i][j];
        newtuple.key[keylen-1]='\0';

        //prepare the data
        uint32_t datalen = data_arr[i].length()+1;
        newtuple.datalen = (uint32_t*)malloc(sizeof(uint32_t));
        *newtuple.datalen = datalen;
        
        newtuple.data = (datatuple::data_t) malloc(datalen);
        for(int j=0; j<datalen-1; j++)
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
    printf("datasize: %d\n", datasize);

    printf("Stage 2: Looking up %d keys:\n", NUM_ENTRIES);

    int found_tuples=0;
    for(int i=NUM_ENTRIES-1; i>=0; i--)
    {        
        int ri = i;

        //get the key
        uint32_t keylen = key_arr[ri].length()+1;        
        datatuple::key_t rkey = (datatuple::key_t) malloc(keylen);
        for(int j=0; j<keylen-1; j++)
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

