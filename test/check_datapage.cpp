#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <logstore.h>
#include <datapage.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include "check_util.h"

#undef begin
#undef end

template class DataPage<datatuple>;


void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

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
    
    //for(int i = 0; i < NUM_ENTRIES; i++)
    //{
    //   printf("%s\t", arr[i].c_str());
    //   int keylen = arr[i].length()+1;
    //  printf("%d\n", keylen);      
    //}



    recordid alloc_state = Talloc(xid,sizeof(RegionAllocConf_t));
    
    Tset(xid,alloc_state, &logtree::REGION_ALLOC_STATIC_INITIALIZER);


    
    
    

    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);
      
    int pcount = 10;
    int dpages = 0;
    DataPage<datatuple> *dp=0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple newtuple;        
        uint32_t keylen = key_arr[i].length()+1;
        newtuple.keylen = &keylen;
        
        newtuple.key = (datatuple::key_t) malloc(keylen);
        for(size_t j=0; j<keylen-1; j++)
            newtuple.key[j] = key_arr[i][j];
        newtuple.key[keylen-1]='\0';

        //prepare the data
        uint32_t datalen = data_arr[i].length()+1;
        newtuple.datalen = &datalen;
        
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
        if(dp==NULL || !dp->append(xid, newtuple))
        {
            dpages++;
            if(dp)
                delete dp;
            
            dp = new DataPage<datatuple>(xid, pcount, &DataPage<datatuple>::dp_alloc_region_rid, &alloc_state );
            
            if(!dp->append(xid, newtuple))
            {            
                delete dp;
                dp = new DataPage<datatuple>(xid, pcount, &DataPage<datatuple>::dp_alloc_region_rid, &alloc_state );            
                assert(dp->append(xid, newtuple));
            }
               
            dsp.push_back(dp->get_start_pid());
        }
        
        
    }

    printf("Total data set length: %lld\n", (long long)datasize);
    printf("Storage utilization: %.2f\n", (datasize+.0) / (PAGE_SIZE * pcount * dpages));
    printf("Number of datapages: %d\n", dpages);
    printf("Writes complete.\n");
    
    Tcommit(xid);
    xid = Tbegin();


    printf("Stage 2: Reading %d tuples\n", NUM_ENTRIES);

    
    int tuplenum = 0;
    for(int i = 0; i < dpages ; i++)
    {
        DataPage<datatuple> dp(xid, dsp[i]);
        DataPage<datatuple>::RecordIterator itr = dp.begin();
        datatuple *dt=0;
        while( (dt=itr.getnext(xid)) != NULL)
            {
                assert(*(dt->keylen) == key_arr[tuplenum].length()+1);
                assert(*(dt->datalen) == data_arr[tuplenum].length()+1);
                tuplenum++;
                free(dt->keylen);
                free(dt);
                dt = 0;
            }

    }
    
    printf("Reads completed.\n");
/*
    
    int64_t count = 0;
    lladdIterator_t * it = logtreeIterator::open(xid, tree);

    while(logtreeIterator::next(xid, it)) {
        byte * key;
        byte **key_ptr = &key;
        int keysize = logtreeIterator::key(xid, it, (byte**)key_ptr);
        
        pageid_t *value;
        pageid_t **value_ptr = &value;
        int valsize = lsmTreeIterator_value(xid, it, (byte**)value_ptr);
        //printf("keylen %d key %s\n", keysize, (char*)(key)) ;
        assert(valsize == sizeof(pageid_t));
        assert(!mycmp(std::string((char*)key), arr[count]) && !mycmp(arr[count],std::string((char*)key)));
        assert(keysize == arr[count].length()+1);
        count++;
    }
    assert(count == NUM_ENTRIES);

    logtreeIterator::close(xid, it);

    
    */

  
        Tcommit(xid);
        Tdeinit();
}


/** @test
 */
int main()
{
    insertProbeIter(10000);

    
    
    return 0;
}

