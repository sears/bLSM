#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <logstore.h>
#include <datapage.cpp> // XXX

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#undef begin
#undef end

template class DataPage<datatuple>;

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

/**
 * REGION ALLOCATION
 **/
pageid_t alloc_region(int xid, void *conf)
{
    RegionAllocConf_t* a = (RegionAllocConf_t*)conf;
    
  if(a->nextPage == a->endOfRegion) {
    if(a->regionList.size == -1) {
        //DEBUG("nextPage: %lld\n", a->nextPage);
        a->regionList = TarrayListAlloc(xid, 1, 4, sizeof(pageid_t));
        DEBUG("regionList.page: %lld\n", a->regionList.page);
        DEBUG("regionList.slot: %d\n", a->regionList.slot);
        DEBUG("regionList.size: %lld\n", a->regionList.size);
        
        a->regionCount = 0;
    }
    DEBUG("{%lld <- alloc region arraylist}\n", a->regionList.page);
    TarrayListExtend(xid,a->regionList,1);
    a->regionList.slot = a->regionCount;
    DEBUG("region lst slot %d\n",a->regionList.slot);
    a->regionCount++;
    DEBUG("region count %lld\n",a->regionCount);
    a->nextPage = TregionAlloc(xid, a->regionSize,12);
    DEBUG("next page %lld\n",a->nextPage);
    a->endOfRegion = a->nextPage + a->regionSize;
    Tset(xid,a->regionList,&a->nextPage);
    DEBUG("next page %lld\n",a->nextPage);
  }
    
  DEBUG("%lld ?= %lld\n", a->nextPage,a->endOfRegion);
  pageid_t ret = a->nextPage;
  // Ensure the page is in buffer cache without accessing disk (this
  // sets it to clean and all zeros if the page is not in cache).
  // Hopefully, future reads will get a cache hit, and avoid going to
  // disk.

  Page * p = loadUninitializedPage(xid, ret);
  releasePage(p);
  DEBUG("ret %lld\n",ret);
  (a->nextPage)++;
  return ret;

}


pageid_t alloc_region_rid(int xid, void * ridp) {
  recordid rid = *(recordid*)ridp;
  RegionAllocConf_t conf;
  Tread(xid,rid,&conf);
  pageid_t ret = alloc_region(xid,&conf);
  DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);

  Tset(xid,rid,&conf);
  return ret;
}


void insertProbeIter(int  NUM_ENTRIES)
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
    for(int i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple newtuple;        
        uint32_t keylen = key_arr[i].length()+1;
        newtuple.keylen = &keylen;
        
        newtuple.key = (datatuple::key_t) malloc(keylen);
        for(int j=0; j<keylen-1; j++)
            newtuple.key[j] = key_arr[i][j];
        newtuple.key[keylen-1]='\0';

        //prepare the data
        uint32_t datalen = data_arr[i].length()+1;
        newtuple.datalen = &datalen;
        
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

    printf("Total data set length: %d\n", datasize);
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

