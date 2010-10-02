#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <logstore.h>
#include <datapage.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include "check_util.h"
#include "regionAllocator.h"

#include <stasis/transactional.h>
#undef begin
#undef end

template class DataPage<datatuple>;


void insertWithConcurrentReads(size_t NUM_ENTRIES) {
  srand(1001);
  unlink("storefile.txt");
  unlink("logfile.txt");

  sync();

  logtable<datatuple>::init_stasis();

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

  RegionAllocator * alloc = new RegionAllocator(xid, 10000); // ~ 10 datapages per region.

  printf("Stage 1: Writing %llu keys\n", (unsigned long long)NUM_ENTRIES);

  int pcount = 1000;
  int dpages = 0;
  DataPage<datatuple> *dp=0;
  int64_t datasize = 0;
  std::vector<pageid_t> dsp;
  size_t last_i = 0;
  for(size_t i = 0; i < NUM_ENTRIES; i++)
  {
      //prepare the key
      datatuple *newtuple = datatuple::create(key_arr[i].c_str(), key_arr[i].length()+1, data_arr[i].c_str(), data_arr[i].length()+1);

      datasize += newtuple->byte_length();
      if(dp==NULL || !dp->append(newtuple))
      {
          last_i = i;
          dpages++;
          if(dp)
              dp->writes_done();
              delete dp;

          // Free the old region allocator so that we repeatedly overwrite the same space.  This will find bugs when we fail to null pad at write.
          alloc->done();
          alloc->dealloc_regions(xid);
          delete alloc;
          Tcommit(xid);
          xid = Tbegin();
          alloc = new RegionAllocator(xid, 10000);

          dp = new DataPage<datatuple>(xid, pcount, alloc);
//          printf("%lld\n", dp->get_start_pid());
          bool succ = dp->append(newtuple);
          assert(succ);

          dsp.push_back(dp->get_start_pid());
      }
      size_t j = (rand() % (2 * (1 + i - last_i))) + last_i;

      bool found = 0;
      {
        DataPage<datatuple>::iterator it = dp->begin();
        datatuple * dt;
        while((dt = it.getnext()) != NULL) {
          if(!strcmp((char*)dt->key(), key_arr[j].c_str())) {
            found = true;
          }
          datatuple::freetuple(dt);
        }
      }
      if(found) {
        assert(j <= i);
//        printf("found!");
      } else {
        assert(i < j);
      }
  }
  if(dp) {
    dp->writes_done();
    delete dp;
  }

  printf("Total data set length: %lld\n", (long long)datasize);
  printf("Storage utilization: %.2f\n", (datasize+.0) / (PAGE_SIZE * pcount * dpages));
  printf("Number of datapages: %d\n", dpages);
  printf("Writes complete.\n");

  Tcommit(xid);

  logtable<datatuple>::deinit_stasis();
}

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    logtable<datatuple>::init_stasis();

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

    RegionAllocator * alloc = new RegionAllocator(xid, 10000); // ~ 10 datapages per region.

    printf("Stage 1: Writing %llu keys\n", (unsigned long long)NUM_ENTRIES);
      
    int pcount = 1000;
    int dpages = 0;
    DataPage<datatuple> *dp=0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the key
        datatuple *newtuple = datatuple::create(key_arr[i].c_str(), key_arr[i].length()+1, data_arr[i].c_str(), data_arr[i].length()+1);

        datasize += newtuple->byte_length();
        if(dp==NULL || !dp->append(newtuple))
        {
            dpages++;
            if(dp)
                dp->writes_done();
                delete dp;

            dp = new DataPage<datatuple>(xid, pcount, alloc);

			bool succ = dp->append(newtuple);
			assert(succ);

            dsp.push_back(dp->get_start_pid());
        }
    }
    if(dp) {
      dp->writes_done();
      delete dp;
    }

    printf("Total data set length: %lld\n", (long long)datasize);
    printf("Storage utilization: %.2f\n", (datasize+.0) / (PAGE_SIZE * pcount * dpages));
    printf("Number of datapages: %d\n", dpages);
    printf("Writes complete.\n");
    
    Tcommit(xid);
    xid = Tbegin();


    printf("Stage 2: Reading %llu tuples\n", (unsigned long long)NUM_ENTRIES);

    
    int tuplenum = 0;
    for(int i = 0; i < dpages ; i++)
    {
        DataPage<datatuple> dp(xid, 0, dsp[i]);
        DataPage<datatuple>::iterator itr = dp.begin();
        datatuple *dt=0;
        while( (dt=itr.getnext()) != NULL)
            {
                assert(dt->keylen() == key_arr[tuplenum].length()+1);
                assert(dt->datalen() == data_arr[tuplenum].length()+1);
                tuplenum++;
                datatuple::freetuple(dt);
                dt = 0;
            }

    }
    
    printf("Reads completed.\n");
  
	Tcommit(xid);

	logtable<datatuple>::deinit_stasis();
}


/** @test
 */
int main()
{

  insertWithConcurrentReads(5000);

  insertProbeIter(10000);



  return 0;
}

