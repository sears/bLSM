
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.h"
#include "logiterators.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#undef begin
#undef end

#include "check_util.h"

template class treeIterator<datatuple>;

void insertProbeIter(size_t NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    DataPage<datatuple>::register_stasis_page_impl();

    Tinit();

    int xid = Tbegin();

    logtable ltable;

    int pcount = 5;
    ltable.set_fixed_page_count(pcount);
    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);
    
    xid = Tbegin();
    logtree *lt = ltable.get_tree_c1();
    
    recordid tree_root = lt->get_root_rec();


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
    
    
    
    
    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);

    
    int dpages = 0;
    int npages = 0;
    DataPage<datatuple> *dp=0;
    int64_t datasize = 0;
    std::vector<pageid_t> dsp;
    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the tuple
        datatuple* newtuple = datatuple::create(key_arr[i].c_str(), key_arr[i].length()+1, data_arr[i].c_str(), data_arr[i].length()+1);

        datasize += newtuple->byte_length();

        if(dp == NULL)
        {
            dp = ltable.insertTuple(xid, newtuple, ltable.get_dpstate1(), lt);
            dpages++;
            dsp.push_back(dp->get_start_pid());
        }
        else
        {
            if(!dp->append(xid, newtuple))
            {
                npages += dp->get_page_count();
                delete dp;
                dp = ltable.insertTuple(xid, newtuple, ltable.get_dpstate1(), lt);
                dpages++;
                dsp.push_back(dp->get_start_pid());            
            }
        }

        datatuple::freetuple(newtuple);
    }

    printf("\nTREE STRUCTURE\n");
    lt->print_tree(xid);
    
    printf("Total data set length: %lld\n", datasize);
    printf("Storage utilization: %.2f\n", (datasize+.0) / (PAGE_SIZE * npages));
    printf("Number of datapages: %d\n", dpages);
    printf("Writes complete.\n");
    
    Tcommit(xid);
    xid = Tbegin();

    printf("Stage 2: Sequentially reading %d tuples\n", NUM_ENTRIES);
    
    size_t tuplenum = 0;
    treeIterator<datatuple> tree_itr(tree_root);


    datatuple *dt=0;
    while( (dt=tree_itr.getnext()) != NULL)
    {
        assert(dt->keylen() == key_arr[tuplenum].length()+1);
        assert(dt->datalen() == data_arr[tuplenum].length()+1);
        tuplenum++;
        datatuple::freetuple(dt);
        dt = 0;
    }

    assert(tuplenum == key_arr.size());
    
    printf("Sequential Reads completed.\n");

    int rrsize=key_arr.size() / 3;
    printf("Stage 3: Randomly reading %d tuples by key\n", rrsize);

    for(int i=0; i<rrsize; i++)
    {
        //randomly pick a key
        int ri = rand()%key_arr.size();

		datatuple *dt = ltable.findTuple(xid, (const datatuple::key_t) key_arr[ri].c_str(), (size_t)key_arr[ri].length()+1, lt);

        assert(dt!=0);
        assert(dt->keylen() == key_arr[ri].length()+1);
        assert(dt->datalen() == data_arr[ri].length()+1);
        datatuple::freetuple(dt);
        dt = 0;        
    }

    printf("Random Reads completed.\n");
    Tcommit(xid);
    Tdeinit();

}

/** @test
 */
int main()
{
    insertProbeIter(15000);

    
    
    return 0;
}

