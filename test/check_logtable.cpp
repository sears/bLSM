
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.h"
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

    int xid = Tbegin();

    Tcommit(xid);

    xid = Tbegin();

    mergeManager merge_mgr(0);
    mergeStats * stats = merge_mgr.newMergeStats(1);

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
    
    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);

    for(size_t i = 0; i < NUM_ENTRIES; i++)
    {
        //prepare the tuple
        datatuple* newtuple = datatuple::create(key_arr[i].c_str(), key_arr[i].length()+1, data_arr[i].c_str(), data_arr[i].length()+1);

        ltable_c1->insertTuple(xid, newtuple);
        datatuple::freetuple(newtuple);
    }
    printf("\nTREE STRUCTURE\n");
    ltable_c1->print_tree(xid);

    printf("Writes complete.\n");

    ltable_c1->writes_done();

    Tcommit(xid);
    xid = Tbegin();

    printf("Stage 2: Sequentially reading %d tuples\n", NUM_ENTRIES);
    
    size_t tuplenum = 0;
    diskTreeComponent::iterator * tree_itr = ltable_c1->open_iterator();


    datatuple *dt=0;
    while( (dt=tree_itr->next_callerFrees()) != NULL)
    {
        assert(dt->keylen() == key_arr[tuplenum].length()+1);
        assert(dt->datalen() == data_arr[tuplenum].length()+1);
        tuplenum++;
        datatuple::freetuple(dt);
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

		datatuple *dt = ltable_c1->findTuple(xid, (const datatuple::key_t) key_arr[ri].c_str(), (size_t)key_arr[ri].length()+1);

        assert(dt!=0);
        assert(dt->keylen() == key_arr[ri].length()+1);
        assert(dt->datalen() == data_arr[ri].length()+1);
        datatuple::freetuple(dt);
        dt = 0;        
    }

    printf("Random Reads completed.\n");
    Tcommit(xid);
    logtable<datatuple>::deinit_stasis();
}

/** @test
 */
int main()
{
    insertProbeIter(15000);

    
    
    return 0;
}

