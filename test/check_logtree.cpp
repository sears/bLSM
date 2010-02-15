
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_logTree.log"
#define NUM_ENTRIES_A 10000
#define NUM_ENTRIES_B 10
#define NUM_ENTRIES_C 0

#define OFFSET      (NUM_ENTRIES * 10)

#undef begin
#undef end

#include "check_util.h"

void insertProbeIter_str(int  NUM_ENTRIES)
{
    srand(1000);
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    logtree::init_stasis();

    int xid = Tbegin();

    logtable ltable;

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);
    
    xid = Tbegin();
    logtree *lt = ltable.get_tree_c1();
    
    recordid tree = lt->get_root_rec();
  
    long oldpagenum = -1;

    std::vector<std::string> arr;
    preprandstr(NUM_ENTRIES, arr, 50, false);
    std::sort(arr.begin(), arr.end(), &mycmp);
    
    //for(int i = 0; i < NUM_ENTRIES; i++)
    //{
    //   printf("%s\t", arr[i].c_str());
    //   int keylen = arr[i].length()+1;
    //  printf("%d\n", keylen);      
    //}


    printf("Stage 1: Writing %d keys\n", NUM_ENTRIES);
      

    for(int i = 0; i < NUM_ENTRIES; i++)
    {
        int keylen = arr[i].length()+1;
        byte *currkey = (byte*)malloc(keylen);
        for(int j=0; j<keylen-1; j++)
            currkey[j] = arr[i][j];
        currkey[keylen-1]='\0';      
      
        //printf("\n#########\ni=%d\nkey:\t%s\nkeylen:%d\n",i,((char*)currkey),keylen);
        long pagenum = logtree::findPage(xid, tree, currkey, keylen);
        //printf("pagenum:%d\n", pagenum);
        assert(pagenum == -1 || pagenum == oldpagenum || oldpagenum == -1);
        //printf("TlsmAppendPage %d\n",i);

        recordid rid = lt->get_tree_state();
        RegionAllocConf_t alloc_conf;
        Tread(xid,rid,&alloc_conf);
      
        logtree::appendPage(xid, tree, lt->lastLeaf, currkey, keylen, lt->alloc_region, &alloc_conf, i + OFFSET);

        //DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);
        // XXX get rid of Tset by storing next page in memory, and losing it
        //     on crash.
        Tset(xid,rid,&alloc_conf);
      
      
        pagenum = logtree::findPage(xid, tree, currkey,keylen);
        oldpagenum = pagenum;
        //printf("pagenum:%d\n", pagenum);      
        assert(pagenum == i + OFFSET);
        free(currkey);


    }

    printf("Writes complete.");
    
    tree = lt->get_root_rec();
    Tcommit(xid);
    xid = Tbegin();

    printf("\nTREE STRUCTURE\n");
    lt->print_tree(xid);

    printf("Stage 2: Looking up %d keys\n", NUM_ENTRIES);
  
    for(int i = 0; i < NUM_ENTRIES; i++) {
        int keylen = arr[i].length()+1;
        byte *currkey = (byte*)malloc(keylen);
        for(int j=0; j<keylen-1; j++)
            currkey[j] = arr[i][j];
        currkey[keylen-1]='\0';

        //printf("\n#########\ni=%d\nkey:\t%s\nkeylen:%d\n",i,((char*)currkey),keylen);
        long pagenum = logtree::findPage(xid, tree, currkey, keylen);
        //printf("pagenum:%d\n", pagenum);      
        assert(pagenum == i + OFFSET);
        free(currkey);
    }


    printf("Stage 3: Iterating over %d keys\n", NUM_ENTRIES);

    
    int64_t count = 0;
    lladdIterator_t * it = logtreeIterator::open(xid, tree);

    while(logtreeIterator::next(xid, it)) {
        byte * key;
        byte **key_ptr = &key;
        size_t keysize = logtreeIterator::key(xid, it, (byte**)key_ptr);
        
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

	Tcommit(xid);
    logtree::deinit_stasis();
}




void insertProbeIter_int(int  NUM_ENTRIES)
{

    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    logtable ltable;

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);
    
    xid = Tbegin();
    logtree *lt = ltable.get_tree_c1();
    
    recordid tree = lt->get_root_rec();
  
    long oldpagenum = -1;
    
    for(int32_t i = 0; i < NUM_ENTRIES; i++) {
        int keylen = sizeof(int32_t);
        byte *currkey = (byte*)malloc(keylen);
        memcpy(currkey, (byte*)(&i), keylen);
        //currkey[]='\0';
      
        printf("\n#########\ni=%d\nkey:\t%d\nkeylen:%d\n",i,*((int32_t*)currkey),keylen);
        pageid_t pagenum = logtree::findPage(xid, tree, currkey, keylen);
        printf("pagenum:%lld\n", (long long)pagenum);
        assert(pagenum == -1 || pagenum == oldpagenum || oldpagenum == -1);
        printf("TlsmAppendPage %d\n",i);

        recordid rid = lt->get_tree_state();
        RegionAllocConf_t alloc_conf;
        Tread(xid,rid,&alloc_conf);
      
        logtree::appendPage(xid, tree, lt->lastLeaf, currkey, keylen, lt->alloc_region, &alloc_conf, i + OFFSET);

        //DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);
        // XXX get rid of Tset by storing next page in memory, and losing it
        //     on crash.
        Tset(xid,rid,&alloc_conf);
      
      
        pagenum = logtree::findPage(xid, tree, currkey,keylen);
        oldpagenum = pagenum;
        printf("pagenum:%lld\n", (long long)pagenum);
        assert(pagenum == i + OFFSET);
        free(currkey);
    }

    printf("Writes complete.");
  
    tree = lt->get_root_rec();
    Tcommit(xid);
    xid = Tbegin();

    printf("\nTREE STRUCTURE\n");
    lt->print_tree(xid);
  
    for(int32_t i = 1; i < NUM_ENTRIES; i++) {
        int keylen = sizeof(int32_t);
        byte *currkey = (byte*)malloc(keylen);
        memcpy(currkey, (byte*)(&i), keylen);

        printf("\n#########\ni=%d\nkey:\t%d\nkeylen:%d\n",i,*((int32_t*)currkey),keylen);
        pageid_t pagenum = logtree::findPage(xid, tree, currkey, keylen);
        printf("pagenum:%lld\n", (long long) pagenum);
        assert(pagenum == i + OFFSET);
        free(currkey);
    }

    /*
      int64_t count = 0;

      lladdIterator_t * it = lsmTreeIterator_open(xid, tree);

      while(lsmTreeIterator_next(xid, it)) {
      lsmkey_t * key;
      lsmkey_t **key_ptr = &key;
      int size = lsmTreeIterator_key(xid, it, (byte**)key_ptr);
      assert(size == sizeof(lsmkey_t));
      long *value;
      long **value_ptr = &value;
      size = lsmTreeIterator_value(xid, it, (byte**)value_ptr);
      assert(size == sizeof(pageid_t));
      assert(*key + OFFSET == *value);
      assert(*key == count);
      count++;
      }
      assert(count == NUM_ENTRIES);

      lsmTreeIterator_close(xid, it);

    */
    Tcommit(xid);
    Tdeinit();
}

/** @test
 */
int main()
{
    insertProbeIter_str(NUM_ENTRIES_A);
    //insertProbeIter_int(NUM_ENTRIES_A);

    
    
    return 0;
}


