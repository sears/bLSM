#ifndef _LOGSTORE_H_
#define _LOGSTORE_H_

#undef end
#undef begin

#include <string>
#include <set>
#include <sstream>
#include <iostream>
#include <queue>
#include <vector>

#include "logserver.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <pthread.h>



#include <stasis/transactional.h>

#include <stasis/operations.h>
#include <stasis/bufferManager.h>
#include <stasis/allocationPolicy.h>
#include <stasis/blobManager.h>
#include <stasis/page.h>
#include <stasis/truncation.h>

#include "diskTreeComponent.h"

#include "datapage.h"
#include "tuplemerger.h"
#include "datatuple.h"

struct logtable_mergedata;

typedef std::set<datatuple*, datatuple> rbtree_t;
typedef rbtree_t* rbtree_ptr_t;

struct indexnode_rec {
    pageid_t ptr;
};

class logtable
{
public:
    logtable();
    ~logtable();

    //user access functions
    datatuple * findTuple(int xid, const datatuple::key_t key, size_t keySize);

    datatuple * findTuple_first(int xid, datatuple::key_t key, size_t keySize);
    
    void insertTuple(struct datatuple *tuple);

    //other class functions
    recordid allocTable(int xid);

    void flushTable();    
    
    static void tearDownTree(rbtree_ptr_t t);

    DataPage<datatuple>* insertTuple(int xid, datatuple *tuple,diskTreeComponent *ltree);

    datatuple * findTuple(int xid, const datatuple::key_t key, size_t keySize,  diskTreeComponent *ltree);

    inline recordid & get_table_rec(){return table_rec;}  // TODO This is called by merger.cpp for no good reason.  (remove the calls)
    
    inline diskTreeComponent * get_tree_c2(){return tree_c2;}
    inline diskTreeComponent * get_tree_c1(){return tree_c1;}
    inline diskTreeComponent * get_tree_c1_mergeable(){return tree_c1_mergeable;}

    inline void set_tree_c1(diskTreeComponent *t){tree_c1=t;}
    inline void set_tree_c1_mergeable(diskTreeComponent *t){tree_c1_mergeable=t;}
    inline void set_tree_c2(diskTreeComponent *t){tree_c2=t;}
    
    inline rbtree_ptr_t get_tree_c0(){return tree_c0;}
    inline rbtree_ptr_t get_tree_c0_mergeable(){return tree_c0_mergeable;}
    void set_tree_c0(rbtree_ptr_t newtree){tree_c0 = newtree;}
    void set_tree_c0_mergeable(rbtree_ptr_t newtree){tree_c0_mergeable = newtree;}

    int get_fixed_page_count(){return fixed_page_count;}
    void set_fixed_page_count(int count){fixed_page_count = count;}

    void setMergeData(logtable_mergedata * mdata) { this->mergedata = mdata;}
    logtable_mergedata* getMergeData(){return mergedata;}

    inline tuplemerger * gettuplemerger(){return tmerger;}
    
public:

    struct table_header {
        recordid c2_root;     //tree root record --> points to the root of the b-tree
        recordid c2_state;    //tree state --> describes the regions used by the index tree
        recordid c2_dp_state; //data pages state --> regions used by the data pages
        recordid c1_root;
        recordid c1_state;
        recordid c1_dp_state;
    };

    const static RegionAllocConf_t DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER;

    logtable_mergedata * mergedata;
    
    int64_t max_c0_size;

    inline bool is_still_running() { return still_running_; }
    inline void stop() {
    	still_running_ = false;
		// XXX must need to do other things!
    }

private:    
    recordid table_rec;
    struct table_header tbl_header;

    diskTreeComponent *tree_c2; //big tree
    diskTreeComponent *tree_c1; //small tree
    diskTreeComponent *tree_c1_mergeable; //small tree: ready to be merged with c2
    rbtree_ptr_t tree_c0; // in-mem red black tree
    rbtree_ptr_t tree_c0_mergeable; // in-mem red black tree: ready to be merged with c1.

    int tsize; //number of tuples
    int64_t tree_bytes; //number of bytes

    
    //DATA PAGE SETTINGS
    int fixed_page_count;//number of pages in a datapage

    tuplemerger *tmerger;

    bool still_running_;
};


typedef struct logtreeIterator_s {
    Page * p;
    recordid current;
    indexnode_rec *t;    
    int justOnePage;
} logtreeIterator_s;


class logtreeIterator
{
    
public:
    static lladdIterator_t* open(int xid, recordid root);
    static lladdIterator_t* openAt(int xid, recordid root, const byte* key);
    static int next(int xid, lladdIterator_t *it);
    static void close(int xid, lladdIterator_t *it);

    
    static inline size_t key (int xid, lladdIterator_t *it, byte **key)
        {
            logtreeIterator_s * impl = (logtreeIterator_s*)it->impl;
            *key = (byte*)(impl->t+1);
            return impl->current.size - sizeof(indexnode_rec);
        }
    
    
    static inline size_t value(int xid, lladdIterator_t *it, byte **value)
        {
            logtreeIterator_s * impl = (logtreeIterator_s*)it->impl;
            *value = (byte*)&(impl->t->ptr);
            return sizeof(impl->t->ptr);
        }
    
    static inline void tupleDone(int xid, void *it) { }
    static inline void releaseLock(int xid, void *it) { }

};


#endif
