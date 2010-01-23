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


#include "datapage.h"
#include "tuplemerger.h"
#include "datatuple.h"


double tv_to_double(struct timeval tv);


struct logtable_mergedata;



typedef struct RegionAllocConf_t
{
  recordid regionList;
  pageid_t regionCount;
  pageid_t nextPage;
  pageid_t endOfRegion;
  pageid_t regionSize;
} RegionAllocConf_t;


//struct logtree_state {
//  pageid_t lastLeaf;
//};


struct indexnode_rec {
    pageid_t ptr;
};

typedef pageid_t(*logtree_page_allocator_t)(int, void *);
typedef void(*logtree_page_deallocator_t)(int, void *);


class logtree{
public:
    logtree();

    recordid create(int xid);

    void print_tree(int xid);
    
    static pageid_t alloc_region(int xid, void *conf);
    static pageid_t alloc_region_rid(int xid, void * ridp);
    static void force_region_rid(int xid, void *conf);
    static void dealloc_region_rid(int xid, void *conf);
    static void free_region_rid(int xid, recordid tree,
                                logtree_page_deallocator_t dealloc,
                                void *allocator_state);

    static void writeNodeRecord(int xid, Page *p, recordid &rid, 
                         const byte *key, size_t keylen, pageid_t ptr);

    static void writeRecord(int xid, Page *p, recordid &rid,
                            const byte *data, size_t datalen);

    static void writeRecord(int xid, Page *p, slotid_t slot,
                            const byte *data, size_t datalen);

    static const byte* readRecord(int xid, Page * p, recordid &rid);
    static const byte* readRecord(int xid, Page * p, slotid_t slot, int64_t size);

    static int32_t readRecordLength(int xid, Page *p, slotid_t slot);

    //return the left-most leaf, these are not data pages, although referred to as leaf
    static pageid_t findFirstLeaf(int xid, Page *root, int64_t depth);
    //return the right-most leaf
    static pageid_t findLastLeaf(int xid, Page *root, int64_t depth) ;

    //reads the given record and returns the page id stored in it
    static pageid_t lookupLeafPageFromRid(int xid, recordid rid);
    
    //returns a record that stores the pageid where the given key should be in, i.e. if it exists
    static recordid lookup(int xid, Page *node, int64_t depth, const byte *key,
                              size_t keySize);

    //returns the id of the data page that could contain the given key
    static pageid_t findPage(int xid, recordid tree, const byte *key, size_t keySize);


    //appends a leaf page, val_page is the id of the leaf page
    //rmLeafID --> rightmost leaf id
    static recordid appendPage(int xid, recordid tree, pageid_t & rmLeafID,
                               const byte *key,size_t keySize,
                               logtree_page_allocator_t allocator, void *allocator_state,
                               long val_page);

    static recordid appendInternalNode(int xid, Page *p,
                                       int64_t depth,
                                       const byte *key, size_t key_len,
                                       pageid_t val_page, pageid_t lastLeaf,
                                       logtree_page_allocator_t allocator,
                                       void *allocator_state);

    static recordid buildPathToLeaf(int xid, recordid root, Page *root_p,
                                    int64_t depth, const byte *key, size_t key_len,
                                    pageid_t val_page, pageid_t lastLeaf,
                                    logtree_page_allocator_t allocator,
                                    void *allocator_state);



    /**
       Initialize a page for use as an internal node of the tree.
     */
    inline static void initializeNodePage(int xid, Page *p);
    
    recordid &get_tree_state(){return tree_state;}
    recordid &get_root_rec(){return root_rec;}
    
public:

    const static RegionAllocConf_t REGION_ALLOC_STATIC_INITIALIZER;
    const static int64_t DEPTH;
    const static int64_t COMPARATOR;
    const static int64_t FIRST_SLOT;
    const static size_t root_rec_size;
    const static int64_t PREV_LEAF;
    const static int64_t NEXT_LEAF;
    
    pageid_t lastLeaf;    
private:

    void print_tree(int xid, pageid_t pid, int64_t depth);
    
private:
    recordid tree_state;
    recordid root_rec;



    
};


class logtable
{
public:
    logtable();
    ~logtable();

    //user access functions
    datatuple * findTuple(int xid, datatuple::key_t key, size_t keySize);

    datatuple * findTuple_first(int xid, datatuple::key_t key, size_t keySize);
    
    void insertTuple(struct datatuple &tuple);


    //other class functions
    recordid allocTable(int xid);

    void flushTable();    
    
    DataPage<datatuple>* insertTuple(int xid, struct datatuple &tuple, recordid &dpstate,logtree *ltree);

    datatuple * findTuple(int xid, datatuple::key_t key, size_t keySize,  logtree *ltree);

    inline recordid & get_table_rec(){return table_rec;}
    
    inline logtree * get_tree_c2(){return tree_c2;}
    inline logtree * get_tree_c1(){return tree_c1;}

    inline void set_tree_c1(logtree *t){tree_c1=t;}
    inline void set_tree_c2(logtree *t){tree_c2=t;}
    
    typedef std::set<datatuple, datatuple> rbtree_t;
    typedef rbtree_t* rbtree_ptr_t;
    inline rbtree_ptr_t get_tree_c0(){return tree_c0;}
    
    void set_tree_c0(rbtree_ptr_t newtree){tree_c0 = newtree;}

    inline recordid & get_dpstate1(){return tbl_header.c1_dp_state;}
    inline recordid & get_dpstate2(){return tbl_header.c2_dp_state;}

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
        //epoch_t beginning;
        //epoch_t end;

    };

    const static RegionAllocConf_t DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER;

    logtable_mergedata * mergedata;
    
private:

    

private:    
    recordid table_rec;
    struct table_header tbl_header;
    
    logtree *tree_c2; //big tree
    logtree *tree_c1; //small tree
    rbtree_ptr_t tree_c0; // in-mem red black tree


    int tsize; //number of tuples
    int64_t tree_bytes; //number of bytes

    
    //DATA PAGE SETTINGS
    int fixed_page_count;//number of pages in a datapage

//    logtable_mergedata * mergedata;

    tuplemerger *tmerger;
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
    //static lladdIterator_t *copy(int xid, lladdIterator_t* i);
    static void close(int xid, lladdIterator_t *it);

    
    static inline int key (int xid, lladdIterator_t *it, byte **key)
        {
            logtreeIterator_s * impl = (logtreeIterator_s*)it->impl;
            *key = (byte*)(impl->t+1);
            return (int) impl->current.size - sizeof(indexnode_rec);        
        }
    
    
    static inline int value(int xid, lladdIterator_t *it, byte **value)
        {
            logtreeIterator_s * impl = (logtreeIterator_s*)it->impl;
            *value = (byte*)&(impl->t->ptr);
            return sizeof(impl->t->ptr);
        }
    
    static inline void tupleDone(int xid, void *it) { }
    static inline void releaseLock(int xid, void *it) { }

};


#endif
