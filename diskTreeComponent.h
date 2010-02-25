/*
 * diskTreeComponent.h
 *
 *  Created on: Feb 18, 2010
 *      Author: sears
 */

#ifndef DISKTREECOMPONENT_H_
#define DISKTREECOMPONENT_H_

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


typedef struct RegionAllocConf_t
{
  recordid regionList;
  pageid_t regionCount;
  pageid_t nextPage;
  pageid_t endOfRegion;
  pageid_t regionSize;
} RegionAllocConf_t;

struct indexnode_rec {
    pageid_t ptr;
};

typedef pageid_t(*diskTreeComponent_page_allocator_t)(int, void *);
typedef void(*diskTreeComponent_page_deallocator_t)(int, void *);

class diskTreeComponent{
public:
    diskTreeComponent(int xid): region_alloc(new DataPage<datatuple>::RegionAllocator(xid, 10000)) {create(xid);}  // XXX shouldn't hardcode region size.
private:
    recordid create(int xid);
public:
    void print_tree(int xid);

    static void init_stasis();
    static void deinit_stasis();
private:
    static pageid_t alloc_region(int xid, void *conf);
public:
    static pageid_t alloc_region_rid(int xid, void * ridp);
    static void force_region_rid(int xid, recordid rid);
    static pageid_t*list_region_rid(int xid, void * ridp, pageid_t * region_len, pageid_t * region_count);
    static void dealloc_region_rid(int xid, recordid rid);
    static void free_region_rid(int xid, recordid tree,
                                diskTreeComponent_page_deallocator_t dealloc,
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
                               diskTreeComponent_page_allocator_t allocator, void *allocator_state,
                               long val_page);

    static recordid appendInternalNode(int xid, Page *p,
                                       int64_t depth,
                                       const byte *key, size_t key_len,
                                       pageid_t val_page, pageid_t lastLeaf,
                                       diskTreeComponent_page_allocator_t allocator,
                                       void *allocator_state);

    static recordid buildPathToLeaf(int xid, recordid root, Page *root_p,
                                    int64_t depth, const byte *key, size_t key_len,
                                    pageid_t val_page, pageid_t lastLeaf,
                                    diskTreeComponent_page_allocator_t allocator,
                                    void *allocator_state);

    inline DataPage<datatuple>::RegionAllocator* get_alloc() { return region_alloc; }

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

    DataPage<datatuple>::RegionAllocator* region_alloc;


};


typedef struct {
    Page * p;
    recordid current;
    indexnode_rec *t;
    int justOnePage;
} diskTreeComponentIterator_t;


class diskTreeComponentIterator
{

public:
    static lladdIterator_t* open(int xid, recordid root);
    static lladdIterator_t* openAt(int xid, recordid root, const byte* key, len_t keylen);
    static int next(int xid, lladdIterator_t *it);
    static void close(int xid, lladdIterator_t *it);


    static inline size_t key (int xid, lladdIterator_t *it, byte **key)
        {
            diskTreeComponentIterator_t * impl = (diskTreeComponentIterator_t*)it->impl;
            *key = (byte*)(impl->t+1);
            return impl->current.size - sizeof(indexnode_rec);
        }


    static inline size_t value(int xid, lladdIterator_t *it, byte **value)
        {
            diskTreeComponentIterator_t * impl = (diskTreeComponentIterator_t*)it->impl;
            *value = (byte*)&(impl->t->ptr);
            return sizeof(impl->t->ptr);
        }

    static inline void tupleDone(int xid, void *it) { }
    static inline void releaseLock(int xid, void *it) { }

};



#endif /* DISKTREECOMPONENT_H_ */
