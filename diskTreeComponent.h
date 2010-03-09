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

#include "regionAllocator.h"
#include "datapage.h"
#include "tuplemerger.h"
#include "datatuple.h"

class diskTreeComponent {

public:
  class internalNodes{
  public:
    struct indexnode_rec {
      pageid_t ptr;
    };

    typedef struct RegionAllocConf_t
    {
      recordid regionList;
      pageid_t regionCount;
      pageid_t nextPage;
      pageid_t endOfRegion;
      pageid_t regionSize;
    } RegionAllocConf_t;

    typedef pageid_t(*diskTreeComponent_page_allocator_t)(int, void *);
    typedef void(*diskTreeComponent_page_deallocator_t)(int, void *);


    internalNodes(int xid): datapage_alloc(new RegionAllocator(xid, 10000)) {create(xid);}  // XXX shouldn't hardcode region size.
    internalNodes(int xid, recordid root, recordid internal_node_state, recordid datapage_state)
    : internal_node_alloc(internal_node_state),
      root_rec(root),
      datapage_alloc(new RegionAllocator(xid, datapage_state)) { lastLeaf = -1; }
  private:
    recordid create(int xid);
  public:
    void print_tree(int xid);

    static void init_stasis();
    static void deinit_stasis();
  private:
    static pageid_t alloc_region(int xid, void *conf);

    static void writeNodeRecord(int xid, Page *p, recordid &rid,
                                const byte *key, size_t keylen, pageid_t ptr);
    //reads the given record and returns the page id stored in it
    static pageid_t lookupLeafPageFromRid(int xid, recordid rid);

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


    /**
       Initialize a page for use as an internal node of the tree.
     */
    inline static void initializeNodePage(int xid, Page *p);


    //return the left-most leaf, these are not data pages, although referred to as leaf
    static pageid_t findFirstLeaf(int xid, Page *root, int64_t depth);
    //return the right-most leaf
    static pageid_t findLastLeaf(int xid, Page *root, int64_t depth) ;


    //returns a record that stores the pageid where the given key should be in, i.e. if it exists
    static recordid lookup(int xid, Page *node, int64_t depth, const byte *key,
                           size_t keySize);

  public:
    static pageid_t alloc_region_rid(int xid, void * ridp);
    static void force_region_rid(int xid, recordid rid);
    static pageid_t*list_region_rid(int xid, void * ridp,
                                    pageid_t * region_len, pageid_t * region_count);
    static void dealloc_region_rid(int xid, recordid rid);
    static void free_region_rid(int xid, recordid tree,
                                diskTreeComponent_page_deallocator_t dealloc,
                                void *allocator_state);

    //returns the id of the data page that could contain the given key
    pageid_t findPage(int xid, const byte *key, size_t keySize);


    //appends a leaf page, val_page is the id of the leaf page
    //rmLeafID --> rightmost leaf id
    recordid appendPage(int xid,// recordid tree,
                               const byte *key,size_t keySize, pageid_t val_page);

    inline RegionAllocator* get_datapage_alloc() { return datapage_alloc; }

    recordid &get_tree_state(){return internal_node_alloc;}
    recordid &get_root_rec(){return root_rec;}

  private:
    const static int64_t DEPTH;
    const static int64_t COMPARATOR;
    const static int64_t FIRST_SLOT;
    const static size_t root_rec_size;
    const static int64_t PREV_LEAF;
    const static int64_t NEXT_LEAF;
  public:
    const static RegionAllocConf_t REGION_ALLOC_STATIC_INITIALIZER;
  private:
    pageid_t lastLeaf;

    void print_tree(int xid, pageid_t pid, int64_t depth);

    recordid internal_node_alloc;
    recordid root_rec;

    RegionAllocator* datapage_alloc;

  public:
    class iterator {
    public:
      iterator(int xid, recordid root);
      iterator(int xid, recordid root, const byte* key, len_t keylen);
      int next();
      void close();


      inline size_t key (byte **key) {
        *key = (byte*)(t+1);
        return current.size - sizeof(indexnode_rec);
      }

      inline size_t value(byte **value) {
        *value = (byte*)&(t->ptr);
        return sizeof(t->ptr);
      }

      inline void tupleDone() { }
      inline void releaseLock() { }

    private:
      Page * p;
      int xid_;
      bool done;
      recordid current;
      indexnode_rec *t;
      int justOnePage;

    };
  };
};
#endif /* DISKTREECOMPONENT_H_ */
