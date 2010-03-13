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

#include "merger.h"
#include "regionAllocator.h"
#include "datapage.h"
#include "tuplemerger.h"
#include "datatuple.h"

class diskTreeComponent {
 public:
  class internalNodes;
  class diskTreeIterator;

  diskTreeComponent(int xid, pageid_t internal_region_size, pageid_t datapage_region_size, pageid_t datapage_size) :
    ltree(new diskTreeComponent::internalNodes(xid, internal_region_size, datapage_region_size, datapage_size)),
    dp(0),
    datapage_size(datapage_size) {}


  diskTreeComponent(int xid, recordid root, recordid internal_node_state, recordid datapage_state) :
    ltree(new diskTreeComponent::internalNodes(xid, root, internal_node_state, datapage_state)),
    dp(0) {}

  ~diskTreeComponent() {
    delete dp;
    delete ltree;
  }

  recordid get_root_rid() { return ltree->get_root_rec(); }
  recordid get_datapage_allocator_rid() { return ltree->get_datapage_alloc()->header_rid(); }
  recordid get_internal_node_allocator_rid() { return ltree->get_internal_node_alloc()->header_rid(); }
  internalNodes * get_internal_nodes() { return ltree; }
  datatuple* findTuple(int xid, datatuple::key_t key, size_t keySize);
  int insertTuple(int xid, /*DataPage<datatuple> *dp,*/ datatuple *t, merge_stats_t *stats);
  void writes_done();


  diskTreeIterator * iterator() {
    return new diskTreeIterator(ltree);
  }
  diskTreeIterator * iterator(datatuple * key) {
    return new diskTreeIterator(ltree, key);
  }

  void force(int xid) {
    ltree->get_datapage_alloc()->force_regions(xid);
    ltree->get_internal_node_alloc()->force_regions(xid);
  }
  void dealloc(int xid) {
    ltree->get_datapage_alloc()->dealloc_regions(xid);
    ltree->get_internal_node_alloc()->dealloc_regions(xid);
  }
  void list_regions(int xid, pageid_t *internal_node_region_length, pageid_t *internal_node_region_count, pageid_t **internal_node_regions,
		    pageid_t *datapage_region_length, pageid_t *datapage_region_count, pageid_t **datapage_regions) {
    *internal_node_regions = ltree->get_internal_node_alloc()->list_regions(xid, internal_node_region_length, internal_node_region_count);
    *datapage_regions      = ltree->get_datapage_alloc()     ->list_regions(xid, datapage_region_length, datapage_region_count);
  }

  void print_tree(int xid) {
    ltree->print_tree(xid);
  }



 private:
  DataPage<datatuple>* insertDataPage(int xid, datatuple *tuple);

  internalNodes * ltree;
  DataPage<datatuple>* dp;
  pageid_t datapage_size;

 public:
  class internalNodes{
  public:
    // XXX move these to another module.
    static void init_stasis();
    static void deinit_stasis();

    internalNodes(int xid, pageid_t internal_region_size, pageid_t datapage_region_size, pageid_t datapage_size)
    : lastLeaf(-1),
      internal_node_alloc(new RegionAllocator(xid, internal_region_size)),
      datapage_alloc(new RegionAllocator(xid, datapage_region_size))
    { create(xid); }

    internalNodes(int xid, recordid root, recordid internal_node_state, recordid datapage_state)
    : lastLeaf(-1),
      root_rec(root),
      internal_node_alloc(new RegionAllocator(xid, internal_node_state)),
      datapage_alloc(new RegionAllocator(xid, datapage_state))
    { }

    void print_tree(int xid);

    //returns the id of the data page that could contain the given key
    pageid_t findPage(int xid, const byte *key, size_t keySize);

    //appends a leaf page, val_page is the id of the leaf page
    recordid appendPage(int xid, const byte *key,size_t keySize, pageid_t val_page);

    inline RegionAllocator* get_datapage_alloc() { return datapage_alloc; }
    inline RegionAllocator* get_internal_node_alloc() { return internal_node_alloc; }
    const recordid &get_root_rec(){return root_rec;}

  private:
    recordid create(int xid);

    void writeNodeRecord(int xid, Page *p, recordid &rid,
                                const byte *key, size_t keylen, pageid_t ptr);
    //reads the given record and returns the page id stored in it
    static pageid_t lookupLeafPageFromRid(int xid, recordid rid);

    recordid appendInternalNode(int xid, Page *p,
                                       int64_t depth,
                                       const byte *key, size_t key_len,
                                       pageid_t val_page);

    recordid buildPathToLeaf(int xid, recordid root, Page *root_p,
                                    int64_t depth, const byte *key, size_t key_len,
                                    pageid_t val_page);

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

    const static int64_t DEPTH;
    const static int64_t COMPARATOR;
    const static int64_t FIRST_SLOT;
    const static size_t root_rec_size;
    const static int64_t PREV_LEAF;
    const static int64_t NEXT_LEAF;
    pageid_t lastLeaf;

    void print_tree(int xid, pageid_t pid, int64_t depth);

    recordid root_rec;
    RegionAllocator* internal_node_alloc;
    RegionAllocator* datapage_alloc;

    struct indexnode_rec {
      pageid_t ptr;
    };

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
  class diskTreeIterator
  {

  public:
      explicit diskTreeIterator(diskTreeComponent::internalNodes *tree);

      explicit diskTreeIterator(diskTreeComponent::internalNodes *tree,datatuple *key);

      ~diskTreeIterator();

      datatuple * next_callerFrees();

  private:
      void init_iterators(datatuple * key1, datatuple * key2);
      inline void init_helper(datatuple * key1);

    explicit diskTreeIterator() { abort(); }
    void operator=(diskTreeIterator & t) { abort(); }
    int operator-(diskTreeIterator & t) { abort(); }

  private:
      recordid tree_; //root of the tree

      diskTreeComponent::internalNodes::iterator* lsmIterator_;

      pageid_t curr_pageid; //current page id
      DataPage<datatuple>    *curr_page;   //current page
      typedef DataPage<datatuple>::iterator DPITR_T;
      DPITR_T *dp_itr;
  };
};
#endif /* DISKTREECOMPONENT_H_ */
