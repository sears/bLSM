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

    internalNodes(int xid)
    : lastLeaf(-1),
      internal_node_alloc(new RegionAllocator(xid, 1000)),
      datapage_alloc(new RegionAllocator(xid, 10000))
    { create(xid); }  // XXX shouldn't hardcode region size.

    internalNodes(int xid, recordid root, recordid internal_node_state, recordid datapage_state)
    : lastLeaf(-1),
      root_rec(root),
      internal_node_alloc(new RegionAllocator(xid, internal_node_state)),
      datapage_alloc(new RegionAllocator(xid, datapage_state))
      { }
  private:
    recordid create(int xid);
  public:
    void print_tree(int xid);

    static void init_stasis();
    static void deinit_stasis();
  private:

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

  public:

    //returns the id of the data page that could contain the given key
    pageid_t findPage(int xid, const byte *key, size_t keySize);


    //appends a leaf page, val_page is the id of the leaf page
    recordid appendPage(int xid, const byte *key,size_t keySize, pageid_t val_page);

    inline RegionAllocator* get_datapage_alloc() { return datapage_alloc; }
    inline RegionAllocator* get_internal_node_alloc() { return internal_node_alloc; }
    const recordid &get_root_rec(){return root_rec;}

  private:
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
