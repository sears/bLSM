/*
 * diskTreeComponent.cpp
 *
 *  Created on: Feb 18, 2010
 *      Author: sears
 */

#include <string.h>
#include <assert.h>
#include <math.h>
#include <ctype.h>

#include "merger.h"
#include "diskTreeComponent.h"
#include "regionAllocator.h"

#include <stasis/transactional.h>
#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>

/////////////////////////////////////////////////////////////////
// LOGTREE implementation
/////////////////////////////////////////////////////////////////

//LSM_ROOT_PAGE

const int64_t diskTreeComponent::internalNodes::DEPTH = 0;      //in root this is the slot num where the DEPTH (of tree) is stored
const int64_t diskTreeComponent::internalNodes::COMPARATOR = 1; //in root this is the slot num where the COMPARATOR id is stored
const int64_t diskTreeComponent::internalNodes::FIRST_SLOT = 2; //this is the first unused slot in all index pages
const size_t diskTreeComponent::internalNodes::root_rec_size = sizeof(int64_t);
const int64_t diskTreeComponent::internalNodes::PREV_LEAF = 0; //pointer to prev leaf page
const int64_t diskTreeComponent::internalNodes::NEXT_LEAF = 1; //pointer to next leaf page

// TODO move init_stasis to a more appropriate module

void diskTreeComponent::internalNodes::init_stasis() {

  bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_PFILE;

  DataPage<datatuple>::register_stasis_page_impl();

  stasis_buffer_manager_factory = stasis_buffer_manager_hash_factory; // XXX workaround stasis issue #22.

  Tinit();

}

recordid diskTreeComponent::get_root_rid() { return ltree->get_root_rec(); }
recordid diskTreeComponent::get_datapage_allocator_rid() { return ltree->get_datapage_alloc()->header_rid(); }
recordid diskTreeComponent::get_internal_node_allocator_rid() { return ltree->get_internal_node_alloc()->header_rid(); }



void diskTreeComponent::force(int xid) {
  ltree->get_datapage_alloc()->force_regions(xid);
  ltree->get_internal_node_alloc()->force_regions(xid);
}
void diskTreeComponent::dealloc(int xid) {
  ltree->get_datapage_alloc()->dealloc_regions(xid);
  ltree->get_internal_node_alloc()->dealloc_regions(xid);
}
void diskTreeComponent::list_regions(int xid, pageid_t *internal_node_region_length, pageid_t *internal_node_region_count, pageid_t **internal_node_regions,
          pageid_t *datapage_region_length, pageid_t *datapage_region_count, pageid_t **datapage_regions) {
  *internal_node_regions = ltree->get_internal_node_alloc()->list_regions(xid, internal_node_region_length, internal_node_region_count);
  *datapage_regions      = ltree->get_datapage_alloc()     ->list_regions(xid, datapage_region_length, datapage_region_count);
}


void diskTreeComponent::writes_done() {
  if(dp) {
    dp->writes_done();
    delete dp;
    dp = 0;
  }
}

int diskTreeComponent::insertTuple(int xid, datatuple *t, merge_stats_t *stats)
{
  int ret = 0; // no error.
  if(dp==0) {
    dp = insertDataPage(xid, t);
    stats->num_datapages_out++;
  } else if(!dp->append(t)) {
    stats->bytes_out += (PAGE_SIZE * dp->get_page_count());
    dp->writes_done();
    delete dp;
    dp = insertDataPage(xid, t);
    stats->num_datapages_out++;
  }
  return ret;
}

DataPage<datatuple>* diskTreeComponent::insertDataPage(int xid, datatuple *tuple) {
    //create a new data page -- either the last region is full, or the last data page doesn't want our tuple.  (or both)

    DataPage<datatuple> * dp = 0;
    int count = 0;
    while(dp==0)
    {
      dp = new DataPage<datatuple>(xid, datapage_size, ltree->get_datapage_alloc());

        //insert the record into the data page
        if(!dp->append(tuple))
        {
            // the last datapage must have not wanted the tuple, and then this datapage figured out the region is full.
            dp->writes_done();
            delete dp;
            dp = 0;
            assert(count == 0); // only retry once.
            count ++;
        }
    }


    ltree->appendPage(xid,
                        tuple->key(),
                        tuple->keylen(),
                        dp->get_start_pid()
                        );


    //return the datapage
    return dp;
}

datatuple * diskTreeComponent::findTuple(int xid, datatuple::key_t key, size_t keySize)
{
    datatuple * tup=0;

    //find the datapage
    pageid_t pid = ltree->findPage(xid, (byte*)key, keySize);

    if(pid!=-1)
    {
        DataPage<datatuple> * dp = new DataPage<datatuple>(xid, pid);
        dp->recordRead(key, keySize, &tup);
        delete dp;
    }
    return tup;
}

void diskTreeComponent::internalNodes::deinit_stasis() { Tdeinit(); }
recordid diskTreeComponent::internalNodes::create(int xid) {

  pageid_t root = internal_node_alloc->alloc_extent(xid, 1);
  DEBUG("Root = %lld\n", root);
  recordid ret = { root, 0, 0 };

  Page *p = loadPage(xid, ret.page);
  writelock(p->rwlatch,0);

  //initialize root node
  stasis_page_slotted_initialize_page(p);
  recordid tmp  = stasis_record_alloc_begin(xid, p, root_rec_size);
  stasis_record_alloc_done(xid,p,tmp);

  assert(tmp.page == ret.page
         && tmp.slot == DEPTH
         && tmp.size == root_rec_size);

  int64_t zero = 0;
  assert(sizeof(zero) == root_rec_size);
  stasis_record_write(xid, p, tmp, (byte*)&zero);

  tmp = stasis_record_alloc_begin(xid, p, root_rec_size);
  stasis_record_alloc_done(xid,p,tmp);

  assert(tmp.page == ret.page
         && tmp.slot == COMPARATOR
         && tmp.size == root_rec_size);

  stasis_record_write(xid, p, tmp, (byte*)&zero);

  stasis_page_lsn_write(xid, p, internal_node_alloc->get_lsn(xid));

  unlock(p->rwlatch);
  releasePage(p);

  root_rec = ret;

  return ret;
}

void diskTreeComponent::internalNodes::writeNodeRecord(int xid, Page * p, recordid & rid,
                              const byte *key, size_t keylen, pageid_t ptr) {
  DEBUG("writenoderecord:\tp->id\t%lld\tkey:\t%s\tkeylen: %d\tval_page\t%lld\n",
        p->id, datatuple::key_to_str(key).c_str(), keylen, ptr);
  indexnode_rec *nr = (indexnode_rec*)stasis_record_write_begin(xid, p, rid);
  nr->ptr = ptr;
  memcpy(nr+1, key, keylen);
  stasis_record_write_done(xid, p, rid, (byte*)nr);
  stasis_page_lsn_write(xid, p, internal_node_alloc->get_lsn(xid));
}

void diskTreeComponent::internalNodes::initializeNodePage(int xid, Page *p) {
  stasis_page_slotted_initialize_page(p);
  recordid reserved1 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
  stasis_record_alloc_done(xid, p, reserved1);
  recordid reserved2 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
  stasis_record_alloc_done(xid, p, reserved2);
}


recordid diskTreeComponent::internalNodes::appendPage(int xid,
                             const byte *key, size_t keySize, pageid_t val_page) {
  recordid tree = root_rec;

  Page *p = loadPage(xid, tree.page);
  writelock(p->rwlatch, 0);

  tree.slot = DEPTH;
  tree.size = 0;

  const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid, p, tree);
  int64_t depth = *((int64_t*)nr);
  stasis_record_read_done(xid, p, tree, (const byte*)nr);

  if(lastLeaf == -1) {
    lastLeaf = findLastLeaf(xid, p, depth);
  }

  Page *lastLeafPage;

  if(lastLeaf != tree.page) {
    lastLeafPage= loadPage(xid, lastLeaf);
    writelock(lastLeafPage->rwlatch, 0);
  } else {
    lastLeafPage = p;
  }

  recordid ret = stasis_record_alloc_begin(xid, lastLeafPage,
                                           sizeof(indexnode_rec)+keySize);

  if(ret.size == INVALID_SLOT) {
    if(lastLeafPage->id != p->id) {
      assert(lastLeaf != tree.page);
      unlock(lastLeafPage->rwlatch);
      releasePage(lastLeafPage); // don't need that page anymore...
      lastLeafPage = 0;
    }
    // traverse down the root of the tree.

    tree.slot = 0;

    assert(tree.page == p->id);

    ret = appendInternalNode(xid, p, depth, key, keySize, val_page);

    if(ret.size == INVALID_SLOT) {
      DEBUG("Need to split root; depth = %d\n", depth);

      pageid_t child = internal_node_alloc->alloc_extent(xid, 1);
      Page *lc = loadPage(xid, child);
      writelock(lc->rwlatch,0);

      initializeNodePage(xid, lc);

      //creates a copy of the root page records in the
      //newly allocated child page
      slotid_t numslots = stasis_record_last(xid, p).slot+1;
      recordid rid;
      rid.page = p->id;
      for(rid.slot = FIRST_SLOT; rid.slot < numslots; rid.slot++) {
        //read the record from the root page
        rid.size = stasis_record_length_read(xid, p, rid);
        const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid, p, rid);

        recordid cnext = stasis_record_alloc_begin(xid, lc,rid.size);

        assert(rid.slot == cnext.slot);
        assert(cnext.size != INVALID_SLOT);

        stasis_record_alloc_done(xid, lc, cnext);
        stasis_record_write(xid, lc, cnext, (byte*)nr);
        stasis_record_read_done(xid, p, rid, (const byte*)nr);
      }

      // deallocate old entries, and update pointer on parent node.
      // NOTE: stasis_record_free call goes to slottedFree in slotted.c
      // this function only reduces the numslots when you call it
      // with the last slot. so thats why i go backwards here.
      DEBUG("slots %d (%d) keysize=%lld\n", (int)last_slot+1, (int)FIRST_SLOT+1, (long long int)keySize);
      assert(numslots >= FIRST_SLOT+1);
      // Note that we leave the first slot in place.
      for(int i = numslots-1; i>FIRST_SLOT; i--) {
        recordid tmp_rec= {p->id, i, INVALID_SIZE};
        stasis_record_free(xid, p, tmp_rec);
      }
      recordid pFirstSlot = stasis_record_last(xid, p);
      assert(pFirstSlot.slot == FIRST_SLOT);
      //TODO: could change with stasis_slotted_page_initialize(...);
      // TODO: fsck?

      // reinsert first.

      indexnode_rec *nr
          = (indexnode_rec*)stasis_record_write_begin(xid, p, pFirstSlot);

      // don't overwrite key...
      nr->ptr = child;
      stasis_record_write_done(xid,p,pFirstSlot,(byte*)nr);

      if(!depth) {
          lastLeaf = lc->id;
          pageid_t tmpid = -1;
          recordid rid = { lc->id, PREV_LEAF, root_rec_size };
          stasis_record_write(xid, lc, rid, (byte*)&tmpid);
          rid.slot = NEXT_LEAF;
          stasis_record_write(xid, lc, rid, (byte*)&tmpid);
      }

      stasis_page_lsn_write(xid, lc, internal_node_alloc->get_lsn(xid));
      unlock(lc->rwlatch);
      releasePage(lc);

      //update the depth info at the root
      depth ++;
      recordid depth_rid = { p->id, DEPTH, root_rec_size };
      stasis_record_write(xid, p, depth_rid, (byte*)(&depth));

      assert(tree.page == p->id);
      ret = appendInternalNode(xid, p, depth, key, keySize, val_page);

      assert(ret.size != INVALID_SLOT);

    } else {
      DEBUG("Appended new internal node tree depth = %lld key = %s\n",
            depth, datatuple::key_to_str(key).c_str());
    }

    lastLeaf = ret.page;
    DEBUG("lastleaf is %lld\n", lastLeaf);

  } else {
    // write the new value to an existing page
    DEBUG("Writing %s\t%d to existing page# %lld\n", datatuple::key_to_str(key).c_str(),
          val_page, lastLeafPage->id);

    stasis_record_alloc_done(xid, lastLeafPage, ret);

    writeNodeRecord(xid, lastLeafPage, ret, key, keySize, val_page);

    if(lastLeafPage->id != p->id) {
      assert(lastLeaf != tree.page);
      unlock(lastLeafPage->rwlatch);
      releasePage(lastLeafPage);
    }
  }

  stasis_page_lsn_write(xid, p, internal_node_alloc->get_lsn(xid));

  unlock(p->rwlatch);
  releasePage(p);

  return ret;
}

diskTreeComponent::internalNodes::internalNodes(int xid, pageid_t internal_region_size, pageid_t datapage_region_size, pageid_t datapage_size)
: lastLeaf(-1),
  internal_node_alloc(new RegionAllocator(xid, internal_region_size)),
  datapage_alloc(new RegionAllocator(xid, datapage_region_size))
{ create(xid); }

diskTreeComponent::internalNodes::internalNodes(int xid, recordid root, recordid internal_node_state, recordid datapage_state)
: lastLeaf(-1),
  root_rec(root),
  internal_node_alloc(new RegionAllocator(xid, internal_node_state)),
  datapage_alloc(new RegionAllocator(xid, datapage_state))
{ }


/* adding pages:

  1) Try to append value to lsmTreeState->lastLeaf

  2) If that fails, traverses down the root of the tree, split pages while
     traversing back up.

  3) Split is done by adding new page at end of row (no key
     redistribution), except at the root, where root contents are
     pushed into the first page of the next row, and a new path from root to
     leaf is created starting with the root's immediate second child.

*/

recordid diskTreeComponent::internalNodes::appendInternalNode(int xid, Page *p,
                                     int64_t depth,
                                     const byte *key, size_t key_len,
                                     pageid_t val_page) {

  assert(p->pageType == SLOTTED_PAGE);

  recordid ret;

  if(!depth) {
    // leaf node.
    ret = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec)+key_len);
    if(ret.size != INVALID_SLOT) {
      stasis_record_alloc_done(xid, p, ret);
      writeNodeRecord(xid,p,ret,key,key_len,val_page);
      stasis_page_lsn_write(xid, p, internal_node_alloc->get_lsn(xid));
    }
  } else {
    
    // recurse
    recordid last_rid = stasis_record_last(xid, p);

    assert(last_rid.slot >= FIRST_SLOT); // there should be no empty nodes
    const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid, p, last_rid);

    pageid_t child_id = nr->ptr;
    stasis_record_read_done(xid, p, last_rid, (const byte*)nr);
    nr = 0;
    {
      Page *child_page = loadPage(xid, child_id);
      writelock(child_page->rwlatch,0);
      ret = appendInternalNode(xid, child_page, depth-1, key, key_len,
                               val_page);

      unlock(child_page->rwlatch);
      releasePage(child_page);
    }

    if(ret.size == INVALID_SLOT) { // subtree is full; split
      ret = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec)+key_len);
      DEBUG("keylen %d\tnumslots %d for page id %lld ret.size %lld  prv rec len %d\n",
            key_len,
            stasis_record_last(xid, p).slot+1,
            p->id,
            ret.size,
            readRecordLength(xid, p, slot));
      if(ret.size != INVALID_SLOT) {
        stasis_record_alloc_done(xid, p, ret);
        ret = buildPathToLeaf(xid, ret, p, depth, key, key_len, val_page);

        DEBUG("split tree rooted at %lld, wrote value to {%d %d %lld}\n",
              p->id, ret.page, ret.slot, ret.size);
      } else {
          // ret is NULLRID; this is the root of a full tree. Return
          // NULLRID to the caller.
      }
    } else {
      // we inserted the value in to a subtree rooted here.
    }
  }
  return ret;
}

recordid diskTreeComponent::internalNodes::buildPathToLeaf(int xid, recordid root, Page *root_p,
                                  int64_t depth, const byte *key, size_t key_len,
                                  pageid_t val_page) {

  // root is the recordid on the root page that should point to the
  // new subtree.
  assert(depth);
  DEBUG("buildPathToLeaf(depth=%lld) (lastleaf=%lld) called\n",depth, lastLeaf);

  pageid_t child = internal_node_alloc->alloc_extent(xid, 1);
  DEBUG("new child = %lld internal? %lld\n", child, depth-1);

  Page *child_p = loadPage(xid, child);
  writelock(child_p->rwlatch,0);
  initializeNodePage(xid, child_p);

  recordid ret;

  if(depth-1) {
    // recurse: the page we just allocated is not a leaf.
    recordid child_rec = stasis_record_alloc_begin(xid, child_p, sizeof(indexnode_rec)+key_len);
    assert(child_rec.size != INVALID_SLOT);
    stasis_record_alloc_done(xid, child_p, child_rec);

    ret = buildPathToLeaf(xid, child_rec, child_p, depth-1, key, key_len,
                          val_page);

    unlock(child_p->rwlatch);
    releasePage(child_p);

  } else {
    // set leaf

    // backward link. records were alloced by page initialization
    recordid prev_leaf_rid = { child_p->id, PREV_LEAF, root_rec_size };
    stasis_record_write(xid, child_p, prev_leaf_rid, (byte*)&lastLeaf);

    // forward link (initialize to -1)
    pageid_t tmp_pid = -1;
    recordid next_leaf_rid = { child_p->id, NEXT_LEAF, root_rec_size };
    stasis_record_write(xid, child_p, next_leaf_rid, (byte*)&tmp_pid);

    recordid leaf_rec = stasis_record_alloc_begin(xid, child_p,
                                     sizeof(indexnode_rec)+key_len);

    assert(leaf_rec.slot == FIRST_SLOT);

    stasis_record_alloc_done(xid, child_p, leaf_rec);
    writeNodeRecord(xid,child_p,leaf_rec,key,key_len,val_page);

    ret = leaf_rec;

    stasis_page_lsn_write(xid, child_p, internal_node_alloc->get_lsn(xid));
    unlock(child_p->rwlatch);
    releasePage(child_p);
    if(lastLeaf != -1 && lastLeaf != root_rec.page) {
      // install forward link in previous page
      Page *lastLeafP = loadPage(xid, lastLeaf);
      writelock(lastLeafP->rwlatch,0);
      recordid last_next_leaf_rid = {lastLeaf, NEXT_LEAF, root_rec_size };
      stasis_record_write(xid,lastLeafP,last_next_leaf_rid,(byte*)&child);
      stasis_page_lsn_write(xid, lastLeafP, internal_node_alloc->get_lsn(xid));
      unlock(lastLeafP->rwlatch);
      releasePage(lastLeafP);
    }

    DEBUG("%lld <-> %lld\n", lastLeaf, child);
  }

  writeNodeRecord(xid, root_p, root, key, key_len, child);

  return ret;

}

/**
 * Traverse from the root of the page to the right most leaf (the one
 * with the higest base key value).
 **/
pageid_t diskTreeComponent::internalNodes::findLastLeaf(int xid, Page *root, int64_t depth) {
  if(!depth) {
    DEBUG("Found last leaf = %lld\n", root->id);
    return root->id;
  } else {
    recordid last_rid = stasis_record_last(xid, root);

    const indexnode_rec *nr = (const indexnode_rec*) stasis_record_read_begin(xid, root, last_rid);
    Page *p = loadPage(xid, nr->ptr);
    stasis_record_read_done(xid, root, last_rid, (const byte*)nr);

    readlock(p->rwlatch,0);
    pageid_t ret = findLastLeaf(xid,p,depth-1);
    unlock(p->rwlatch);
    releasePage(p);

    return ret;
  }
}

/**
 *  Traverse from the root of the tree to the left most (lowest valued
 *  key) leaf.
 */
pageid_t diskTreeComponent::internalNodes::findFirstLeaf(int xid, Page *root, int64_t depth) {

  if(!depth) { //if depth is 0, then returns the id of the page
    return root->id;
  } else {
    recordid rid = {root->id, FIRST_SLOT, 0};
    const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid, root, rid);
    Page *p = loadPage(xid, nr->ptr);
    stasis_record_read_done(xid, root, rid, (const byte*)nr);
    readlock(p->rwlatch,0);
    pageid_t ret = findFirstLeaf(xid,p,depth-1);
    unlock(p->rwlatch);
    releasePage(p);
    return ret;
  }
}


pageid_t diskTreeComponent::internalNodes::findPage(int xid, const byte *key, size_t keySize) {

  Page *p = loadPage(xid, root_rec.page);
  readlock(p->rwlatch,0);

  recordid depth_rid = {p->id, DEPTH, 0};
  int64_t * depth = (int64_t*)stasis_record_read_begin(xid, p, depth_rid);

  recordid rid = lookup(xid, p, *depth, key, keySize);
  stasis_record_read_done(xid, p, depth_rid, (const byte*)depth);

  pageid_t ret = lookupLeafPageFromRid(xid,rid);
  unlock(p->rwlatch);
  releasePage(p);

  return ret;

}

pageid_t diskTreeComponent::internalNodes::lookupLeafPageFromRid(int xid, recordid rid) {

  pageid_t pid = -1;
  if(rid.page != NULLRID.page || rid.slot != NULLRID.slot) {
    Page * p2 = loadPage(xid, rid.page);
    readlock(p2->rwlatch,0);
    const indexnode_rec * nr = (const indexnode_rec*)stasis_record_read_begin(xid, p2, rid);
    pid = nr->ptr;
    stasis_record_read_done(xid, p2, rid, (const byte*)nr);
    unlock(p2->rwlatch);
    releasePage(p2);
  }
  return pid;
}

recordid diskTreeComponent::internalNodes::lookup(int xid,
                            Page *node,
                            int64_t depth,
                            const byte *key, size_t keySize ) {

  //DEBUG("lookup: pid %lld\t depth %lld\n", node->id, depth);
  slotid_t numslots = stasis_record_last(xid, node).slot + 1;

  if(numslots == FIRST_SLOT) {
    return NULLRID;
  }
  assert(numslots > FIRST_SLOT);

  // don't need to compare w/ first item in tree, since we need to position ourselves at the the max tree value <= key.
  // positioning at FIRST_SLOT puts us "before" the first value
  int match = FIRST_SLOT; // (so match is now < key)
  recordid rid;
  rid.page = node->id;
  rid.size = 0;
  for(rid.slot = FIRST_SLOT+1; rid.slot < numslots; rid.slot++) {
    rid.size = stasis_record_length_read(xid, node, rid);

    const indexnode_rec *rec = (const indexnode_rec*)stasis_record_read_begin(xid,node,rid);
    int cmpval = datatuple::compare((datatuple::key_t) (rec+1), rid.size-sizeof(*rec),
                                    (datatuple::key_t) key, keySize);
    stasis_record_read_done(xid,node,rid,(const byte*)rec);

    // key of current node is too big; there can be no matches under it.
    if(cmpval>0) break;

    match = rid.slot; // only increment match after comparing with the current node.
  }
  rid.slot = match;
  rid.size = 0;

  if(depth) {
    const indexnode_rec* nr = (const indexnode_rec*)stasis_record_read_begin(xid, node, rid);
    pageid_t child_id = nr->ptr;
    stasis_record_read_done(xid, node, rid, (const byte*)nr);

    Page* child_page = loadPage(xid, child_id);
    readlock(child_page->rwlatch,0);
    recordid ret = lookup(xid,child_page,depth-1,key,keySize);
    unlock(child_page->rwlatch);
    releasePage(child_page);
    return ret;
  } else {
    recordid ret = {node->id, match, keySize};
    return ret;
  }
}

void diskTreeComponent::internalNodes::print_tree(int xid) {
  Page *p = loadPage(xid, root_rec.page);
  readlock(p->rwlatch,0);
  recordid depth_rid = {p->id, DEPTH, 0};
  const indexnode_rec *depth_nr = (const indexnode_rec*)stasis_record_read_begin(xid, p , depth_rid);

  int64_t depth = depth_nr->ptr;
  stasis_record_read_done(xid,p,depth_rid,(const byte*)depth_nr);

  print_tree(xid, root_rec.page, depth);

  unlock(p->rwlatch);
  releasePage(p);

}

void diskTreeComponent::internalNodes::print_tree(int xid, pageid_t pid, int64_t depth) {

  Page *node = loadPage(xid, pid);
  readlock(node->rwlatch,0);

  slotid_t numslots = stasis_record_last(xid,node).slot + 1;

  printf("page_id:%lld\tnum_slots:%d\t\n", node->id, numslots);

  if(numslots == FIRST_SLOT) {
    return;
  }

  assert(numslots > FIRST_SLOT);

  recordid rid = { node->id, 0, 0 };

  if(depth) {
    printf("\tnot_leaf\n");

    for(int i = FIRST_SLOT; i < numslots; i++) {
      rid.slot = i;
      const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid,node,rid);
      printf("\tchild_page_id:%lld\tkey:%s\n", nr->ptr,
             datatuple::key_to_str((byte*)(nr+1)).c_str());
      stasis_record_read_done(xid, node, rid, (const byte*)nr);
    }

    for(int i = FIRST_SLOT; i < numslots; i++) {
      rid.slot = i;
      const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid,node,rid);
      print_tree(xid, nr->ptr, depth-1);
      stasis_record_read_done(xid, node, rid, (const byte*)nr);
    }

  } else {
    printf("\tis_leaf\t\n");

    rid.slot = FIRST_SLOT;
    const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid,node,rid);
    printf("\tdata_page_id:%lld\tkey:%s\n", nr->ptr,
           datatuple::key_to_str((byte*)(nr+1)).c_str());
    stasis_record_read_done(xid, node, rid, (const byte*)nr);

    printf("\t...\n");

    rid.slot= numslots - 1;
    nr = (const indexnode_rec*)stasis_record_read_begin(xid,node,rid);
    printf("\tdata_page_id:%lld\tkey:%s\n", nr->ptr,
           datatuple::key_to_str((byte*)(nr+1)).c_str());
    stasis_record_read_done(xid, node, rid, (const byte*)nr);
}
  unlock(node->rwlatch);
  releasePage(node);
}

/////////////////////////////////////////////////
//diskTreeComponentIterator implementation
/////////////////////////////////////////////////

diskTreeComponent::internalNodes::iterator::iterator(int xid, recordid root) {
  if(root.page == 0 && root.slot == 0 && root.size == -1) abort();
  p = loadPage(xid,root.page);
  readlock(p->rwlatch,0);

  DEBUG("ROOT_REC_SIZE %d\n", diskTreeComponent::internalNodes::root_rec_size);
  recordid rid = {p->id, diskTreeComponent::internalNodes::DEPTH, diskTreeComponent::internalNodes::root_rec_size};
  const indexnode_rec* nr = (const indexnode_rec*)stasis_record_read_begin(xid,p, rid);

  int64_t depth = nr->ptr;
  DEBUG("DEPTH = %lld\n", depth);
  stasis_record_read_done(xid,p,rid,(const byte*)nr);

  pageid_t leafid = diskTreeComponent::internalNodes::findFirstLeaf(xid, p, depth);
  if(leafid != root.page) {

    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid,leafid);
    readlock(p->rwlatch,0);
    assert(depth != 0);
  } else {
    assert(depth == 0);
  }

  {
    // Position just before the first slot.
    // The first call to next() will increment us to the first slot, or return NULL.
    recordid rid = { p->id, diskTreeComponent::internalNodes::FIRST_SLOT-1, 0};
    current = rid;
  }

  DEBUG("keysize = %d, slot = %d\n", keySize, current.slot);
  xid_ = xid;
  done = false;
  t = 0;
  justOnePage = (depth == 0);
}

diskTreeComponent::internalNodes::iterator::iterator(int xid, recordid root, const byte* key, len_t keylen) {
  if(root.page == NULLRID.page && root.slot == NULLRID.slot) abort();

  p = loadPage(xid,root.page);
  readlock(p->rwlatch,0);
  recordid rid = {p->id, diskTreeComponent::internalNodes::DEPTH, diskTreeComponent::internalNodes::root_rec_size};

  const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid,p,rid);
  int64_t depth = nr->ptr;
  stasis_record_read_done(xid,p,rid,(const byte*)nr);

  recordid lsm_entry_rid = diskTreeComponent::internalNodes::lookup(xid,p,depth,key,keylen);

  if(lsm_entry_rid.page == NULLRID.page && lsm_entry_rid.slot == NULLRID.slot) {
    unlock(p->rwlatch);
    releasePage(p);
    p = NULL;
    done = true;
  } else {
    assert(lsm_entry_rid.size != INVALID_SLOT);

    if(root.page != lsm_entry_rid.page)
    {
      unlock(p->rwlatch);
      releasePage(p);
      p = loadPage(xid,lsm_entry_rid.page);
      readlock(p->rwlatch,0);
    }

    done = false;
    current.page = lsm_entry_rid.page;
    current.slot = lsm_entry_rid.slot-1;  // this is current rid, which is one less than the first thing next will return (so subtract 1)
    current.size = lsm_entry_rid.size;

    xid_ = xid;
    t = 0; // must be zero so free() doesn't croak.
    justOnePage = (depth==0);

    DEBUG("diskTreeComponentIterator: index root %lld index page %lld data page %lld key %s\n", root.page, current.page, rec->ptr, key);
    DEBUG("entry = %s key = %s\n", (char*)(rec+1), (char*)key);
  }
}

/**
 * move to the next page
 **/
int diskTreeComponent::internalNodes::iterator::next()
{
  if(done) return 0;

  current = stasis_record_next(xid_, p, current);

  if(current.size == INVALID_SLOT) {

    recordid next_leaf_rid = {p->id, diskTreeComponent::internalNodes::NEXT_LEAF,0};
    const indexnode_rec *nr = (const indexnode_rec*)stasis_record_read_begin(xid_, p, next_leaf_rid);
    pageid_t next_rec = nr->ptr;
    stasis_record_read_done(xid_,p,next_leaf_rid,(const byte*)nr);

    unlock(p->rwlatch);
    releasePage(p);

    DEBUG("done with page %lld next = %lld\n", p->id, next_rec.ptr);

    if(next_rec != -1 && ! justOnePage) {
      p = loadPage(xid_, next_rec);
      readlock(p->rwlatch,0);
      current.page = next_rec;
      current.slot = 2;
      current.size = stasis_record_length_read(xid_, p, current);
    } else {
      p = 0;
      current.size = INVALID_SLOT;
    }

  }

  if(current.size != INVALID_SLOT) {
    if(t != NULL) { free(t); t = NULL; }

    t = (indexnode_rec*)malloc(current.size);
    const byte * buf = stasis_record_read_begin(xid_, p, current);
    memcpy(t, buf, current.size);
    stasis_record_read_done(xid_, p, current, buf);

    return 1;
  } else {
    assert(!p);
    if(t != NULL) { free(t); t = NULL; }
    t = 0;
    return 0;
  }
}

void diskTreeComponent::internalNodes::iterator::close() {

  if(p) {
    unlock(p->rwlatch);
    releasePage(p);
  }
  if(t) free(t);
}


/////////////////////////////////////////////////////////////////////
// tree iterator implementation
/////////////////////////////////////////////////////////////////////

void diskTreeComponent::iterator::init_iterators(datatuple * key1, datatuple * key2) {
    assert(!key2); // unimplemented
    if(tree_.size == INVALID_SIZE) {
        lsmIterator_ = NULL;
    } else {
        if(key1) {
            lsmIterator_ = new diskTreeComponent::internalNodes::iterator(-1, tree_, key1->key(), key1->keylen());
        } else {
            lsmIterator_ = new diskTreeComponent::internalNodes::iterator(-1, tree_);
        }
    }
  }

diskTreeComponent::iterator::iterator(diskTreeComponent::internalNodes *tree) :
    tree_(tree ? tree->get_root_rec() : NULLRID)
{
    init_iterators(NULL, NULL);
    init_helper(NULL);
}

diskTreeComponent::iterator::iterator(diskTreeComponent::internalNodes *tree, datatuple* key) :
    tree_(tree ? tree->get_root_rec() : NULLRID)
{
    init_iterators(key,NULL);
    init_helper(key);

}

diskTreeComponent::iterator::~iterator()
{
    if(lsmIterator_) {
        lsmIterator_->close();
        delete lsmIterator_;
    }

    if(curr_page!=NULL)
    {
        delete curr_page;
        curr_page = 0;
    }


}

void diskTreeComponent::iterator::init_helper(datatuple* key1)
{
    if(!lsmIterator_)
    {
        DEBUG("treeIterator:\t__error__ init_helper():\tnull lsmIterator_");
        curr_page = 0;
        dp_itr = 0;
    }
    else
    {
        if(lsmIterator_->next() == 0)
        {
            DEBUG("diskTreeIterator:\t__error__ init_helper():\tlogtreeIteratr::next returned 0." );
            curr_page = 0;
            dp_itr = 0;
        }
        else
        {
            pageid_t * pid_tmp;
            pageid_t ** hack = &pid_tmp;
            lsmIterator_->value((byte**)hack);

            curr_pageid = *pid_tmp;
            curr_page = new DataPage<datatuple>(-1, curr_pageid);

            DEBUG("opening datapage iterator %lld at key %s\n.", curr_pageid, key1 ? (char*)key1->key() : "NULL");
            dp_itr = new DPITR_T(curr_page, key1);
        }

    }
}

datatuple * diskTreeComponent::iterator::next_callerFrees()
{
    if(!this->lsmIterator_) { return NULL; }

    if(dp_itr == 0)
        return 0;

    datatuple* readTuple = dp_itr->getnext();


    if(!readTuple)
    {
        delete dp_itr;
        dp_itr = 0;
        delete curr_page;
        curr_page = 0;

        if(lsmIterator_->next())
        {
            pageid_t *pid_tmp;

            pageid_t **hack = &pid_tmp;
            size_t ret = lsmIterator_->value((byte**)hack);
            assert(ret == sizeof(pageid_t));
            curr_pageid = *pid_tmp;
            curr_page = new DataPage<datatuple>(-1, curr_pageid);
            DEBUG("opening datapage iterator %lld at beginning\n.", curr_pageid);
            dp_itr = new DPITR_T(curr_page->begin());


            readTuple = dp_itr->getnext();
            assert(readTuple);
        }
      // else readTuple is null.  We're done.
    }
    return readTuple;
}

