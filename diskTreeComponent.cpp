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

#include <stasis/transactional.h>
#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>

/////////////////////////////////////////////////////////////////
// LOGTREE implementation
/////////////////////////////////////////////////////////////////

const RegionAllocConf_t diskTreeComponent::REGION_ALLOC_STATIC_INITIALIZER = { {0,0,-1}, 0, -1, -1, 1000 };

#define LOGTREE_ROOT_PAGE SLOTTED_PAGE

//LSM_ROOT_PAGE

const int64_t diskTreeComponent::DEPTH = 0;      //in root this is the slot num where the DEPTH (of tree) is stored
const int64_t diskTreeComponent::COMPARATOR = 1; //in root this is the slot num where the COMPARATOR id is stored
const int64_t diskTreeComponent::FIRST_SLOT = 2; //this is the first unused slot in all index pages
const size_t diskTreeComponent::root_rec_size = sizeof(int64_t);
const int64_t diskTreeComponent::PREV_LEAF = 0; //pointer to prev leaf page
const int64_t diskTreeComponent::NEXT_LEAF = 1; //pointer to next leaf page

// XXX hack, and cut and pasted from datapage.cpp.
static lsn_t get_lsn(int xid) {
	lsn_t xid_lsn = stasis_transaction_table_get((stasis_transaction_table_t*)stasis_runtime_transaction_table(), xid)->prevLSN;
	lsn_t log_lsn = ((stasis_log_t*)stasis_log())->next_available_lsn((stasis_log_t*)stasis_log());
	lsn_t ret = xid_lsn == INVALID_LSN ? log_lsn-1 : xid_lsn;
	assert(ret != INVALID_LSN);
	return ret;
}


void diskTreeComponent::init_stasis() {

    bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_PFILE;

    DataPage<datatuple>::register_stasis_page_impl();

    stasis_buffer_manager_factory = stasis_buffer_manager_hash_factory; // XXX workaround stasis issue #22.

    Tinit();

}

void diskTreeComponent::deinit_stasis() { Tdeinit(); }

void diskTreeComponent::free_region_rid(int xid, recordid tree,
          diskTreeComponent_page_deallocator_t dealloc, void *allocator_state)
{
  dealloc(xid,allocator_state);
  // XXX fishy shouldn't caller do this?
  Tdealloc(xid, *(recordid*)allocator_state);
}


void diskTreeComponent::dealloc_region_rid(int xid, recordid rid)
{
    RegionAllocConf_t a;
    Tread(xid,rid,&a);
    DEBUG("{%lld <- dealloc region arraylist}\n", a.regionList.page);

    for(int i = 0; i < a.regionCount; i++) {
     a.regionList.slot = i;
     pageid_t pid;
     Tread(xid,a.regionList,&pid);
     TregionDealloc(xid,pid);
    }
    a.regionList.slot = 0;
    TarrayListDealloc(xid, a.regionList);
}


void diskTreeComponent::force_region_rid(int xid, recordid rid)
{
    RegionAllocConf_t a;
    Tread(xid,rid,&a);

    for(int i = 0; i < a.regionCount; i++)
    {
        a.regionList.slot = i;
        pageid_t pid;
        Tread(xid,a.regionList,&pid);
        stasis_dirty_page_table_flush_range((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), pid, pid+a.regionSize);
        stasis_buffer_manager_t *bm =
               (stasis_buffer_manager_t*)stasis_runtime_buffer_manager();
        bm->forcePageRange(bm, pid, pid+a.regionSize);
    }
}


pageid_t diskTreeComponent::alloc_region(int xid, void *conf)
{
    RegionAllocConf_t* a = (RegionAllocConf_t*)conf;


  if(a->nextPage == a->endOfRegion) {
    if(a->regionList.size == -1) {
        //DEBUG("nextPage: %lld\n", a->nextPage);
        a->regionList = TarrayListAlloc(xid, 1, 4, sizeof(pageid_t));
        DEBUG("regionList.page: %lld\n", a->regionList.page);
        DEBUG("regionList.slot: %d\n", a->regionList.slot);
        DEBUG("regionList.size: %lld\n", a->regionList.size);

        a->regionCount = 0;
    }
    DEBUG("{%lld <- alloc region arraylist}\n", a->regionList.page);
    TarrayListExtend(xid,a->regionList,1);
    a->regionList.slot = a->regionCount;
    DEBUG("region lst slot %d\n",a->regionList.slot);
    a->regionCount++;
    DEBUG("region count %lld\n",a->regionCount);
    a->nextPage = TregionAlloc(xid, a->regionSize,12);
    DEBUG("next page %lld\n",a->nextPage);
    a->endOfRegion = a->nextPage + a->regionSize;
    Tset(xid,a->regionList,&a->nextPage);
    DEBUG("next page %lld\n",a->nextPage);
  }

  DEBUG("%lld ?= %lld\n", a->nextPage,a->endOfRegion);
  pageid_t ret = a->nextPage;
  (a->nextPage)++;
  DEBUG("tree %lld-%lld\n", (long long)ret, a->endOfRegion);
  return ret;

}

pageid_t diskTreeComponent::alloc_region_rid(int xid, void * ridp) {
  recordid rid = *(recordid*)ridp;
  RegionAllocConf_t conf;
  Tread(xid,rid,&conf);
  pageid_t ret = alloc_region(xid,&conf);
  //DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);
  // XXX get rid of Tset by storing next page in memory, and losing it
  //     on crash.
  Tset(xid,rid,&conf);
  return ret;
}

pageid_t * diskTreeComponent::list_region_rid(int xid, void *ridp, pageid_t * region_len, pageid_t * region_count) {
	recordid header = *(recordid*)ridp;
	RegionAllocConf_t conf;
	Tread(xid,header,&conf);
	recordid header_list = conf.regionList;
	*region_len = conf.regionSize;
	*region_count = conf.regionCount;
	pageid_t * ret = (pageid_t*) malloc(sizeof(pageid_t) * *region_count);
	for(pageid_t i = 0; i < *region_count; i++) {
		header_list.slot = i;
		Tread(xid,header_list,&ret[i]);
	}
	return ret;
}



recordid diskTreeComponent::create(int xid)
{

    tree_state = Talloc(xid,sizeof(RegionAllocConf_t));

    Tset(xid,tree_state, &REGION_ALLOC_STATIC_INITIALIZER);

    pageid_t root = alloc_region_rid(xid, &tree_state);
    DEBUG("Root = %lld\n", root);
    recordid ret = { root, 0, 0 };

    Page *p = loadPage(xid, ret.page);
    writelock(p->rwlatch,0);

    lastLeaf = -1;

    //initialize root node
    stasis_page_slotted_initialize_page(p);
    recordid tmp  = stasis_record_alloc_begin(xid, p, root_rec_size);
    stasis_record_alloc_done(xid,p,tmp);

    assert(tmp.page == ret.page
           && tmp.slot == DEPTH
           && tmp.size == root_rec_size);

    int64_t zero = 0;
    writeRecord(xid, p, tmp, (byte*)&zero, root_rec_size);

    tmp = stasis_record_alloc_begin(xid, p, root_rec_size);
    stasis_record_alloc_done(xid,p,tmp);

    assert(tmp.page == ret.page
           && tmp.slot == COMPARATOR
           && tmp.size == root_rec_size);

    writeRecord(xid, p, tmp, (byte*)&COMPARATOR, root_rec_size);

    unlock(p->rwlatch);
    releasePage(p);

    root_rec = ret;

    return ret;
}

//  XXX remove the next N records, which are completely redundant.

/**
 * TODO: what happen if there is already such a record with a different size?
 * I guess this should never happen in rose, but what if?
 **/
void diskTreeComponent::writeRecord(int xid, Page *p, recordid &rid,
                          const byte *data, size_t datalen)
{
    byte *byte_arr = stasis_record_write_begin(xid, p, rid);
    memcpy(byte_arr, data, datalen); //TODO: stasis write call
    stasis_record_write_done(xid, p, rid, byte_arr);
    stasis_page_lsn_write(xid, p, get_lsn(xid));

}


void diskTreeComponent::writeNodeRecord(int xid, Page * p, recordid & rid,
                              const byte *key, size_t keylen, pageid_t ptr)
{
    DEBUG("writenoderecord:\tp->id\t%lld\tkey:\t%s\tkeylen: %d\tval_page\t%lld\n",
          p->id, datatuple::key_to_str(key).c_str(), keylen, ptr);
    indexnode_rec *nr = (indexnode_rec*)stasis_record_write_begin(xid, p, rid);
    nr->ptr = ptr;
    memcpy(nr+1, key, keylen);
    stasis_record_write_done(xid, p, rid, (byte*)nr);
    stasis_page_lsn_write(xid, p, get_lsn(xid));
}

void diskTreeComponent::writeRecord(int xid, Page *p, slotid_t slot,
                          const byte *data, size_t datalen)
{
    recordid rid;
    rid.page = p->id;
    rid.slot = slot;
    rid.size = datalen;
    byte *byte_arr = stasis_record_write_begin(xid, p, rid);
    memcpy(byte_arr, data, datalen); //TODO: stasis write call
    stasis_record_write_done(xid, p, rid, byte_arr);
    stasis_page_lsn_write(xid, p, get_lsn(xid));

}

const byte* diskTreeComponent::readRecord(int xid, Page * p, recordid &rid)
{
    const byte *nr = stasis_record_read_begin(xid,p,rid); // XXX API violation?
    return nr;
}

const byte* diskTreeComponent::readRecord(int xid, Page * p, slotid_t slot, int64_t size)
{
    recordid rid;
    rid.page = p->id;
    rid.slot = slot;
    rid.size = size;
    const byte *nr = stasis_record_read_begin(xid,p,rid);
    return nr;
}

int32_t diskTreeComponent::readRecordLength(int xid, Page *p, slotid_t slot)
{
    recordid rec = {p->id, slot, 0};
    int32_t reclen = stasis_record_length_read(xid, p, rec);
    return reclen;
}

void diskTreeComponent::initializeNodePage(int xid, Page *p)
{
    stasis_page_slotted_initialize_page(p);
    recordid reserved1 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
    stasis_record_alloc_done(xid, p, reserved1);
    recordid reserved2 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
    stasis_record_alloc_done(xid, p, reserved2);
}


recordid diskTreeComponent::appendPage(int xid, recordid tree, pageid_t & rmLeafID,
                             const byte *key, size_t keySize,
                             lsm_page_allocator_t allocator, void *allocator_state,
                             long val_page)
{
  Page *p = loadPage(xid, tree.page);
  writelock(p->rwlatch, 0);
  //logtree_state *s = (logtree_state*)p->impl;

  tree.slot = 0;
  //tree.size = sizeof(lsmTreeNodeRecord)+keySize;

  const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid, p , DEPTH, 0);
  int64_t depth = *((int64_t*)nr);

  if(rmLeafID == -1) {
    rmLeafID = findLastLeaf(xid, p, depth);
  }

  Page *lastLeaf;

  if(rmLeafID != tree.page)
  {
    lastLeaf= loadPage(xid, rmLeafID);
    writelock(lastLeaf->rwlatch, 0);
  } else
    lastLeaf = p;


  recordid ret = stasis_record_alloc_begin(xid, lastLeaf,
                                           sizeof(indexnode_rec)+keySize);

  if(ret.size == INVALID_SLOT)
  {
      if(lastLeaf->id != p->id)
      {
          assert(rmLeafID != tree.page);
          unlock(lastLeaf->rwlatch);
          releasePage(lastLeaf); // don't need that page anymore...
          lastLeaf = 0;
      }
      // traverse down the root of the tree.

      tree.slot = 0;

      assert(tree.page == p->id);

      ret = appendInternalNode(xid, p, depth, key, keySize, val_page,
                               rmLeafID == tree.page ? -1 : rmLeafID,
                               allocator, allocator_state);

      if(ret.size == INVALID_SLOT)
      {
          DEBUG("Need to split root; depth = %d\n", depth);

          pageid_t child = allocator(xid, allocator_state);
          Page *lc = loadPage(xid, child);
          writelock(lc->rwlatch,0);

          initializeNodePage(xid, lc);

          //creates a copy of the root page records in the
          //newly allocated child page
          for(int i = FIRST_SLOT; i < *stasis_page_slotted_numslots_ptr(p); i++)
          {
              //read the record from the root page
              const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid,p,i,0);
              int reclen = readRecordLength(xid, p, i);

              recordid cnext = stasis_record_alloc_begin(xid, lc,reclen);

              assert(i == cnext.slot);
              assert(cnext.size != INVALID_SLOT);

              stasis_record_alloc_done(xid, lc, cnext);

              writeRecord(xid,lc,i,(byte*)(nr),reclen);
          }

          // deallocate old entries, and update pointer on parent node.
          // NOTE: stasis_record_free call goes to slottedFree in slotted.c
          // this function only reduces the numslots when you call it
          // with the last slot. so thats why i go backwards here.
	  DEBUG("slots %d (%d) keysize=%lld\n", (int)*stasis_page_slotted_numslots_ptr(p), (int)FIRST_SLOT+1, (long long int)keySize);
	  assert(*stasis_page_slotted_numslots_ptr(p) >= FIRST_SLOT+1);
	  for(int i = *stasis_page_slotted_numslots_ptr(p)-1; i>FIRST_SLOT; i--)
          {
	    assert(*stasis_page_slotted_numslots_ptr(p) > FIRST_SLOT+1);
              recordid tmp_rec= {p->id, i, INVALID_SIZE};
              stasis_record_free(xid, p, tmp_rec);
	  }

          //TODO: could change with stasis_slotted_page_initialize(...);
	  // TODO: fsck?
	  //	  stasis_page_slotted_initialize_page(p);

          // reinsert first.
          recordid pFirstSlot = { p->id, FIRST_SLOT, readRecordLength(xid, p, FIRST_SLOT)};
          if(*stasis_page_slotted_numslots_ptr(p) != FIRST_SLOT+1) {
	    DEBUG("slots %d (%d)\n", *stasis_page_slotted_numslots_ptr(p), (int)FIRST_SLOT+1);
	    assert(*stasis_page_slotted_numslots_ptr(p) == FIRST_SLOT+1);
	  }

          indexnode_rec *nr
              = (indexnode_rec*)stasis_record_write_begin(xid, p, pFirstSlot);

          // don't overwrite key...
          nr->ptr = child;
          stasis_record_write_done(xid,p,pFirstSlot,(byte*)nr);
          stasis_page_lsn_write(xid, p, get_lsn(xid));

          if(!depth) {
              rmLeafID = lc->id;
              pageid_t tmpid = -1;
              writeRecord(xid,lc,PREV_LEAF,(byte*)(&tmpid), root_rec_size);
              writeRecord(xid,lc,NEXT_LEAF,(byte*)(&tmpid), root_rec_size);
          }

          unlock(lc->rwlatch);
          releasePage(lc);

          //update the depth info at the root
          depth ++;
          writeRecord(xid,p,DEPTH,(byte*)(&depth), root_rec_size);

          assert(tree.page == p->id);
          ret = appendInternalNode(xid, p, depth, key, keySize, val_page,
                                   rmLeafID == tree.page ? -1 : rmLeafID,
                                   allocator, allocator_state);

          assert(ret.size != INVALID_SLOT);

      }
      else {
          DEBUG("Appended new internal node tree depth = %lld key = %s\n",
                depth, datatuple::key_to_str(key).c_str());
      }

      rmLeafID = ret.page;
      DEBUG("lastleaf is %lld\n", rmLeafID);


  }
  else
  {
    // write the new value to an existing page
      DEBUG("Writing %s\t%d to existing page# %lld\n", datatuple::key_to_str(key).c_str(),
            val_page, lastLeaf->id);

      stasis_record_alloc_done(xid, lastLeaf, ret);

      diskTreeComponent::writeNodeRecord(xid, lastLeaf, ret, key, keySize, val_page);

    if(lastLeaf->id != p->id) {
      assert(rmLeafID != tree.page);
      unlock(lastLeaf->rwlatch);
      releasePage(lastLeaf);
    }
  }

  unlock(p->rwlatch);
  releasePage(p);

  return ret;
}

/* adding pages:

  1) Try to append value to lsmTreeState->lastLeaf

  2) If that fails, traverses down the root of the tree, split pages while
     traversing back up.

  3) Split is done by adding new page at end of row (no key
     redistribution), except at the root, where root contents are
     pushed into the first page of the next row, and a new path from root to
     leaf is created starting with the root's immediate second child.

*/

recordid diskTreeComponent::appendInternalNode(int xid, Page *p,
                                     int64_t depth,
                                     const byte *key, size_t key_len,
                                     pageid_t val_page, pageid_t lastLeaf,
                                     diskTreeComponent_page_allocator_t allocator,
                                     void *allocator_state)
{
//    assert(*stasis_page_type_ptr(p) == LOGTREE_ROOT_PAGE ||
//           *stasis_page_type_ptr(p) == SLOTTED_PAGE);
    assert(p->pageType == LOGTREE_ROOT_PAGE ||
           p->pageType == SLOTTED_PAGE);

  DEBUG("appendInternalNode\tdepth %lldkeylen%d\tnumslots %d\n", depth, key_len, *stasis_page_slotted_numslots_ptr(p));

  if(!depth)
  {
      // leaf node.
      recordid ret = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec)+key_len);
      if(ret.size != INVALID_SLOT) {
          stasis_record_alloc_done(xid, p, ret);
          writeNodeRecord(xid,p,ret,key,key_len,val_page);
      }
      return ret;
  }
  else
  {
    // recurse
      int slot = *stasis_page_slotted_numslots_ptr(p)-1;//*recordcount_ptr(p)-1;

      assert(slot >= FIRST_SLOT); // there should be no empty nodes
      const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid, p, slot, 0);
      pageid_t child_id = nr->ptr;
      nr = 0;
      recordid ret;
      {
          Page *child_page = loadPage(xid, child_id);
          writelock(child_page->rwlatch,0);
          ret = appendInternalNode(xid, child_page, depth-1, key, key_len,
                                   val_page, lastLeaf, allocator, allocator_state);

          unlock(child_page->rwlatch);
          releasePage(child_page);
      }

      if(ret.size == INVALID_SLOT)  // subtree is full; split
      {
          ret = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec)+key_len);
          DEBUG("keylen %d\tnumslots %d for page id %lld ret.size %lld  prv rec len %d\n",
                key_len,
                *stasis_page_slotted_numslots_ptr(p),
                p->id,
                ret.size,
                readRecordLength(xid, p, slot));
          if(ret.size != INVALID_SLOT)
          {
              stasis_record_alloc_done(xid, p, ret);
              ret = buildPathToLeaf(xid, ret, p, depth, key, key_len, val_page,
                                    lastLeaf, allocator, allocator_state);

              DEBUG("split tree rooted at %lld, wrote value to {%d %d %lld}\n",
                    p->id, ret.page, ret.slot, ret.size);
          } else {
              // ret is NULLRID; this is the root of a full tree. Return
              // NULLRID to the caller.
          }
      } else {
          // we inserted the value in to a subtree rooted here.
      }
      return ret;
  }
}

recordid diskTreeComponent::buildPathToLeaf(int xid, recordid root, Page *root_p,
                                  int64_t depth, const byte *key, size_t key_len,
                                  pageid_t val_page, pageid_t lastLeaf,
                                  diskTreeComponent_page_allocator_t allocator,
                                  void *allocator_state)
{

  // root is the recordid on the root page that should point to the
  // new subtree.
  assert(depth);
  DEBUG("buildPathToLeaf(depth=%lld) (lastleaf=%lld) called\n",depth, lastLeaf);

  pageid_t child = allocator(xid,allocator_state);
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
			  val_page,lastLeaf, allocator, allocator_state);

    unlock(child_p->rwlatch);
    releasePage(child_p);

  } else {
    // set leaf

    // backward link.//these writes do not need alloc_begin as it is done in page initialization
      writeRecord(xid, child_p, PREV_LEAF, (byte*)(&lastLeaf), root_rec_size);
    //writeNodeRecord(xid,child_p,PREV_LEAF,dummy,key_len,lastLeaf);

    // forward link (initialize to -1)

      pageid_t tmp_pid = -1;
      writeRecord(xid, child_p, NEXT_LEAF, (byte*)(&tmp_pid), root_rec_size);
      //writeNodeRecord(xid,child_p,NEXT_LEAF,dummy,key_len,-1);

      recordid leaf_rec = stasis_record_alloc_begin(xid, child_p,
                                       sizeof(indexnode_rec)+key_len);

      assert(leaf_rec.slot == FIRST_SLOT);

      stasis_record_alloc_done(xid, child_p, leaf_rec);
      writeNodeRecord(xid,child_p,leaf_rec,key,key_len,val_page);

      ret = leaf_rec;

      unlock(child_p->rwlatch);
      releasePage(child_p);
      if(lastLeaf != -1)
      {
          // install forward link in previous page
          Page *lastLeafP = loadPage(xid, lastLeaf);
          writelock(lastLeafP->rwlatch,0);
          writeRecord(xid,lastLeafP,NEXT_LEAF,(byte*)(&child),root_rec_size);
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
pageid_t diskTreeComponent::findLastLeaf(int xid, Page *root, int64_t depth)
{
  if(!depth)
  {
      DEBUG("Found last leaf = %lld\n", root->id);
      return root->id;
  }
  else
  {
      const indexnode_rec *nr = (indexnode_rec*) readRecord(xid, root,
                                                            (*stasis_page_slotted_numslots_ptr(root))-1, 0);
      pageid_t ret;

      Page *p = loadPage(xid, nr->ptr);
      readlock(p->rwlatch,0);
      ret = findLastLeaf(xid,p,depth-1);
      unlock(p->rwlatch);
      releasePage(p);

      return ret;
  }
}


/**
 *  Traverse from the root of the tree to the left most (lowest valued
 *  key) leaf.
 */
pageid_t diskTreeComponent::findFirstLeaf(int xid, Page *root, int64_t depth)
{
    if(!depth) //if depth is 0, then returns the id of the page
        return root->id;
    else
    {
        const indexnode_rec *nr = (indexnode_rec*)readRecord(xid,root,FIRST_SLOT,0);
        Page *p = loadPage(xid, nr->ptr);
        readlock(p->rwlatch,0);
        pageid_t ret = findFirstLeaf(xid,p,depth-1);
        unlock(p->rwlatch);
        releasePage(p);
        return ret;
    }
}


pageid_t diskTreeComponent::findPage(int xid, recordid tree, const byte *key, size_t keySize)
{
  Page *p = loadPage(xid, tree.page);
  readlock(p->rwlatch,0);

  int64_t depth = *(int64_t*)readRecord(xid, p , DEPTH, 0);

  recordid rid = lookup(xid, p, depth, key, keySize);
  pageid_t ret = lookupLeafPageFromRid(xid,rid);//,keySize);
  unlock(p->rwlatch);
  releasePage(p);

  return ret;

}

pageid_t diskTreeComponent::lookupLeafPageFromRid(int xid, recordid rid)
{
  pageid_t pid = -1;
  if(rid.page != NULLRID.page || rid.slot != NULLRID.slot)
  {
      Page * p2 = loadPage(xid, rid.page);
      readlock(p2->rwlatch,0);
      pid = ((const indexnode_rec*)(readRecord(xid,p2,rid.slot,0)))->ptr;
      unlock(p2->rwlatch);
      releasePage(p2);
  }
  return pid;
}


recordid diskTreeComponent::lookup(int xid,
                            Page *node,
                            int64_t depth,
                            const byte *key, size_t keySize )
{
    //DEBUG("lookup: pid %lld\t depth %lld\n", node->id, depth);
    if(*stasis_page_slotted_numslots_ptr(node) == FIRST_SLOT)
        return NULLRID;

    assert(*stasis_page_slotted_numslots_ptr(node) > FIRST_SLOT);

    // don't need to compare w/ first item in tree, since we need to position ourselves at the the max tree value <= key.
    // positioning at FIRST_SLOT puts us "before" the first value
    int match = FIRST_SLOT; // (so match is now < key)

    for(int i = FIRST_SLOT+1; i < *stasis_page_slotted_numslots_ptr(node); i++)
    {
        const indexnode_rec *rec = (const indexnode_rec*)readRecord(xid,node,i,0);
        int cmpval = datatuple::compare((datatuple::key_t) (rec+1), *stasis_page_slotted_slot_length_ptr(node, i)-sizeof(*rec),
					(datatuple::key_t) key, keySize);
        if(cmpval>0) // key of current node is too big; there can be no matches under it.
            break;
        match = i; // only increment match after comparing with the current node.
    }


    if(depth)
    {
        pageid_t child_id = ((const indexnode_rec*)readRecord(xid,node,match,0))->ptr;
        Page* child_page = loadPage(xid, child_id);
        readlock(child_page->rwlatch,0);
        recordid ret = lookup(xid,child_page,depth-1,key,keySize);
        unlock(child_page->rwlatch);
        releasePage(child_page);
        return ret;
    }
    else
    {
        recordid ret = {node->id, match, keySize};
        return ret;
    }
}


void diskTreeComponent::print_tree(int xid)
{
    Page *p = loadPage(xid, root_rec.page);
    readlock(p->rwlatch,0);

    const indexnode_rec *depth_nr = (const indexnode_rec*)readRecord(xid, p , DEPTH, 0);

    int64_t depth = *((int64_t*)depth_nr);

    print_tree(xid, root_rec.page, depth);

    unlock(p->rwlatch);
    releasePage(p);

}

void diskTreeComponent::print_tree(int xid, pageid_t pid, int64_t depth)
{

    Page *node = loadPage(xid, pid);
    readlock(node->rwlatch,0);

    //const indexnode_rec *depth_nr = (const indexnode_rec*)readRecord(xid, p , DEPTH, 0);

    printf("page_id:%lld\tnum_slots:%d\t\n", node->id, *stasis_page_slotted_numslots_ptr(node));

    if(*stasis_page_slotted_numslots_ptr(node) == FIRST_SLOT)
        return;

    assert(*stasis_page_slotted_numslots_ptr(node) > FIRST_SLOT);

    if(depth)
    {
        printf("\tnot_leaf\n");

        for(int i = FIRST_SLOT; i < *stasis_page_slotted_numslots_ptr(node); i++)
        {
            const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid,node,i,0);
            printf("\tchild_page_id:%lld\tkey:%s\n", nr->ptr,
                   datatuple::key_to_str((byte*)(nr+1)).c_str());

        }

        for(int i = FIRST_SLOT; i < *stasis_page_slotted_numslots_ptr(node); i++)
        {
            const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid,node,i,0);
            print_tree(xid, nr->ptr, depth-1);

        }

    }
    else
    {
        printf("\tis_leaf\t\n");
        const indexnode_rec *nr = (const indexnode_rec*)readRecord(xid,node,FIRST_SLOT,0);
        printf("\tdata_page_id:%lld\tkey:%s\n", nr->ptr,
               datatuple::key_to_str((byte*)(nr+1)).c_str());
        printf("\t...\n");
        nr = (const indexnode_rec*)readRecord(xid,node,(*stasis_page_slotted_numslots_ptr(node))-1,0);
        printf("\tdata_page_id:%lld\tkey:%s\n", nr->ptr,
                           datatuple::key_to_str((byte*)(nr+1)).c_str());


    }


    unlock(node->rwlatch);
    releasePage(node);


}

/////////////////////////////////////////////////
//diskTreeComponentIterator implementation
/////////////////////////////////////////////////

lladdIterator_t* diskTreeComponentIterator::open(int xid, recordid root)
{
    if(root.page == 0 && root.slot == 0 && root.size == -1)
        return 0;

    Page *p = loadPage(xid,root.page);
    readlock(p->rwlatch,0);

    //size_t keySize = getKeySize(xid,p);
    DEBUG("ROOT_REC_SIZE %d\n", diskTreeComponent::root_rec_size);
    const byte * nr = diskTreeComponent::readRecord(xid,p,
                                                  diskTreeComponent::DEPTH,
                                                  diskTreeComponent::root_rec_size);
    int64_t depth = *((int64_t*)nr);
    DEBUG("DEPTH = %lld\n", depth);

    pageid_t leafid = diskTreeComponent::findFirstLeaf(xid, p, depth);
    if(leafid != root.page)
    {
        unlock(p->rwlatch);
        releasePage(p);
        p = loadPage(xid,leafid);
        readlock(p->rwlatch,0);
        assert(depth != 0);
    }
    else
        assert(depth == 0);


    diskTreeComponentIterator_t *impl = (diskTreeComponentIterator_t*)malloc(sizeof(diskTreeComponentIterator_t));
    impl->p = p;
    {
    	// Position just before the first slot.
    	// The first call to next() will increment us to the first slot, or return NULL.
        recordid rid = { p->id, diskTreeComponent::FIRST_SLOT-1, 0};
        impl->current = rid;
    }
    DEBUG("keysize = %d, slot = %d\n", keySize, impl->current.slot);
    impl->t = 0;
    impl->justOnePage = (depth == 0);

    lladdIterator_t *it = (lladdIterator_t*) malloc(sizeof(lladdIterator_t));
    it->type = -1; // XXX  LSM_TREE_ITERATOR;
    it->impl = impl;
    return it;
}

lladdIterator_t* diskTreeComponentIterator::openAt(int xid, recordid root, const byte* key, len_t keylen)
{
  if(root.page == NULLRID.page && root.slot == NULLRID.slot)
      return 0;

  Page *p = loadPage(xid,root.page);
  readlock(p->rwlatch,0);

  const byte *nr = diskTreeComponent::readRecord(xid,p,diskTreeComponent::DEPTH, diskTreeComponent::root_rec_size);

  int64_t depth = *((int64_t*)nr);

  recordid lsm_entry_rid = diskTreeComponent::lookup(xid,p,depth,key,keylen);

  if(lsm_entry_rid.page == NULLRID.page && lsm_entry_rid.slot == NULLRID.slot) {
    unlock(p->rwlatch);
    return 0;
  }
  assert(lsm_entry_rid.size != INVALID_SLOT);

  if(root.page != lsm_entry_rid.page)
  {
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid,lsm_entry_rid.page);
    readlock(p->rwlatch,0);
  }

  diskTreeComponentIterator_t *impl = (diskTreeComponentIterator_t*) malloc(sizeof(diskTreeComponentIterator_t));
  impl->p = p;

  impl->current.page = lsm_entry_rid.page;
  impl->current.slot = lsm_entry_rid.slot-1;  // this is current rid, which is one less than the first thing next will return (so subtract 1)
  impl->current.size = lsm_entry_rid.size;

  impl->t = 0; // must be zero so free() doesn't croak.
  impl->justOnePage = (depth==0);

  DEBUG("diskTreeComponentIterator: index root %lld index page %lld data page %lld key %s\n", root.page, impl->current.page, rec->ptr, key);
  DEBUG("entry = %s key = %s\n", (char*)(rec+1), (char*)key);

  lladdIterator_t *it = (lladdIterator_t*) malloc(sizeof(lladdIterator_t));
  it->type = -1; // XXX LSM_TREE_ITERATOR
  it->impl = impl;
  return it;
}

/**
 * move to the next page
 **/
int diskTreeComponentIterator::next(int xid, lladdIterator_t *it)
{
    diskTreeComponentIterator_t *impl = (diskTreeComponentIterator_t*) it->impl;

    impl->current = stasis_record_next(xid, impl->p, impl->current);

    if(impl->current.size == INVALID_SLOT)
    {

        const indexnode_rec next_rec = *(const indexnode_rec*)diskTreeComponent::readRecord(xid,impl->p,
                                                                   diskTreeComponent::NEXT_LEAF,
                                                                   0);
        unlock(impl->p->rwlatch);
        releasePage(impl->p);

        DEBUG("done with page %lld next = %lld\n", impl->p->id, next_rec.ptr);


        if(next_rec.ptr != -1 && ! impl->justOnePage)
        {
            impl->p = loadPage(xid, next_rec.ptr);
            readlock(impl->p->rwlatch,0);
            impl->current.page = next_rec.ptr;
            impl->current.slot = 2;
            impl->current.size = stasis_record_length_read(xid, impl->p, impl->current); //keySize;
        } else {
            impl->p = 0;
            impl->current.size = INVALID_SLOT;
        }

    }
    else
    {
        /*
        assert(impl->current.size == keySize + sizeof(lsmTreeNodeRecord));
        impl->current.size = keySize;
        */
    }


    if(impl->current.size != INVALID_SLOT)
    {
        //size_t sz = sizeof(*impl->t) + impl->current.size;
        if(impl->t != NULL)
            free(impl->t);

        impl->t = (indexnode_rec*)malloc(impl->current.size);
        memcpy(impl->t, diskTreeComponent::readRecord(xid,impl->p,impl->current), impl->current.size);

        return 1;
    }
    else
    {
    	assert(!impl->p);
        if(impl->t != NULL)
            free(impl->t);
        impl->t = 0;
        return 0;
    }

}

void diskTreeComponentIterator::close(int xid, lladdIterator_t *it)
{
    diskTreeComponentIterator_t *impl = (diskTreeComponentIterator_t*)it->impl;
  if(impl->p)
  {
    unlock(impl->p->rwlatch);
    releasePage(impl->p);
  }
  if(impl->t)
  {
      free(impl->t);
  }
  free(impl);
  free(it);
}
