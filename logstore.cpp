#include <string.h>
#include <assert.h>
#include <math.h>
#include <ctype.h>

#include "merger.h"
#include "logstore.h"
#include "logiterators.h"
#include "datapage.cpp"

#include <stasis/page.h>
#include <stasis/page/slotted.h>

/////////////////////////////////////////////////////////////////
// LOGTREE implementation
/////////////////////////////////////////////////////////////////

const RegionAllocConf_t logtree::REGION_ALLOC_STATIC_INITIALIZER = { {0,0,-1}, 0, -1, -1, 1000 };
const RegionAllocConf_t
logtable::DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER = { {0,0,-1}, 0, -1, -1, 1000 };

//printf(__VA_ARGS__); fflush(NULL)

#define LOGTREE_ROOT_PAGE SLOTTED_PAGE

//LSM_ROOT_PAGE 

const int64_t logtree::DEPTH = 0;      //in root this is the slot num where the DEPTH (of tree) is stored
const int64_t logtree::COMPARATOR = 1; //in root this is the slot num where the COMPARATOR id is stored
const int64_t logtree::FIRST_SLOT = 2; //this is the first unused slot in all index pages
const size_t logtree::root_rec_size = sizeof(int64_t);
const int64_t logtree::PREV_LEAF = 0; //pointer to prev leaf page
const int64_t logtree::NEXT_LEAF = 1; //pointer to next leaf page



logtree::logtree()
{

}

void logtree::free_region_rid(int xid, recordid tree,
          logtree_page_deallocator_t dealloc, void *allocator_state)
{
  //  Tdealloc(xid,tree);
  dealloc(xid,allocator_state);
  // XXX fishy shouldn't caller do this?
  Tdealloc(xid, *(recordid*)allocator_state);
}


void logtree::dealloc_region_rid(int xid, void *conf)
{
    recordid rid = *(recordid*)conf;
    RegionAllocConf_t a;
    Tread(xid,rid,&a);
    DEBUG("{%lld <- dealloc region arraylist}\n", a.regionList.page);

    for(int i = 0; i < a.regionCount; i++) {
     a.regionList.slot = i;
     pageid_t pid;
     Tread(xid,a.regionList,&pid);
     TregionDealloc(xid,pid);
    }
}


void logtree::force_region_rid(int xid, void *conf)
{
    recordid rid = *(recordid*)conf;
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


pageid_t logtree::alloc_region(int xid, void *conf)
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
  // Ensure the page is in buffer cache without accessing disk (this
  // sets it to clean and all zeros if the page is not in cache).
  // Hopefully, future reads will get a cache hit, and avoid going to
  // disk.

  Page * p = loadUninitializedPage(xid, ret);
  releasePage(p);
  DEBUG("ret %lld\n",ret);
  (a->nextPage)++;
  return ret;

}

pageid_t logtree::alloc_region_rid(int xid, void * ridp) {
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



recordid logtree::create(int xid)
{

    tree_state = Talloc(xid,sizeof(RegionAllocConf_t));

    //int ptype = TpageGetType(xid, tree_state.page);
    //DEBUG("page type %d\n", ptype); //returns a slotted page
    
    Tset(xid,tree_state, &REGION_ALLOC_STATIC_INITIALIZER);

    pageid_t root = alloc_region_rid(xid, &tree_state); 
    DEBUG("Root = %lld\n", root);
    recordid ret = { root, 0, 0 };
    
    Page *p = loadPage(xid, ret.page);
    writelock(p->rwlatch,0);
    
    stasis_page_slotted_initialize_page(p);
    
    //*stasis_page_type_ptr(p) = SLOTTED_PAGE; //LOGTREE_ROOT_PAGE;
    
    //logtree_state *state = (logtree_state*) ( malloc(sizeof(logtree_state)));
    //state->lastLeaf = -1;
    
    //p->impl = state;
    lastLeaf = -1;

    //initialize root node
    recordid tmp  = stasis_record_alloc_begin(xid, p, root_rec_size);
    stasis_record_alloc_done(xid,p,tmp);
    
    assert(tmp.page == ret.page
           && tmp.slot == DEPTH
           && tmp.size == root_rec_size);

    writeRecord(xid, p, tmp, (byte*)&DEPTH, root_rec_size);
    
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


/**
 * TODO: what happen if there is already such a record with a different size?
 * I guess this should never happen in rose, but what if? 
 **/
void logtree::writeRecord(int xid, Page *p, recordid &rid,
                          const byte *data, size_t datalen)
{
    byte *byte_arr = stasis_record_write_begin(xid, p, rid);
    memcpy(byte_arr, data, datalen); //TODO: stasis write call
    stasis_record_write_done(xid, p, rid, byte_arr);
    stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?   

}

void logtree::writeNodeRecord(int xid, Page * p, recordid & rid, 
                              const byte *key, size_t keylen, pageid_t ptr)
{
    DEBUG("writenoderecord:\tp->id\t%lld\tkey:\t%s\tkeylen: %d\tval_page\t%lld\n",
          p->id, datatuple::key_to_str(key).c_str(), keylen, ptr);
    indexnode_rec *nr = (indexnode_rec*)stasis_record_write_begin(xid, p, rid);
    nr->ptr = ptr;
    memcpy(nr+1, key, keylen);
    stasis_record_write_done(xid, p, rid, (byte*)nr);
    stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?   
}

void logtree::writeRecord(int xid, Page *p, slotid_t slot,
                          const byte *data, size_t datalen)
{
    recordid rid;
    rid.page = p->id;
    rid.slot = slot;
    rid.size = datalen;
    byte *byte_arr = stasis_record_write_begin(xid, p, rid);
    memcpy(byte_arr, data, datalen); //TODO: stasis write call
    stasis_record_write_done(xid, p, rid, byte_arr);
    stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?   

}

const byte* logtree::readRecord(int xid, Page * p, recordid &rid)
{    
    //byte *ret = (byte*)malloc(rid.size);
    //const byte *nr = stasis_record_read_begin(xid,p,rid);
    //memcpy(ret, nr, rid.size);
    //stasis_record_read_done(xid,p,rid,nr);

    const byte *nr = stasis_record_read_begin(xid,p,rid);
    return nr;

    //DEBUG("reading {%lld, %d, %d}\n",
    //      p->id, rid.slot, rid.size );

    //return ret;
}

const byte* logtree::readRecord(int xid, Page * p, slotid_t slot, int64_t size)
{
    recordid rid;
    rid.page = p->id;
    rid.slot = slot;
    rid.size = size;
    //byte *ret = (byte*)malloc(rid.size);
    //stasis_record_read(xid,p,rid,ret);
    //return ret;
    const byte *nr = stasis_record_read_begin(xid,p,rid);
    return nr;
//    return readRecord(xid, p, rid);

}

int32_t logtree::readRecordLength(int xid, Page *p, slotid_t slot)
{
    recordid rec = {p->id, slot, 0};
    int32_t reclen = stasis_record_length_read(xid, p, rec);
    return reclen;
}

void logtree::initializeNodePage(int xid, Page *p)
{
    stasis_page_slotted_initialize_page(p);            
    recordid reserved1 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
    stasis_record_alloc_done(xid, p, reserved1);
    recordid reserved2 = stasis_record_alloc_begin(xid, p, sizeof(indexnode_rec));
    stasis_record_alloc_done(xid, p, reserved2);
}


recordid logtree::appendPage(int xid, recordid tree, pageid_t & rmLeafID,
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
	  printf("slots %d (%d) keysize=%lld\n", (int)*stasis_page_slotted_numslots_ptr(p), (int)FIRST_SLOT+1, (long long int)keySize);
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
	    printf("slots %d (%d)\n", *stasis_page_slotted_numslots_ptr(p), (int)FIRST_SLOT+1);
	    assert(*stasis_page_slotted_numslots_ptr(p) == FIRST_SLOT+1);
	  }
      
          indexnode_rec *nr
              = (indexnode_rec*)stasis_record_write_begin(xid, p, pFirstSlot);

          // don't overwrite key...
          nr->ptr = child;
          stasis_record_write_done(xid,p,pFirstSlot,(byte*)nr);
          stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?
          
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

      logtree::writeNodeRecord(xid, lastLeaf, ret, key, keySize, val_page);

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

recordid logtree::appendInternalNode(int xid, Page *p,
                                     int64_t depth,
                                     const byte *key, size_t key_len,
                                     pageid_t val_page, pageid_t lastLeaf,
                                     logtree_page_allocator_t allocator,
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

recordid logtree::buildPathToLeaf(int xid, recordid root, Page *root_p,
                                  int64_t depth, const byte *key, size_t key_len,
                                  pageid_t val_page, pageid_t lastLeaf,
                                  logtree_page_allocator_t allocator,
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
pageid_t logtree::findLastLeaf(int xid, Page *root, int64_t depth)
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
pageid_t logtree::findFirstLeaf(int xid, Page *root, int64_t depth)
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


pageid_t logtree::findPage(int xid, recordid tree, const byte *key, size_t keySize)
{
  Page *p = loadPage(xid, tree.page);
  readlock(p->rwlatch,0);

  const indexnode_rec *depth_nr = (const indexnode_rec*)readRecord(xid, p , DEPTH, 0);  
  
  int64_t depth = *((int64_t*)depth_nr);  

  recordid rid = lookup(xid, p, depth, key, keySize);
  pageid_t ret = lookupLeafPageFromRid(xid,rid);//,keySize);
  unlock(p->rwlatch);
  releasePage(p);

  return ret;

}

pageid_t logtree::lookupLeafPageFromRid(int xid, recordid rid)
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


recordid logtree::lookup(int xid,
                            Page *node,
                            int64_t depth,
                            const byte *key, size_t keySize )
{
    //DEBUG("lookup: pid %lld\t depth %lld\n", node->id, depth);
    if(*stasis_page_slotted_numslots_ptr(node) == FIRST_SLOT) 
        return NULLRID;

    assert(*stasis_page_slotted_numslots_ptr(node) > FIRST_SLOT);
    
    int match = FIRST_SLOT;
    
    // don't need to compare w/ first item in tree.    
    const indexnode_rec * rec = (indexnode_rec*)readRecord(xid,node,FIRST_SLOT,0); //TODO: why read it then?
    
    for(int i = FIRST_SLOT+1; i < *stasis_page_slotted_numslots_ptr(node); i++)
    {
        rec = (const indexnode_rec*)readRecord(xid,node,i,0);
        int cmpval = datatuple::compare((datatuple::key_t) (rec+1),(datatuple::key_t) key);
        if(cmpval>0) //changed it from >
            break;        
        match = i;
    }
    
    
    if(depth)
    {
        pageid_t child_id = ((const indexnode_rec*)readRecord(xid,node,match,0))->ptr;
        Page* child_page = loadPage(xid, child_id);
        readlock(child_page->rwlatch,0);
        recordid ret = lookup(xid,child_page,depth-1,key,0);
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


void logtree::print_tree(int xid)
{
    Page *p = loadPage(xid, root_rec.page);
    readlock(p->rwlatch,0);
    
    const indexnode_rec *depth_nr = (const indexnode_rec*)readRecord(xid, p , DEPTH, 0);
    
    int64_t depth = *((int64_t*)depth_nr);
    
    print_tree(xid, root_rec.page, depth);

    unlock(p->rwlatch);
    releasePage(p);

}

void logtree::print_tree(int xid, pageid_t pid, int64_t depth)
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

/////////////////////////////////////////////////////////////////
// LOG TABLE IMPLEMENTATION
/////////////////////////////////////////////////////////////////

template class DataPage<datatuple>;


logtable::logtable()
{

    tree_c0 = NULL;
    tree_c1 = NULL;
    tree_c2 = NULL;
//    rbtree_mut = NULL;
    this->mergedata = 0;
    fixed_page_count = -1;
    //tmerger = new tuplemerger(&append_merger);
    tmerger = new tuplemerger(&replace_merger);

    tsize = 0;
    tree_bytes = 0;
        
    
}

void logtable::tearDownTree(rbtree_ptr_t tree) {
    datatuple * t = 0;
    for(rbtree_t::iterator delitr  = tree->begin();
                           delitr != tree->end();
                           delitr++) {
    	if(t) {
    		datatuple::freetuple(t);
    	}
    	t = *delitr;
    	tree->erase(delitr);
    }
	if(t) { datatuple::freetuple(t); }
    delete tree;
}

logtable::~logtable()
{
    if(tree_c1 != NULL)        
        delete tree_c1;
    if(tree_c2 != NULL)
        delete tree_c2;

    if(tree_c0 != NULL)
    {
    	tearDownTree(tree_c0);
    }

    delete tmerger;
}

recordid logtable::allocTable(int xid)
{

    table_rec = Talloc(xid, sizeof(tbl_header));
    
    //create the big tree
    tree_c2 = new logtree();
    tree_c2->create(xid);

    tbl_header.c2_dp_state = Talloc(xid, sizeof(RegionAllocConf_t));
    Tset(xid, tbl_header.c2_dp_state, &DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER);    
                         

    //create the small tree
    tree_c1 = new logtree();
    tree_c1->create(xid);
    tbl_header.c1_dp_state = Talloc(xid, sizeof(RegionAllocConf_t));
    Tset(xid, tbl_header.c1_dp_state, &DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER);
    
    tbl_header.c2_root = tree_c2->get_root_rec();
    tbl_header.c2_state = tree_c2->get_tree_state();
    tbl_header.c1_root = tree_c1->get_root_rec();
    tbl_header.c1_state = tree_c1->get_tree_state();
    
    Tset(xid, table_rec, &tbl_header);    
    
    return table_rec;
}

void logtable::flushTable()
{
    struct timeval start_tv, stop_tv;
    double start, stop;
    
    static double last_start;
    static bool first = 1;
    static int merge_count = 0;
    
    gettimeofday(&start_tv,0);
    start = tv_to_double(start_tv);

    
    writelock(mergedata->header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);
    
    int expmcount = merge_count;


    //this is for waiting the previous merger of the mem-tree
    //hopefullly this wont happen
    printf("prv merge not complete\n");


    while(*mergedata->old_c0) {
        unlock(mergedata->header_lock);
//        pthread_mutex_lock(mergedata->rbtree_mut);
        if(tree_bytes >= max_c0_size)
            pthread_cond_wait(mergedata->input_needed_cond, mergedata->rbtree_mut);
        else
        {
            pthread_mutex_unlock(mergedata->rbtree_mut);
            return;
        }

        
        pthread_mutex_unlock(mergedata->rbtree_mut);
        
        writelock(mergedata->header_lock,0);
        pthread_mutex_lock(mergedata->rbtree_mut);
        
        if(expmcount != merge_count)
        {
            unlock(mergedata->header_lock);
            pthread_mutex_unlock(mergedata->rbtree_mut);
            return;                    
        }
        
    }

    printf("prv merge complete\n");

    gettimeofday(&stop_tv,0);
    stop = tv_to_double(stop_tv);
    
    //rbtree_ptr *tmp_ptr = new rbtree_ptr_t; //(typeof(h->scratch_tree)*) malloc(sizeof(void*));
    //*tmp_ptr = tree_c0;
    *(mergedata->old_c0) = tree_c0; 

//    pthread_mutex_lock(mergedata->rbtree_mut);
    pthread_cond_signal(mergedata->input_ready_cond);
//    pthread_mutex_unlock(mergedata->rbtree_mut);

    merge_count ++;
    tree_c0 = new rbtree_t;    
    tsize = 0;
    tree_bytes = 0;
    
    pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(mergedata->header_lock);
    if(first)
    {
        printf("flush waited %f sec\n", stop-start);
        first = 0;
    }
    else
    {
        printf("flush waited %f sec (worked %f)\n",
               stop-start, start-last_start);
    }
    last_start = stop;

}

datatuple * logtable::findTuple(int xid, const datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
	datatuple *search_tuple = datatuple::create(key, keySize);

    readlock(mergedata->header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 

    //step 1: look in tree_c0
    rbtree_t::iterator rbitr = tree_c0->find(search_tuple);
    if(rbitr != tree_c0->end())
    {
        DEBUG("tree_c0 size %d\n", tree_c0->size());
        ret_tuple = (*rbitr)->create_copy();
    }

    bool done = false;
    //step: 2 look into first in tree if exists (a first level merge going on)
    if(*(mergedata->old_c0) != 0)
    {
        DEBUG("old mem tree not null %d\n", (*(mergedata->old_c0))->size());
        rbitr = (*(mergedata->old_c0))->find(search_tuple);
        if(rbitr != (*(mergedata->old_c0))->end())
        {
            datatuple *tuple = *rbitr;

            if(tuple->isDelete())  //tuple deleted
                done = true;  //return ret_tuple            
            else if(ret_tuple != 0)  //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from current tree
                ret_tuple = mtuple; //set return tuple to merge result
            }
            else //key first found in old mem tree
            {
            	ret_tuple = tuple->create_copy();
            }
            //we cannot free tuple from old-tree 'cos it is not a copy
        }            
    }

    //release the memtree lock
    pthread_mutex_unlock(mergedata->rbtree_mut);
    
    //step 3: check c1    
    if(!done)
    {
        datatuple *tuple_c1 = findTuple(xid, key, keySize, tree_c1);
        if(tuple_c1 != NULL)
        {
            bool use_copy = false;
            if(tuple_c1->isDelete()) //tuple deleted
                done = true;        
            else if(ret_tuple != 0) //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple_c1, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple);  //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }            
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_c1;
                //byte *barr = (byte*)malloc(tuple_c1->byte_length());
                //memcpy(barr, (byte*)tuple_c1->keylen, tuple_c1->byte_length());
                //ret_tuple = datatuple::from_bytes(barr);
            }

            if(!use_copy)
            {
                datatuple::freetuple(tuple_c1); //free tuple from tree c1
            }
        }
    }

    //step 4: check old c1 if exists
    if(!done && *(mergedata->diskmerge_args->in_tree) != 0)
    {
        DEBUG("old c1 tree not null\n");
        datatuple *tuple_oc1 = findTuple(xid, key, keySize,
                                             (logtree*)( *(mergedata->diskmerge_args->in_tree)));
        
        if(tuple_oc1 != NULL)
        {
            bool use_copy = false;
            if(tuple_oc1->isDelete())
                done = true;        
            else if(ret_tuple != 0) //merge the two
            {
                datatuple *mtuple = tmerger->merge(tuple_oc1, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_oc1;
                //byte *barr = (byte*)malloc(tuple_oc1->byte_length());
                //memcpy(barr, (byte*)tuple_oc1->keylen, tuple_oc1->byte_length());
                //ret_tuple = datatuple::from_bytes(barr);
            }

            if(!use_copy)
            {
            	datatuple::freetuple(tuple_oc1); //free tuple from tree old c1
            }
        }        
    }

    //step 5: check c2
    if(!done)
    {
        DEBUG("Not in old first disk tree\n");        
        datatuple *tuple_c2 = findTuple(xid, key, keySize, tree_c2);

        if(tuple_c2 != NULL)
        {
            bool use_copy = false;
            if(tuple_c2->isDelete())
                done = true;        
            else if(ret_tuple != 0)
            {
                datatuple *mtuple = tmerger->merge(tuple_c2, ret_tuple);  //merge the two
                datatuple::freetuple(ret_tuple); //free tuple from before
                ret_tuple = mtuple; //set return tuple to merge result            
            }
            else //found for the first time
            {
                use_copy = true;
                ret_tuple = tuple_c2;                
            }

            if(!use_copy)
            {
            	datatuple::freetuple(tuple_c2);  //free tuple from tree c2
            }
        }        
    }     

    //pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(mergedata->header_lock);
    datatuple::freetuple(search_tuple);
    return ret_tuple;

}

/*
 * returns the first record found with the matching key
 * (not to be used together with diffs)
 **/
datatuple * logtable::findTuple_first(int xid, datatuple::key_t key, size_t keySize)
{
    //prepare a search tuple
    datatuple * search_tuple = datatuple::create(key, keySize);
        
    pthread_mutex_lock(mergedata->rbtree_mut);

    datatuple *ret_tuple=0; 
    //step 1: look in tree_c0

    rbtree_t::iterator rbitr = tree_c0->find(search_tuple);
    if(rbitr != tree_c0->end())
    {
        DEBUG("tree_c0 size %d\n", tree_c0->size());
        ret_tuple = (*rbitr)->create_copy();
        
    }
    else
    {
        DEBUG("Not in mem tree %d\n", tree_c0->size());
        //step: 2 look into first in tree if exists (a first level merge going on)
        if(*(mergedata->old_c0) != 0)
        {
            DEBUG("old mem tree not null %d\n", (*(mergedata->old_c0))->size());
            rbitr = (*(mergedata->old_c0))->find(search_tuple);
            if(rbitr != (*(mergedata->old_c0))->end())
            {
                ret_tuple = (*rbitr)->create_copy();
            }            
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in old mem tree\n");

            //step 3: check c1
            ret_tuple = findTuple(xid, key, keySize, tree_c1);    
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in first disk tree\n");

            //step 4: check old c1 if exists
            if( *(mergedata->diskmerge_args->in_tree) != 0)
            {
                DEBUG("old c1 tree not null\n");
                ret_tuple = findTuple(xid, key, keySize,
                                      (logtree*)( *(mergedata->diskmerge_args->in_tree)));
            }
                
        }

        if(ret_tuple == 0)
        {
            DEBUG("Not in old first disk tree\n");

            //step 5: check c2
            ret_tuple = findTuple(xid, key, keySize, tree_c2);            
        }        
    }


     

    pthread_mutex_unlock(mergedata->rbtree_mut);
    datatuple::freetuple(search_tuple);
    
    return ret_tuple;

}

void logtable::insertTuple(datatuple *tuple)
{
    //static int count = LATCH_INTERVAL;
    //static int tsize = 0; //number of tuples
    //static int64_t tree_bytes = 0; //number of bytes

    //lock the red-black tree
    readlock(mergedata->header_lock,0);
    pthread_mutex_lock(mergedata->rbtree_mut);
    //find the previous tuple with same key in the memtree if exists
    rbtree_t::iterator rbitr = tree_c0->find(tuple);
    if(rbitr != tree_c0->end())
    {        
        datatuple *pre_t = *rbitr;
        //do the merging
        datatuple *new_t = tmerger->merge(pre_t, tuple);
        tree_c0->erase(pre_t); //remove the previous tuple        

        tree_c0->insert(new_t); //insert the new tuple

        //update the tree size (+ new_t size - pre_t size)
        tree_bytes += (new_t->byte_length() - pre_t->byte_length());

        datatuple::freetuple(pre_t); //free the previous tuple
    }
    else //no tuple with same key exists in mem-tree
    {

    	datatuple *t = tuple->create_copy();

        //insert tuple into the rbtree        
        tree_c0->insert(t);
        tsize++;
        tree_bytes += t->byte_length() + RB_TREE_OVERHEAD;

    }

    //flushing logic
    /*
    bool go = false;
    if(tree_bytes >= MAX_C0_SIZE)
    {
        go = *mergedata->input_needed;
        DEBUG("go %d\n", go);
     }
    */

    if(tree_bytes >= max_c0_size )
    {
        DEBUG("tree size before merge %d tuples %lld bytes.\n", tsize, tree_bytes);
        pthread_mutex_unlock(mergedata->rbtree_mut);
        unlock(mergedata->header_lock);
        flushTable();

        readlock(mergedata->header_lock,0);        
        pthread_mutex_lock(mergedata->rbtree_mut);
        
        //tsize = 0;
        //tree_bytes = 0;

    }
    
    //unlock
    pthread_mutex_unlock(mergedata->rbtree_mut);
    unlock(mergedata->header_lock);


    DEBUG("tree size %d tuples %lld bytes.\n", tsize, tree_bytes);
}


DataPage<datatuple>* logtable::insertTuple(int xid, datatuple *tuple, recordid &dpstate, logtree *ltree)
{

    //create a new data page    
    
    DataPage<datatuple> * dp = 0;

    while(dp==0)
    {
        dp = new DataPage<datatuple>(xid, fixed_page_count,
                                     &DataPage<datatuple>::dp_alloc_region_rid,
                                     &dpstate );

        //insert the record into the data page
        if(!dp->append(xid, tuple))
        {            
            delete dp;
            dp = 0;
        }
    }
    

    RegionAllocConf_t alloc_conf;
    //insert the record key and id of the first page of the datapage to the logtree
    Tread(xid,ltree->get_tree_state(), &alloc_conf);
    logtree::appendPage(xid, ltree->get_root_rec(), ltree->lastLeaf,
                        tuple->key(),
                        tuple->keylen(),
                        ltree->alloc_region,
                        &alloc_conf,
                        dp->get_start_pid()
                        );
    Tset(xid,ltree->get_tree_state(),&alloc_conf);
                        

    //return the datapage
    return dp;
}

datatuple * logtable::findTuple(int xid, datatuple::key_t key, size_t keySize,  logtree *ltree)
{
    datatuple * tup=0;

    //find the datapage
    pageid_t pid = ltree->findPage(xid, ltree->get_root_rec(), (byte*)key, keySize);

    if(pid!=-1)
    {
        DataPage<datatuple> * dp = new DataPage<datatuple>(xid, pid);
        dp->recordRead(xid, key, keySize, &tup);
        delete dp;           
    }
    return tup;
}


/////////////////////////////////////////////////
//logtreeIterator implementation
/////////////////////////////////////////////////

lladdIterator_t* logtreeIterator::open(int xid, recordid root)
{
    if(root.page == 0 && root.slot == 0 && root.size == -1)
        return 0;
    
    Page *p = loadPage(xid,root.page);
    readlock(p->rwlatch,0);
    
    //size_t keySize = getKeySize(xid,p);
    DEBUG("ROOT_REC_SIZE %d\n", logtree::root_rec_size);
    const byte * nr = logtree::readRecord(xid,p,
                                                  logtree::DEPTH,
                                                  logtree::root_rec_size);
    int64_t depth = *((int64_t*)nr);
    DEBUG("DEPTH = %lld\n", depth);
    
    pageid_t leafid = logtree::findFirstLeaf(xid, p, depth);
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

    
    logtreeIterator_s *impl = (logtreeIterator_s*)malloc(sizeof(logtreeIterator_s));
    impl->p = p;    
    {
        recordid rid = { p->id, 1, 0};//keySize }; //TODO: why does this start from 1? 
        impl->current = rid;        
    }
    //DEBUG("keysize = %d, slot = %d\n", keySize, impl->current.slot);
    impl->t = 0;    
    impl->justOnePage = (depth == 0);    

    lladdIterator_t *it = (lladdIterator_t*) malloc(sizeof(lladdIterator_t));
    it->type = -1; // XXX  LSM_TREE_ITERATOR;    
    it->impl = impl;    
    return it;    
}

lladdIterator_t* logtreeIterator::openAt(int xid, recordid root, const byte* key)
{
  if(root.page == NULLRID.page && root.slot == NULLRID.slot)
      return 0;

  Page *p = loadPage(xid,root.page);
  readlock(p->rwlatch,0);
  //size_t keySize = getKeySize(xid,p);
  //assert(keySize);
  const byte *nr = logtree::readRecord(xid,p,logtree::DEPTH, logtree::root_rec_size);
  //const byte *cmp_nr = logtree::readRecord(xid, p , logtree::COMPARATOR, logtree::root_rec_size);

  int64_t depth = *((int64_t*)nr);

  recordid lsm_entry_rid = logtree::lookup(xid,p,depth,key,0);//keySize,comparators[cmp_nr->ptr]);

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
  
  logtreeIterator_s *impl = (logtreeIterator_s*) malloc(sizeof(logtreeIterator_s));
  impl->p = p;

  impl->current.page = lsm_entry_rid.page;
  impl->current.slot = lsm_entry_rid.slot - 1;  // slot before thing of interest
  impl->current.size = lsm_entry_rid.size;

  impl->t = 0; // must be zero so free() doesn't croak.
  impl->justOnePage = (depth==0);  

  lladdIterator_t *it = (lladdIterator_t*) malloc(sizeof(lladdIterator_t));
  it->type = -1; // XXX LSM_TREE_ITERATOR
  it->impl = impl;
  return it;
}

/**
 * move to the next page
 **/
int logtreeIterator::next(int xid, lladdIterator_t *it)
{
    logtreeIterator_s *impl = (logtreeIterator_s*) it->impl;

    impl->current = stasis_record_next(xid, impl->p, impl->current);
  
    if(impl->current.size == INVALID_SLOT)
    {
        
        const indexnode_rec next_rec = *(const indexnode_rec*)logtree::readRecord(xid,impl->p,
                                                                   logtree::NEXT_LEAF,
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
        memcpy(impl->t, logtree::readRecord(xid,impl->p,impl->current), impl->current.size);
        
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

/*
lladdIterator_t *logtreeIterator::copy(int xid, lladdIterator_t* i)
{
    logtreeIterator_s *it = (logtreeIterator_s*) i->impl;
    logtreeIterator_s *mine = (logtreeIterator_s*) malloc(sizeof(logtreeIterator_s));
    
    if(it->p)
    {
        mine->p = loadPage(xid, it->p->id);
        readlock(mine->p->rwlatch,0);
    }
    else 
        mine->p = 0;
  
    memcpy(&mine->current, &it->current,sizeof(recordid));
    
    if(it->t)
    {
        mine->t = (datatuple*)malloc(sizeof(*it->t)); //TODO: DATA IS NOT COPIED, MIGHT BE WRONG
        //mine->t = malloc(sizeof(*it->t) + it->current.size);
        memcpy(mine->t, it->t, sizeof(*it->t));// + it->current.size);
    }
    else 
        mine->t = 0;
    
    mine->justOnePage = it->justOnePage;
    lladdIterator_t * ret = (lladdIterator_t*)malloc(sizeof(lladdIterator_t));
    ret->type = -1; // XXX LSM_TREE_ITERATOR
    ret->impl = mine;
    return ret;
}
*/

void logtreeIterator::close(int xid, lladdIterator_t *it)
{
    logtreeIterator_s *impl = (logtreeIterator_s*)it->impl;
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


/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////




double tv_to_double(struct timeval tv)
{
  return static_cast<double>(tv.tv_sec) +
      (static_cast<double>(tv.tv_usec) / 1000000.0);
}


///////////////////////////////////////////////////////////////////                       

