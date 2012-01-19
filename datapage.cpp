/*
 * datapage.cpp
 *
 * Copyright 2009-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *      Author: makdere
 */
#include "logstore.h"
#include "datapage.h"
#include "regionAllocator.h"

#include <stasis/page.h>

static const int DATA_PAGE = USER_DEFINED_PAGE(1);
#define MAX_PAGE_COUNT 1000 // ~ 4MB

BEGIN_C_DECLS
static void dataPageFsck(Page* p) {
	int32_t is_last_page = *stasis_page_int32_cptr_from_start(p, 0);
	assert(is_last_page == 0 || is_last_page == 1 || is_last_page == 2);
}
static void dataPageLoaded(Page* p) {
	dataPageFsck(p);
}
static void dataPageFlushed(Page* p) {
	*stasis_page_lsn_ptr(p) = p->LSN;
	dataPageFsck(p);
}
static int notSupported(int xid, Page * p) { return 0; }

END_C_DECLS

void DataPage::register_stasis_page_impl() {
	static page_impl pi =  {
	    DATA_PAGE,
	    1,
	    0, //slottedRead,
	    0, //slottedWrite,
	    0,// readDone
	    0,// writeDone
	    0,//slottedGetType,
	    0,//slottedSetType,
	    0,//slottedGetLength,
	    0,//slottedFirst,
	    0,//slottedNext,
	    0,//slottedLast,
	    notSupported, // is block supported
	    stasis_block_first_default_impl,
	    stasis_block_next_default_impl,
	    stasis_block_done_default_impl,
	    0,//slottedFreespace,
	    0,//slottedCompact,
	    0,//slottedCompactSlotIDs,
	    0,//slottedPreRalloc,
	    0,//slottedPostRalloc,
	    0,//slottedSpliceSlot,
	    0,//slottedFree,
	    0,//XXX page_impl_dereference_identity,
	    dataPageLoaded, //dataPageLoaded,
	    dataPageFlushed, //dataPageFlushed,
	    0,//slottedCleanup
	  };
	stasis_page_impl_register(pi);

}

DataPage::DataPage(int xid, RegionAllocator * alloc, pageid_t pid):  // XXX Hack!! The read-only constructor signature is too close to the other's
  xid_(xid),
  page_count_(1), // will be opportunistically incremented as we scan the datapage.
  initial_page_count_(-1), // used by append.
  alloc_(alloc),  // read-only, and we don't free data pages one at a time.
  first_page_(pid),
  write_offset_(-1)
  {
  assert(pid!=0);
  Page *p = alloc_ ? alloc_->load_page(xid, first_page_) : loadPage(xid, first_page_);
  if(!(*is_another_page_ptr(p) == 0 || *is_another_page_ptr(p) == 2)) {
    printf("Page %lld is not the start of a datapage\n", first_page_);  fflush(stdout);
    abort();
  }
  assert(*is_another_page_ptr(p) == 0 || *is_another_page_ptr(p) == 2); // would be 1 for page in the middle of a datapage
  releasePage(p);
}

DataPage::DataPage(int xid, pageid_t page_count, RegionAllocator *alloc) :
  xid_(xid),
  page_count_(1),
  initial_page_count_(page_count),
  alloc_(alloc),
  first_page_(alloc_->alloc_extent(xid_, page_count_)),
  write_offset_(0)
{
  DEBUG("Datapage page count: %lld pid = %lld\n", (long long int)initial_page_count_, (long long int)first_page_);
  assert(page_count_ >= 1);
  initialize();
}

void DataPage::initialize() {
  initialize_page(first_page_);
}

void DataPage::initialize_page(pageid_t pageid) {
  //load the first page
  Page *p;
#ifdef CHECK_FOR_SCRIBBLING
  p = alloc_ ? alloc->load_page(xid_, pageid) : loadPage(xid_, pageid);
  if(*stasis_page_type_ptr(p) == DATA_PAGE) {
      printf("Collision on page %lld\n", (long long)pageid); fflush(stdout);
      assert(*stasis_page_type_ptr(p) != DATA_PAGE);
  }
#else
  p = loadUninitializedPage(xid_, pageid);
#endif

  DEBUG("\t\t\t\t\t\t->%lld\n", pageid);

  //initialize header
  p->pageType = DATA_PAGE;

  //clear page (arranges for null-padding)  XXX null pad more carefully and use sentinel value instead?
  memset(p->memAddr, 0, PAGE_SIZE);

  //we're the last page for now.
  *is_another_page_ptr(p) = 0;

  //write 0 to first data size
  *length_at_offset_ptr(p, calc_chunk_from_offset(write_offset_).slot) = 0;

  //set the page dirty
  stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));

  releasePage(p);
}

size_t DataPage::write_bytes(const byte * buf, ssize_t remaining, Page ** latch_p) {
  if(latch_p) { *latch_p  = NULL; }
  recordid chunk = calc_chunk_from_offset(write_offset_);
  if(chunk.size > remaining) {
    chunk.size = remaining;
  }
  if(chunk.page >= first_page_ + page_count_) {
    chunk.size = 0; // no space (should not happen)
  } else {
    Page *p = alloc_ ? alloc_->load_page(xid_, chunk.page) : loadPage(xid_, chunk.page);
    assert(chunk.size);
    memcpy(data_at_offset_ptr(p, chunk.slot), buf, chunk.size);
    stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));
    if(latch_p && !*latch_p) {
      writelock(p->rwlatch,0);
      *latch_p = p;
    } else {
      releasePage(p);
    }
    write_offset_ += chunk.size;
  }
  return chunk.size;
}
size_t DataPage::read_bytes(byte * buf, off_t offset, ssize_t remaining) {
  recordid chunk = calc_chunk_from_offset(offset);
  if(chunk.size > remaining) {
    chunk.size = remaining;
  }
  if(chunk.page >= first_page_ + page_count_) {
    chunk.size = 0; // eof
  } else {
    Page *p = alloc_ ? alloc_->load_page(xid_, chunk.page) : loadPage(xid_, chunk.page);
    if(p->pageType != DATA_PAGE) {
      fprintf(stderr, "Page type %d, id %lld lsn %lld\n", (int)p->pageType, (long long)p->id, (long long)p->LSN);
      assert(p->pageType == DATA_PAGE);
    }
    if((chunk.page + 1 == page_count_ + first_page_)
      && (*is_another_page_ptr(p))) {
        page_count_++;
    }
    memcpy(buf, data_at_offset_ptr(p, chunk.slot), chunk.size);
    releasePage(p);
  }
  return chunk.size;
}

bool DataPage::initialize_next_page() {
  recordid rid = calc_chunk_from_offset(write_offset_);
  assert(rid.slot == 0);
  DEBUG("\t\t%lld\n", (long long)rid.page);

  if(rid.page >= first_page_ + page_count_) {
    assert(rid.page == first_page_ + page_count_);
    if(alloc_->grow_extent(1)) {
      page_count_++;
    } else {
      return false; // The region is full
    }
  } else {
    abort();
  }

  Page *p = alloc_ ? alloc_->load_page(xid_, rid.page-1) : loadPage(xid_, rid.page-1);
  *is_another_page_ptr(p) = (rid.page-1 == first_page_) ? 2 : 1;
  stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));
  releasePage(p);

  initialize_page(rid.page);
  return true;
}

Page * DataPage::write_data_and_latch(const byte * buf, size_t len, bool init_next, bool latch) {
  bool first = true;
  Page * p = 0;
  while(1) {
    assert(len > 0);
    size_t written;
    if(latch && first ) {
      written = write_bytes(buf, len, &p);
    } else {
      written = write_bytes(buf, len);
    }
    if(written == 0) {
      assert(!p);
      return 0; // fail
    }
    if(written == len) {
      if(latch) {
        return p;
      } else {
        return (Page*)1;
      }
    }
    if(len > PAGE_SIZE && ! first) {
      assert(written > 4000);
    }
    buf += written;
    len -= written;
    if(init_next) {
      if(!initialize_next_page()) {
        if(p) {
          unlock(p->rwlatch);
          releasePage(p);
        }
        return 0; // fail
      }
    }
    first = false;
  }
}

bool DataPage::write_data(const byte * buf, size_t len, bool init_next) {
  return 0 != write_data_and_latch(buf, len, init_next, false);
}

bool DataPage::read_data(byte * buf, off_t offset, size_t len) {
  while(1) {
    assert(len > 0);
    size_t read_count = read_bytes(buf, offset, len);
    if(read_count == 0) {
      return false; // fail
    }
    if(read_count == len) {
      return true; // success
    }
    buf += read_count;
    offset += read_count;
    len -= read_count;
  }
}

bool DataPage::append(datatuple const * dat)
{
  // First, decide if we should append to this datapage, based on whether
  // appending will waste more or less space than starting a new datapage

  bool accept_tuple;
  len_t tup_len = dat->byte_length();
  // Decsion tree
  if(write_offset_ > (initial_page_count_ * PAGE_SIZE)) {
    // we already exceeded the page budget
    if(write_offset_ > (2 * initial_page_count_ * PAGE_SIZE)) {
      // ... by a lot.  Reject regardless.  This prevents small tuples from
      //     being stuck behind giant ones without sacrificing much space
      //     (as a percentage of the whole index), because this path only
      //     can happen once per giant object.
      accept_tuple = false;
    } else {
      // ... by a little bit.
      accept_tuple = true;
      //Accept tuple if it fits on this page, or if it's big..
      //accept_tuple = (((write_offset_-1) & ~(PAGE_SIZE-1)) == (((write_offset_ + tup_len)-1) & ~(PAGE_SIZE-1)));
    }
  } else {
    if(write_offset_ + tup_len < (initial_page_count_ * PAGE_SIZE)) {
      // tuple fits.  contractually obligated to accept it.
      accept_tuple = true;
    } else if(write_offset_ == 0) {
      // datapage is empty.  contractually obligated to accept tuple.
      accept_tuple = true;
    } else {
      if(tup_len > initial_page_count_ * PAGE_SIZE) {
        // this is a "big tuple"
        len_t reject_padding = PAGE_SIZE - (write_offset_ & (PAGE_SIZE-1));
        len_t accept_padding = PAGE_SIZE - ((write_offset_ + tup_len) & (PAGE_SIZE-1));
        accept_tuple = accept_padding < reject_padding;
      } else {
        // this is a "small tuple"; only exceed budget if doing so leads to < 33% overhead for this data.
        len_t accept_padding = PAGE_SIZE - (write_offset_ & (PAGE_SIZE-1));
        accept_tuple = (3*accept_padding) < tup_len;
      }
    }
  }

  if(!accept_tuple) {
    DEBUG("offset %lld closing datapage\n", write_offset_);
    return false;
  }

  DEBUG("offset %lld continuing datapage\n", write_offset_);

  // TODO could be more efficient; this does a malloc and memcpy.
  // The alternative couples us more strongly to datatuple, but simplifies
  // datapage.
  byte * buf = dat->to_bytes();
  len_t dat_len = dat->byte_length();

  Page * p = write_data_and_latch((const byte*)&dat_len, sizeof(dat_len));
  bool succ = false;
  if(p) {
    succ = write_data(buf, dat_len);
    unlock(p->rwlatch);
    releasePage(p);
  }

  free(buf);

  return succ;
}

bool DataPage::recordRead(const datatuple::key_t key, size_t keySize,  datatuple ** buf)
{
  iterator itr(this, NULL);

  int match = -1;
  while((*buf=itr.getnext()) != 0) {
    match = datatuple::compare((*buf)->strippedkey(), (*buf)->strippedkeylen(), key, keySize);

    if(match<0) { //keep searching
      datatuple::freetuple(*buf);
      *buf=0;
    } else if(match==0) { //found
      return true;
    } else { // match > 0, then does not exist
      datatuple::freetuple(*buf);
      *buf = 0;
      break;
    }
  }
  return false;
}

///////////////////////////////////////////////////////////////
//RECORD ITERATOR
///////////////////////////////////////////////////////////////


datatuple* DataPage::iterator::getnext() {
  len_t len;
  bool succ;
  if(dp == NULL) { return NULL; }
  // XXX hack: read latch the page that the record will live on.
  // This should be handled by a read_data_in_latch function, or something...
  Page * p = loadPage(dp->xid_, dp->calc_chunk_from_offset(read_offset_).page);
  readlock(p->rwlatch, 0);
  succ = dp->read_data((byte*)&len, read_offset_, sizeof(len));
  if((!succ) || (len == 0)) {
    unlock(p->rwlatch);
    releasePage(p);
    return NULL;
  }
  read_offset_ += sizeof(len);

  byte * buf = (byte*)malloc(len);
  succ = dp->read_data(buf, read_offset_, len);

  // release hacky latch
  unlock(p->rwlatch);
  releasePage(p);

  if(!succ) { read_offset_ -= sizeof(len); free(buf); return NULL; }

  read_offset_ += len;

  datatuple *ret = datatuple::from_bytes(buf);

  free(buf);

  return ret;
}
