#include "logstore.h"
#include "datapage.h"
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

template <class TUPLE>
void DataPage<TUPLE>::register_stasis_page_impl() {
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

template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, pageid_t pid):
    xid_(xid),
    page_count_(1), // will be opportunistically incremented as we scan the datapage.
    initial_page_count_(-1), // used by append.
    alloc_(0),  // read-only, and we don't free data pages one at a time.
    first_page_(pid),
    write_offset_(-1) {
	assert(pid!=0);
	Page *p = loadPage(xid_, first_page_);
	if(!(*is_another_page_ptr(p) == 0 || *is_another_page_ptr(p) == 2)) {
		printf("Page %lld is not the start of a datapage\n", first_page_);  fflush(stdout);
		abort();
	}
	assert(*is_another_page_ptr(p) == 0 || *is_another_page_ptr(p) == 2); // would be 1 for page in the middle of a datapage
	releasePage(p);
}

template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, pageid_t page_count, RegionAllocator *alloc) :
	xid_(xid),
	page_count_(1),
	initial_page_count_(page_count),
	alloc_(alloc),
	first_page_(alloc_->alloc_extent(xid_, page_count_)),
	write_offset_(0)
{
	DEBUG("Datapage page count: %lld pid = %lld\n", (long long int)page_count_, (long long int)first_page_);
    assert(page_count_ >= 1);
    initialize();
}

template<class TUPLE>
void DataPage<TUPLE>::initialize() {
	initialize_page(first_page_);
}
template<class TUPLE>
void DataPage<TUPLE>::initialize_page(pageid_t pageid) {
    //load the first page
    Page *p;
#ifdef CHECK_FOR_SCRIBBLING
    p = loadPage(xid_, pageid);
    if(*stasis_page_type_ptr(p) == DATA_PAGE) {
    	printf("Collision on page %lld\n", (long long)pageid); fflush(stdout);
    	assert(*stasis_page_type_ptr(p) != DATA_PAGE);
    }
#else
    p = loadUninitializedPage(xid_, pageid);
#endif
    //XXX this is pretty obnoxious.  Perhaps stasis shouldn't check for the latch
    writelock(p->rwlatch,0);
    
    DEBUG("\t\t\t\t\t\t->%lld\n", pageid);

    //initialize header
    p->pageType = DATA_PAGE;
    
    //we're the last page for now.
    *is_another_page_ptr(p) = 0;
    
    //write 0 to first data size    
    *length_at_offset_ptr(p, calc_chunk_from_offset(write_offset_).slot) = 0;

    //set the page dirty
    stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));

    //release the page
    unlock(p->rwlatch);
    releasePage(p);
}
template <class TUPLE>
size_t DataPage<TUPLE>::write_bytes(const byte * buf, size_t remaining) {
	recordid chunk = calc_chunk_from_offset(write_offset_);
	if(chunk.size > remaining) {
		chunk.size = remaining;
	}
	if(chunk.page >= first_page_ + page_count_) {
	    chunk.size = 0; // no space (should not happen)
	} else {
		Page *p = loadPage(xid_, chunk.page);
		memcpy(data_at_offset_ptr(p, chunk.slot), buf, chunk.size);
		writelock(p->rwlatch,0);
		stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));
		unlock(p->rwlatch);
		releasePage(p);
		write_offset_ += chunk.size;
	}
	return chunk.size;
}
template <class TUPLE>
size_t DataPage<TUPLE>::read_bytes(byte * buf, off_t offset, size_t remaining) {
	recordid chunk = calc_chunk_from_offset(offset);
	if(chunk.size > remaining) {
		chunk.size = remaining;
	}
	if(chunk.page >= first_page_ + page_count_) {
		chunk.size = 0; // eof
	} else {
		Page *p = loadPage(xid_, chunk.page);
		assert(p->pageType == DATA_PAGE);
		if((chunk.page + 1 == page_count_ + first_page_)
		  && (*is_another_page_ptr(p))) {
			page_count_++;
		}
		memcpy(buf, data_at_offset_ptr(p, chunk.slot), chunk.size);
		releasePage(p);
	}
	return chunk.size;
}

template <class TUPLE>
bool DataPage<TUPLE>::initialize_next_page() {
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

  Page *p = loadPage(xid_, rid.page-1);
  *is_another_page_ptr(p) = (rid.page-1 == first_page_) ? 2 : 1;
  writelock(p->rwlatch, 0);
  stasis_page_lsn_write(xid_, p, alloc_->get_lsn(xid_));
  unlock(p->rwlatch);
  releasePage(p);

  initialize_page(rid.page);
  return true;
}

template <class TUPLE>
bool DataPage<TUPLE>::write_data(const byte * buf, size_t len, bool init_next) {
	bool first = true;
	while(1) {
		assert(len > 0);
		size_t written = write_bytes(buf, len);
		if(written == 0) {
			return false; // fail
		}
		if(written == len) {
			return true; // success
		}
		if(len > PAGE_SIZE && ! first) {
			assert(written > 4000);
		}
		buf += written;
		len -= written;
		if(init_next) {
			if(!initialize_next_page()) {
			  return false; // fail
			}
		}
		first = false;
	}
}
template <class TUPLE>
bool DataPage<TUPLE>::read_data(byte * buf, off_t offset, size_t len) {
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
template <class TUPLE>
bool DataPage<TUPLE>::append(TUPLE const * dat)
{
  // Don't append record to already-full datapage.  The record could push us over the page limit, but that's OK.
        if(write_offset_ > (initial_page_count_ * PAGE_SIZE)) { return false; }

	byte * buf = dat->to_bytes(); // TODO could be more efficient; this does a malloc and memcpy.  The alternative couples us more strongly to datapage, but simplifies datapage.
	len_t dat_len = dat->byte_length();

	bool succ = write_data((const byte*)&dat_len, sizeof(dat_len));
	if(succ) {
		succ = write_data(buf, dat_len);
	}

	free(buf);

	return succ;
}

template <class TUPLE>
bool DataPage<TUPLE>::recordRead(typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf)
{
  iterator itr(this, NULL);

    int match = -1;
    while((*buf=itr.getnext()) != 0)
        {
	  match = TUPLE::compare((*buf)->key(), (*buf)->keylen(), key, keySize);
            
            if(match<0) //keep searching
            {
                datatuple::freetuple(*buf);
                *buf=0;
            }
            else if(match==0) //found
            {
                return true;
            }
            else // match > 0, then does not exist
            {
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


template <class TUPLE>
TUPLE* DataPage<TUPLE>::iterator::getnext()
{
	len_t len;
	bool succ;
	if(dp == NULL) { return NULL; }
	succ = dp->read_data((byte*)&len, read_offset_, sizeof(len));
	if((!succ) || (len == 0)) { return NULL; }
	read_offset_ += sizeof(len);

	byte * buf = (byte*)malloc(len);

	succ = dp->read_data(buf, read_offset_, len);

	if(!succ) { read_offset_ -= sizeof(len); free(buf); return NULL; }

	read_offset_ += len;

	TUPLE *ret = TUPLE::from_bytes(buf);

	free(buf);

	return ret;
}

/*template <class TUPLE>
void DataPage<TUPLE>::RecordIterator::advance(int xid, int count)
{
	len_t len;
	bool succ;
	for(int i = 0; i < count; i++) {
	  succ = dp->read_data(xid, (byte*)&len, read_offset_, sizeof(len));
	  if((!succ) || (len == 0)) { return; }
	  read_offset_ += sizeof(len);
	  read_offset_ += len;
	}
}*/


template class DataPage<datatuple>;

