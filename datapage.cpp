
#include "logstore.h"
#include "datapage.h"
#include <stasis/page.h>

static const int DATA_PAGE = USER_DEFINED_PAGE(1);
#define MAX_PAGE_COUNT 1000 // ~ 4MB

BEGIN_C_DECLS
static void dataPageFsck(Page* p) {
	int32_t pageCount = *stasis_page_int32_cptr_from_start(p, 0);
	assert(pageCount >= 0);
	assert(pageCount <= MAX_PAGE_COUNT);
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
int32_t DataPage<TUPLE>::readPageCount(int xid, pageid_t pid) {
  Page *p = loadPage(xid, pid);
  int32_t ret = *page_count_ptr(p);
  releasePage(p);
  return ret;
}


template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, pageid_t pid):
    page_count_(readPageCount(xid, pid)),
    first_page_(pid),
    write_offset_(-1) { assert(pid!=0); }

template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, int fix_pcount, pageid_t (*alloc_region)(int, void*), void * alloc_state) :
  page_count_(MAX_PAGE_COUNT), // XXX fix_pcount),
	first_page_(alloc_region(xid, alloc_state)),
	write_offset_(0)
{
    assert(fix_pcount >= 1);
    initialize(xid);
}

template<class TUPLE>
DataPage<TUPLE>::~DataPage() { }


template<class TUPLE>
void DataPage<TUPLE>::initialize(int xid)
{
    //load the first page
    Page *p = loadUninitializedPage(xid, first_page_);
    writelock(p->rwlatch,0);
    
    //initialize header
    
    //set number of pages to 1
    *page_count_ptr(p) = page_count_;
    
    //write 0 to first data size    
    *length_at_offset_ptr(p, write_offset_) = 0;

    //set the page dirty
    stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
    
    p->pageType = DATA_PAGE;

    //release the page
    unlock(p->rwlatch);
    releasePage(p);
}

template <class TUPLE>
size_t DataPage<TUPLE>::write_bytes(int xid, const byte * buf, size_t remaining) {
	recordid chunk = calc_chunk_from_offset(write_offset_);
	if(chunk.size > remaining) {
		chunk.size = remaining;
	}
	if(chunk.page >= first_page_ + page_count_) {
		chunk.size = 0; // no space
	} else {
		Page *p = loadPage(xid, chunk.page);
		memcpy(data_at_offset_ptr(p, chunk.slot), buf, chunk.size);
		writelock(p->rwlatch,0);
		stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
		unlock(p->rwlatch);
		releasePage(p);
		write_offset_ += chunk.size;
	}
	return chunk.size;
}
template <class TUPLE>
size_t DataPage<TUPLE>::read_bytes(int xid, byte * buf, off_t offset, size_t remaining) {
	recordid chunk = calc_chunk_from_offset(offset);
	if(chunk.size > remaining) {
		chunk.size = remaining;
	}
	if(chunk.page >= first_page_ + page_count_) {
		chunk.size = 0; // eof
	} else {
		Page *p = loadPage(xid, chunk.page);
		memcpy(buf, data_at_offset_ptr(p, chunk.slot), chunk.size);
		releasePage(p);
	}
	return chunk.size;
}

template <class TUPLE>
bool DataPage<TUPLE>::initialize_next_page(int xid) {
  recordid rid = calc_chunk_from_offset(write_offset_);
  assert(rid.slot == 0);
  if(rid.page >= first_page_ + page_count_) {
    return false;
  } else {
    Page *p = loadUninitializedPage(xid, rid.page);
    p->pageType = DATA_PAGE;
    *page_count_ptr(p) = 0;
    *length_at_offset_ptr(p,0) = 0;
    writelock(p->rwlatch, 0); //XXX this is pretty obnoxious.  Perhaps stasis shouldn't check for the latch
    stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
    unlock(p->rwlatch);
    releasePage(p);
    return true;
  }
}

template <class TUPLE>
bool DataPage<TUPLE>::write_data(int xid, const byte * buf, size_t len) {
	while(1) {
		assert(len > 0);
		size_t written = write_bytes(xid, buf, len);
		if(written == 0) {
			return false; // fail
		}
		if(written == len) {
			return true; // success
		}
		buf += written;
		len -= written;
		if(!initialize_next_page(xid)) {
		  return false; // fail
		}
	}
}
template <class TUPLE>
bool DataPage<TUPLE>::read_data(int xid, byte * buf, off_t offset, size_t len) {
	while(1) {
		assert(len > 0);
		size_t read_count = read_bytes(xid, buf, offset, len);
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
bool DataPage<TUPLE>::append(int xid, TUPLE const * dat)
{
	byte * buf = dat->to_bytes(); // TODO could be more efficient; this does a malloc and memcpy.  The alternative couples us more strongly to datapage, but simplifies datapage.
	len_t dat_len = dat->byte_length();

	bool succ = write_data(xid, (const byte*)&dat_len, sizeof(dat_len));
	if(succ) {
	  succ = write_data(xid, buf, dat_len);
	}

	if(succ) {

	  dat_len = 0; // write terminating zero.

	  succ = write_data(xid, (const byte*)&dat_len, sizeof(dat_len));

	  if(succ) {
	    write_offset_ -= sizeof(dat_len); // want to overwrite zero next time around.
	  }

	  // if writing the zero fails, later reads will fail as well, and assume EOF.
	  succ = true;  // return true (the tuple has been written regardless of whether the zero fit)

	}

	free(buf);

	return succ;
}

template <class TUPLE>
bool DataPage<TUPLE>::recordRead(int xid, typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf)
{
    RecordIterator itr(this);

    int match = -1;
    while((*buf=itr.getnext(xid)) != 0)
        {
            match = TUPLE::compare((*buf)->key(), key);
            
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

template <class TUPLE>
pageid_t DataPage<TUPLE>::dp_alloc_region(int xid, void *conf)
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
  a->nextPage = a->endOfRegion; // Allocate everything all at once.

  DEBUG("ret %lld\n",ret);
  return ret;

}

template <class TUPLE>
pageid_t DataPage<TUPLE>::dp_alloc_region_rid(int xid, void * ridp) {
  recordid rid = *(recordid*)ridp;
  RegionAllocConf_t conf;
  Tread(xid,rid,&conf);
  pageid_t ret = dp_alloc_region(xid,&conf);
  //DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);
  // XXX get rid of Tset by storing next page in memory, and losing it
  //     on crash.
  Tset(xid,rid,&conf);
  return ret;
}

template <class TUPLE>
void DataPage<TUPLE>::dealloc_region_rid(int xid, void *conf)
{
    RegionAllocConf_t a = *((RegionAllocConf_t*)conf);
    DEBUG("{%lld <- dealloc region arraylist}\n", a.regionList.page);

    for(int i = 0; i < a.regionCount; i++) {
     a.regionList.slot = i;
     pageid_t pid;
     Tread(xid,a.regionList,&pid);
     TregionDealloc(xid,pid);
    }
}

template <class TUPLE>
void DataPage<TUPLE>::force_region_rid(int xid, void *conf)
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


///////////////////////////////////////////////////////////////
//RECORD ITERATOR
///////////////////////////////////////////////////////////////


template <class TUPLE>
TUPLE* DataPage<TUPLE>::RecordIterator::getnext(int xid)
{
	len_t len;
	bool succ;

	succ = dp->read_data(xid, (byte*)&len, read_offset_, sizeof(len));
	if((!succ) || (len == 0)) { return NULL; }
	read_offset_ += sizeof(len);

	byte * buf = (byte*)malloc(len);

	succ = dp->read_data(xid, buf, read_offset_, len);

	if(!succ) { read_offset_ -= sizeof(len); free(buf); return NULL; }

	read_offset_ += len;

	TUPLE *ret = TUPLE::from_bytes(buf);

	free(buf);

	return ret;
}

template <class TUPLE>
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
}
