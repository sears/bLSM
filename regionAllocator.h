/*
 * regionAllocator.h
 *
 *  Created on: Mar 9, 2010
 *      Author: sears
 */

#ifndef REGIONALLOCATOR_H_
#define REGIONALLOCATOR_H_

#include <stasis/transactional.h>
#undef try
#undef end


class RegionAllocator
{
public:

    // Open an existing region allocator.
    RegionAllocator(int xid, recordid rid) :
      nextPage_(INVALID_PAGE),
      endOfRegion_(INVALID_PAGE) {
        rid_ = rid;
        Tread(xid, rid_, &header_);
        regionCount_ = TarrayListLength(xid, header_.region_list);
    }
  // Create a new region allocator.
    RegionAllocator(int xid, pageid_t region_length) :
        nextPage_(0),
        endOfRegion_(0),
        regionCount_(0)
    {
        rid_ = Talloc(xid, sizeof(header_));
        header_.region_list = TarrayListAlloc(xid, 1, 2, sizeof(pageid_t));
        header_.region_length = region_length;
        Tset(xid, rid_, &header_);
    }
  // XXX handle disk full?
  pageid_t alloc_extent(int xid, pageid_t extent_length) {
    assert(nextPage_ != INVALID_PAGE);
    pageid_t ret = nextPage_;
    nextPage_ += extent_length;
    if(nextPage_ >= endOfRegion_) {
      ret = TregionAlloc(xid, header_.region_length, 42); // XXX assign a region allocator id
      TarrayListExtend(xid, header_.region_list, 1);
      recordid rid = header_.region_list;
      rid.slot = regionCount_;
      Tset(xid, rid, &ret);
      assert(extent_length <= header_.region_length); // XXX could handle this case if we wanted to.  Would remove this error case, and not be hard.
      nextPage_ = ret + extent_length;
      endOfRegion_ = ret + header_.region_length;
      regionCount_++;
      assert(regionCount_ == TarrayListLength(xid, header_.region_list));
    }
    return ret;
  }
  bool grow_extent(pageid_t extension_length) {
    assert(nextPage_ != INVALID_PAGE);
    nextPage_ += extension_length;
    return(nextPage_ < endOfRegion_);
  }
  void force_regions(int xid) {
    assert(nextPage_ != INVALID_PAGE);
    pageid_t regionCount = TarrayListLength(xid, header_.region_list);
    for(recordid list_entry = header_.region_list;
        list_entry.slot < regionCount; list_entry.slot++) {
      pageid_t pid;
      Tread(xid, list_entry, &pid);
      TregionForce(xid, pid);
    }
  }
  void dealloc_regions(int xid) {
    pageid_t regionCount = TarrayListLength(xid, header_.region_list);

    DEBUG("{%lld %lld %lld}\n", header_.region_list.page, (long long)header_.region_list.slot, (long long)header_.region_list.size);

    for(recordid list_entry = header_.region_list;
        list_entry.slot < regionCount; list_entry.slot++) {
      pageid_t pid;
      Tread(xid, list_entry, &pid);
#ifndef CHECK_FOR_SCRIBBLING  // Don't actually free the page if we'll be checking that pages are used exactly once below.
      TregionDealloc(xid, pid);
#endif
    }
    TarrayListDealloc(xid, header_.region_list);
    Tdealloc(xid, rid_);
  }
  pageid_t * list_regions(int xid, pageid_t * region_length, pageid_t * region_count) {
      *region_count = TarrayListLength(xid, header_.region_list);
      pageid_t * ret = (pageid_t*)malloc(sizeof(pageid_t) * *region_count);
      recordid rid = header_.region_list;
      for(pageid_t i = 0; i < *region_count; i++) {
          rid.slot = i;
          Tread(xid, rid, &ret[i]);
      }
      *region_length = header_.region_length;
      return ret;
  }
  void done() {
    nextPage_ = INVALID_PAGE;
    endOfRegion_ = INVALID_PAGE;
  }
  recordid header_rid() { return rid_; }


  lsn_t get_lsn(int xid) {
    lsn_t xid_lsn = stasis_transaction_table_get((stasis_transaction_table_t*)stasis_runtime_transaction_table(), xid)->prevLSN;
    lsn_t log_lsn = ((stasis_log_t*)stasis_log())->next_available_lsn((stasis_log_t*)stasis_log());
    lsn_t ret = xid_lsn == INVALID_LSN ? log_lsn-1 : xid_lsn;
    assert(ret != INVALID_LSN);
    return ret;
  }
private:
    typedef struct {
        recordid region_list;
        pageid_t region_length;
    } persistent_state;

    recordid rid_;
    pageid_t nextPage_;
    pageid_t endOfRegion_;
    pageid_t regionCount_;
    persistent_state header_;
public:
    static const size_t header_size = sizeof(persistent_state);
};


#endif /* REGIONALLOCATOR_H_ */
