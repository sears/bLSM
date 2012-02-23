/*
 * regionAllocator.h
 *
 * Copyright 2010-2012 Yahoo! Inc.
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
 *  Created on: Mar 9, 2010
 *      Author: sears
 */

#ifndef REGIONALLOCATOR_H_
#define REGIONALLOCATOR_H_

#include <stasis/transactional.h>

class regionAllocator
{
public:

    // Open an existing region allocator.
    regionAllocator(int xid, recordid rid) :
      nextPage_(INVALID_PAGE),
      endOfRegion_(INVALID_PAGE),
      bm_((stasis_buffer_manager_t*)stasis_runtime_buffer_manager()),
      bmh_(bm_->openHandleImpl(bm_, 1)) {
        rid_ = rid;
        Tread(xid, rid_, &header_);
        regionCount_ = TarrayListLength(xid, header_.region_list);
    }
  // Create a new region allocator.
  regionAllocator(int xid, pageid_t region_page_count) :
      nextPage_(0),
      endOfRegion_(0),
      regionCount_(0),
      bm_((stasis_buffer_manager_t*)stasis_runtime_buffer_manager()),
      bmh_(bm_->openHandleImpl(bm_, 1))
  {
      rid_ = Talloc(xid, sizeof(header_));
      header_.region_list = TarrayListAlloc(xid, 1, 2, sizeof(pageid_t));
      header_.region_page_count = region_page_count;
      Tset(xid, rid_, &header_);
  }
  explicit regionAllocator() :
    nextPage_(INVALID_PAGE),
    endOfRegion_(INVALID_PAGE),
    bm_((stasis_buffer_manager_t*)stasis_runtime_buffer_manager()),
    bmh_(bm_->openHandleImpl(bm_, 1)){
    rid_.page = INVALID_PAGE;
    regionCount_ = -1;
  }
  ~regionAllocator() {
    bm_->closeHandleImpl(bm_, bmh_);
  }
  Page * load_page(int xid, pageid_t p) { return bm_->loadPageImpl(bm_, bmh_, xid, p, UNKNOWN_TYPE_PAGE); }

  // XXX handle disk full?
  pageid_t alloc_extent(int xid, pageid_t extent_length) {
    assert(nextPage_ != INVALID_PAGE);
    pageid_t ret = nextPage_;
    nextPage_ += extent_length;
    if(nextPage_ >= endOfRegion_) {
      ret = TregionAlloc(xid, header_.region_page_count, 42); // XXX assign a region allocator id
      TarrayListExtend(xid, header_.region_list, 1);
      recordid rid = header_.region_list;
      rid.slot = regionCount_;
      Tset(xid, rid, &ret);
      assert(extent_length <= header_.region_page_count); // XXX could handle this case if we wanted to.  Would remove this error case, and not be hard.
      nextPage_ = ret + extent_length;
      endOfRegion_ = ret + header_.region_page_count;
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
      TregionForce(xid, bm_, bmh_, pid);
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
      *region_length = header_.region_page_count;
      return ret;
  }
  void done() {
    nextPage_ = INVALID_PAGE;
    endOfRegion_ = INVALID_PAGE;
  }
  recordid header_rid() { return rid_; }


  lsn_t get_lsn(int xid) {
    // XXX we shouldn't need to have this logic in here anymore...
    lsn_t xid_lsn = stasis_transaction_table_get((stasis_transaction_table_t*)stasis_runtime_transaction_table(), xid)->prevLSN;
    lsn_t log_lsn = ((stasis_log_t*)stasis_log())->next_available_lsn((stasis_log_t*)stasis_log());
    lsn_t ret = xid_lsn == INVALID_LSN ? log_lsn-1 : xid_lsn;
    assert(ret != INVALID_LSN);
    return ret;
  }
private:
    typedef struct {
        recordid region_list;
        pageid_t region_page_count;
    } persistent_state;

    recordid rid_;
    pageid_t nextPage_;
    pageid_t endOfRegion_;
    pageid_t regionCount_;
    stasis_buffer_manager_t * bm_;
    stasis_buffer_manager_handle_t *bmh_;
    persistent_state header_;
public:
    static const size_t header_size = sizeof(persistent_state);
};


#endif /* REGIONALLOCATOR_H_ */
