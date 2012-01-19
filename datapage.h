/*
 * datapage.h
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
#ifndef _SIMPLE_DATA_PAGE_H_
#define _SIMPLE_DATA_PAGE_H_

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>
#include "datatuple.h"

struct RegionAllocator;

//#define CHECK_FOR_SCRIBBLING

class DataPage
{
public:
  class iterator
  {
  private:
    void scan_to_key(datatuple * key) {
      if(key) {
        len_t old_off = read_offset_;
        datatuple * t = getnext();
        while(t && datatuple::compare(key->strippedkey(), key->strippedkeylen(), t->strippedkey(), t->strippedkeylen()) > 0) {
          datatuple::freetuple(t);
          old_off = read_offset_;
          t = getnext();
        }
        if(t) {
          DEBUG("datapage opened at %s\n", t->key());
          datatuple::freetuple(t);
          read_offset_ = old_off;
        } else {
          DEBUG("datapage key not found.  Offset = %lld", read_offset_);
          dp = NULL;
        }
      }
    }
  public:
    iterator(DataPage *dp, datatuple * key=NULL) : read_offset_(0), dp(dp) {
      scan_to_key(key);
    }

    void operator=(const iterator &rhs) {
      this->read_offset_ = rhs.read_offset_;
      this->dp = rhs.dp;
    }

    //returns the next tuple and also advances the iterator
    datatuple *getnext();

  private:
    off_t read_offset_;
    DataPage *dp;
  };

public:

  /**
   * if alloc is non-null, then reads will be optimized for sequential access
   */
  DataPage( int xid, RegionAllocator* alloc, pageid_t pid );

  //to be used to create new data pages
  DataPage( int xid, pageid_t page_count, RegionAllocator* alloc);

  ~DataPage() {
    assert(write_offset_ == -1);
  }

  void writes_done() {
    if(write_offset_ != -1) {
      len_t dat_len = 0; // write terminating zero.

      write_data((const byte*)&dat_len, sizeof(dat_len), false);

      // if writing the zero fails, later reads will fail as well, and assume EOF.

      write_offset_ = -1;
    }

  }

  bool append(datatuple const * dat);
  bool recordRead(const  datatuple::key_t key, size_t keySize,  datatuple ** buf);

  inline uint16_t recordCount();

  iterator begin(){return iterator(this);}

  pageid_t get_start_pid(){return first_page_;}
  int get_page_count(){return page_count_;}

  static void register_stasis_page_impl();

private:

  void initialize();

  static const uint16_t DATA_PAGE_HEADER_SIZE = sizeof(int32_t);
  static const uint16_t DATA_PAGE_SIZE = USABLE_SIZE_OF_PAGE - DATA_PAGE_HEADER_SIZE;
  typedef uint32_t len_t;

  static inline int32_t* is_another_page_ptr(Page *p) {
      return stasis_page_int32_ptr_from_start(p,0);
  }
  static inline byte * data_at_offset_ptr(Page *p, slotid_t offset) {
      return ((byte*)(is_another_page_ptr(p)+1))+offset;
  }
  static inline len_t * length_at_offset_ptr(Page *p, slotid_t offset) {
      return (len_t*)data_at_offset_ptr(p,offset);
  }

  inline recordid calc_chunk_from_offset(off_t offset) {
    recordid ret;
    ret.page = first_page_ + offset / DATA_PAGE_SIZE;
    ret.slot = offset % DATA_PAGE_SIZE;
    ret.size = DATA_PAGE_SIZE - ret.slot;
    assert(ret.size);
    return ret;
  }

  size_t write_bytes(const byte * buf, ssize_t remaining, Page ** latch_p = NULL);
  size_t read_bytes(byte * buf, off_t offset, ssize_t remaining);
  Page * write_data_and_latch(const byte * buf, size_t len, bool init_next = true, bool latch = true);
  bool write_data(const byte * buf, size_t len, bool init_next = true);
  bool read_data(byte * buf, off_t offset, size_t len);
  bool initialize_next_page();
  void initialize_page(pageid_t pageid);

  int xid_;
  pageid_t page_count_;
  const pageid_t initial_page_count_;
  RegionAllocator *alloc_;
  const pageid_t first_page_;
  off_t write_offset_; // points to the next free byte (ignoring page boundaries)
};
#endif
