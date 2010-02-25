#ifndef _SIMPLE_DATA_PAGE_H_
#define _SIMPLE_DATA_PAGE_H_

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>


//#define CHECK_FOR_SCRIBBLING

template<class TUPLE>
class DataPage
{
public:
    
    class RecordIterator
    {
    private:
      void scan_to_key(TUPLE * key) {
	if(key) {
	  len_t old_off = read_offset_;
	  TUPLE * t = getnext();
	  while(t && TUPLE::compare(key->key(), key->keylen(), t->key(), t->keylen()) > 0) {
	    TUPLE::freetuple(t);
	    old_off = read_offset_;
	    t = getnext();
	  }
	  if(t) {
	    DEBUG("datapage opened at %s\n", t->key());
	    TUPLE::freetuple(t);
	    read_offset_ = old_off;
	  } else {
		  DEBUG("datapage key not found.  Offset = %lld", read_offset_);
		  dp = NULL;
	  }
	}
      }
    public:
      RecordIterator(DataPage *dp, TUPLE * key) : read_offset_(0), dp(dp) {
    	  scan_to_key(key);
      }

        void operator=(const RecordIterator &rhs)
            {
                this->read_offset_ = rhs.read_offset_;
                this->dp = rhs.dp;
            }

        //returns the next tuple and also advances the iterator
        TUPLE *getnext();

        //advance the iterator by count tuples, i.e. skip over count tuples         
 //       void advance(int xid, int count=1);

        off_t read_offset_;
        DataPage *dp;
        
    };

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
    
public:

    //to be used when reading an existing data page from disk
    DataPage( int xid, pageid_t pid );

    //to be used to create new data pages
    DataPage( int xid, pageid_t page_count, RegionAllocator* alloc);

    ~DataPage() {
    	if(write_offset_ != -1) {
    	  len_t dat_len = 0; // write terminating zero.

    	  write_data((const byte*)&dat_len, sizeof(dat_len), false);

    	  // if writing the zero fails, later reads will fail as well, and assume EOF.

    	}

    }

    bool append(TUPLE const * dat);
    bool recordRead(typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf);

    inline uint16_t recordCount();


    RecordIterator begin(){return RecordIterator(this, NULL);}

    pageid_t get_start_pid(){return first_page_;}
    int get_page_count(){return page_count_;}

    static void register_stasis_page_impl();

private:

    //    static pageid_t dp_alloc_region(int xid, void *conf, pageid_t count);

    void initialize();

private:
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
    size_t write_bytes(const byte * buf, size_t remaining);
    size_t read_bytes(byte * buf, off_t offset, size_t remaining);
    bool write_data(const byte * buf, size_t len, bool init_next = true);
    bool read_data(byte * buf, off_t offset, size_t len);
    bool initialize_next_page();

    int xid_;
    pageid_t page_count_;
    const pageid_t initial_page_count_;
    RegionAllocator *alloc_;
    const pageid_t first_page_;
    off_t write_offset_; // points to the next free byte (ignoring page boundaries)
};
#endif
