#ifndef _SIMPLE_DATA_PAGE_H_
#define _SIMPLE_DATA_PAGE_H_

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>

struct RegionAllocator;

//#define CHECK_FOR_SCRIBBLING

template<class TUPLE>
class DataPage
{
public:
    
    class iterator
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
      iterator(DataPage *dp, TUPLE * key=NULL) : read_offset_(0), dp(dp) {
    	  scan_to_key(key);
      }

        void operator=(const iterator &rhs)
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

    bool append(TUPLE const * dat);
    bool recordRead(const typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf);

    inline uint16_t recordCount();


    iterator begin(){return iterator(this);}

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
    void initialize_page(pageid_t pageid);

    int xid_;
    pageid_t page_count_;
    const pageid_t initial_page_count_;
    RegionAllocator *alloc_;
    const pageid_t first_page_;
    off_t write_offset_; // points to the next free byte (ignoring page boundaries)
};
#endif
