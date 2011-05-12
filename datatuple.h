#include <network.h>

#ifndef _DATATUPLE_H_
#define _DATATUPLE_H_

#include <string>
#include <stdlib.h>
typedef unsigned char byte;
#include <cstring>
#include <assert.h>


typedef struct datatuple
{
public:
	typedef unsigned char* key_t ;
	typedef unsigned char* data_t ;
private:
	len_t datalen_;
	byte* data_; // aliases key().  data_ - 1 should be the \0 terminating key().

  datatuple* sanity_check() {
    assert(rawkeylen() < 3000);
    return this;
  }
public:

	inline len_t rawkeylen() const {
		return data_ - rawkey();
	}
	inline len_t strippedkeylen() const {
	  const size_t ts_sz = sizeof(uint64_t)+1;
          size_t al = rawkeylen();
          if(al <= ts_sz || rawkey()[al-ts_sz]!=0) {
            return al;
          } else {
            return al - ts_sz;
          }
	}
	inline len_t datalen() const {
		return (datalen_ == DELETE) ? 0 : datalen_;
	}

    //returns the length of the byte array representation
    len_t byte_length() const {
		return sizeof(len_t) + sizeof(len_t) + rawkeylen() + datalen();
    }
    static len_t length_from_header(len_t keylen, len_t datalen) {
    	return keylen + ((datalen == DELETE) ? 0 : datalen);
    }

    inline key_t rawkey() const {
		return (key_t)(this+1);
	}
	inline data_t data() const {
		return data_;
	}

	inline key_t strippedkey() const {
	  return (key_t)(this+1);
	}
    //this is used by the stl set
    bool operator() (const datatuple* lhs, const datatuple* rhs) const {
		return compare(lhs->strippedkey(), lhs->strippedkeylen(), rhs->strippedkey(), rhs->strippedkeylen()) < 0; //strcmp((char*)lhs.key(),(char*)rhs.key()) < 0;
	}

    /**
     * This function handles ASCII and UTF-8 correctly, as well as 64-bit
     * and shorter integers encoded with the most significant byte first.
     *
     * It also handles a special tuple encoding, where multiple utf-8 strings
     * are concatenated with \254, and \255 is used as a high key. \254 and
     * \255 never occur in valid UTF-8 strings. (However, this encoding
     * tie-breaks by placing substrings *later* in the sort order, which
     * is non-standard.)
     *
     * If a key is greater than 64-bits long, then this function checks to see
     * if the 9th to last byte is zero (null bytes are disallowed by both ASCII
     * and UTF-8.  If so, it discards everything after the null.  This allows
     * us to pack a 64-bit timestamp into the end of the key.
     *
     *
     * return -1 if k1 < k2
     * 0 if k1 == k2
     * 1 of k1 > k2
     */
    static int compare(const byte* k1,size_t k1l, const byte* k2, size_t k2l) {

      const size_t ts_sz = sizeof(int64_t)+1;
#if 0
      // hack for gc
      if(k1l > ts_sz && ! k1[k1l-ts_sz]) {
        k1l -= ts_sz;
      }
      if(k2l > ts_sz && ! k2[k2l-ts_sz]) {
        k2l -= ts_sz;
      }
#endif

      size_t min_l = k1l < k2l ? k1l : k2l;

      int ret = memcmp(k1,k2, min_l);
      if(ret)        return ret;
      if(k1l < k2l)  return -1;
      if(k1l == k2l) return 0;
      return 1;
    }

    int64_t timestamp() {
      const size_t ts_sz = sizeof(uint64_t)+1;
      size_t al = rawkeylen();
      if(al <= ts_sz || rawkey()[al-ts_sz]!=0) { return (uint64_t)-1; }
      return (int64_t)*(uint64_t*)(rawkey()+1+al-ts_sz);
    }

    static int compare_obj(const datatuple * a, const datatuple* b) {
      return compare(a->strippedkey(), a->strippedkeylen(), b->strippedkey(), b->strippedkeylen());
    }

    inline void setDelete() {
		datalen_ = DELETE;
	}

    inline bool isDelete() const {
		return datalen_ == DELETE;
	}

    static std::string key_to_str(const byte* k) {
		//for strings
		return std::string((char*)k);
		//for int
		/*
		std::ostringstream ostr;
		ostr << *((int32_t*)k);
		return ostr.str();
		*/
	}

    //copy the tuple.  does a deep copy of the contents.
    datatuple* create_copy() const {
        return create(rawkey(), rawkeylen(), data(), datalen_);
    }


    static datatuple* create(const void* key, len_t keylen) {
      return create(key, keylen, 0, DELETE);
    }
    static datatuple* create(const void* key, len_t keylen, const void* data, len_t datalen) {
    	datatuple *ret = (datatuple*)malloc(sizeof(datatuple) + length_from_header(keylen,datalen));
    	memcpy(ret->rawkey(), key, keylen);
    	ret->data_ = ret->rawkey() + keylen;  // need to set this even if delete, since it encodes the key length.
    	if(datalen != DELETE) {
    		memcpy(ret->data_, data, datalen);
    	}
    	ret->datalen_ = datalen;
    	return ret->sanity_check();
    }

    //format: key length _   data length _ key _ data
    byte * to_bytes() const {
    	byte *ret = (byte*)malloc(byte_length());
    	((len_t*)ret)[0] = rawkeylen();
    	((len_t*)ret)[1] = datalen_;
    	memcpy(((len_t*)ret)+2, rawkey(), length_from_header(rawkeylen(), datalen_));
        return ret;
    }

    const byte* get_bytes(len_t *keylen, len_t *datalen) const {
      *keylen  = this->rawkeylen();
      *datalen = datalen_;
      return rawkey();
    }

    //format of buf: key _ data.  The caller needs to 'peel' off key length and data length for this call.
    static datatuple* from_bytes(len_t keylen, len_t datalen, byte* buf) {
    	datatuple *dt = (datatuple*) malloc(sizeof(datatuple) + length_from_header(keylen,datalen));
    	dt->datalen_ = datalen;
    	memcpy(dt->rawkey(),buf, length_from_header(keylen,datalen));
    	dt->data_ = dt->rawkey() + keylen;
    	return dt->sanity_check();
    }
    static datatuple* from_bytes(byte* buf) {
      len_t keylen = ((len_t*)buf)[0];
      len_t buflen = length_from_header(keylen, ((len_t*)buf)[1]);
      datatuple *dt = (datatuple*) malloc(sizeof(datatuple) + buflen);
      dt->datalen_ = ((len_t*)buf)[1];
      memcpy(dt->rawkey(),((len_t*)buf)+2,buflen);
      dt->data_ = dt->rawkey() + keylen;

    	return dt->sanity_check();
    }

    static inline void freetuple(datatuple* dt) {
        free(dt);
    }

} datatuple;


#endif
