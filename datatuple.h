#include <network.h>

#ifndef _DATATUPLE_H_
#define _DATATUPLE_H_

#include <string>
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
	byte* key_;
	byte* data_; // aliases key.  data_ - 1 should be the \0 terminating key_.

  datatuple* sanity_check() {
    assert(keylen() < 3000);
    return this;
  }
public:

	inline len_t keylen() const {
		return data_ - key_;
	}
	inline len_t datalen() const {
		return (datalen_ == DELETE) ? 0 : datalen_;
	}

    //returns the length of the byte array representation
    len_t byte_length() const {
		return sizeof(len_t) + sizeof(len_t) + keylen() + datalen();
    }
    static len_t length_from_header(len_t keylen, len_t datalen) {
    	return keylen + ((datalen == DELETE) ? 0 : datalen);
    }

    inline key_t key() const {
		return key_;
	}
	inline data_t data() const {
		return data_;
	}

    //this is used by the stl set
    bool operator() (const datatuple* lhs, const datatuple* rhs) const {
		return compare(lhs->key(), rhs->key()) < 0; //strcmp((char*)lhs.key(),(char*)rhs.key()) < 0;
	}

    /**
     * return -1 if k1 < k2
     * 0 if k1 == k2
     * 1 of k1 > k2
    **/
    static int compare(const byte* k1,const byte* k2) {
		// XXX string comparison is probably not the right approach.
		//for char* ending with \0
		return strcmp((char*)k1,(char*)k2);
	}

    static int compare_obj(const datatuple * a, const datatuple* b) {
    	return compare(a->key(), b->key());
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
        return create(key(), keylen(), data(), datalen_)->sanity_check();
    }


    static datatuple* create(const void* key, len_t keylen) {
      return create(key, keylen, 0, DELETE)->sanity_check();
    }
    static datatuple* create(const void* key, len_t keylen, const void* data, len_t datalen) {
    	datatuple *ret = (datatuple*)malloc(sizeof(datatuple));
    	ret->key_      = (byte*)malloc(length_from_header(keylen, datalen));
    	memcpy(ret->key_, key, keylen);
    	ret->data_ = ret->key_ + keylen;  // need to set this even if delete, since it encodes the key length.
    	if(datalen != DELETE) {
    		memcpy(ret->data_, data, datalen);
    	}
    	ret->datalen_ = datalen;
    	return ret->sanity_check();
    }

    //format: key length _   data length _ key _ data
    byte * to_bytes() const {
    	byte *ret = (byte*)malloc(byte_length());
    	((len_t*)ret)[0] = keylen();
    	((len_t*)ret)[1] = datalen_;
    	memcpy(((len_t*)ret)+2, key_, length_from_header(keylen(), datalen_));
        return ret;
    }

    const byte* get_bytes(len_t *keylen, len_t *datalen) const {
        *keylen  = this->keylen();
    	*datalen = datalen_;
    	return key_;
    }

    //format of buf: key _ data.  The caller needs to 'peel' off key length and data length for this call.
    static datatuple* from_bytes(len_t keylen, len_t datalen, byte* buf) {
    	datatuple *dt = (datatuple*) malloc(sizeof(datatuple));
    	dt->datalen_ = datalen;
    	dt->key_ = buf;
    	dt->data_ = dt->key_ + keylen;
    	return dt->sanity_check();
    }
    static datatuple* from_bytes(byte* buf) {
    	datatuple *dt = (datatuple*) malloc(sizeof(datatuple));
    	len_t keylen = ((len_t*)buf)[0];
    	dt->datalen_ = ((len_t*)buf)[1];
    	len_t buflen = length_from_header(keylen, dt->datalen_);
    	dt->key_ = (byte*)malloc(buflen);
    	memcpy(dt->key_,((len_t*)buf)+2,buflen);
    	dt->data_ = dt->key_ + keylen;

    	return dt->sanity_check();
    }

    static inline void freetuple(datatuple* dt) {
        free(dt->key_);
        free(dt);
    }

} datatuple;


#endif
