#ifndef _DATATUPLE_H_
#define _DATATUPLE_H_


typedef unsigned char uchar;

#include <string>

//#define byte unsigned char
typedef unsigned char byte;
#include <cstring>

//#include <stdio.h>
//#include <stdlib.h>
//#include <errno.h>

typedef struct datatuple
{
    typedef uchar* key_t;
    typedef uchar* data_t;
    static const size_t isize = sizeof(uint32_t);
    uint32_t *keylen;    //key length should be size of string + 1 for \n
    uint32_t *datalen; 
    key_t key;
    data_t data;

    //this is used by the stl set
    bool operator() (const datatuple& lhs, const datatuple& rhs) const
        {
            //std::basic_string<uchar> s1(lhs.key);
            //std::basic_string<uchar> s2(rhs.key);
            return strcmp((char*)lhs.key,(char*)rhs.key) < 0;
            //return (*((int32_t*)lhs.key)) <= (*((int32_t*)rhs.key));
        }

    void clone(const datatuple& tuple) {
		//create a copy

    	byte * arr = (byte*) malloc(tuple.byte_length());

		keylen = (uint32_t*) arr;
		*keylen = *tuple.keylen;
		datalen = (uint32_t*) (arr+isize);
		*datalen = *tuple.datalen;
		key = (datatuple::key_t) (arr+isize+isize);
		memcpy((byte*)key, (byte*)tuple.key, *keylen);
		if(!tuple.isDelete())
		{
			data = (datatuple::data_t) (arr+isize+isize+ *keylen);
			memcpy((byte*)data, (byte*)tuple.data, *datalen);
		}
		else
			data = 0;
    }

    /**
     * return -1 if k1 < k2
     * 0 if k1 == k2
     * 1 of k1 > k2
    **/
    static int compare(const key_t k1,const key_t k2)
        {            
            //for char* ending with \0
            return strcmp((char*)k1,(char*)k2);

            //for int32_t
            //printf("%d\t%d\n",(*((int32_t*)k1)) ,(*((int32_t*)k2)));
            //return (*((int32_t*)k1)) <= (*((int32_t*)k2));
        }

    void setDelete()
        {
            *datalen = UINT_MAX;
        }

    inline bool isDelete() const
        {
            return *datalen == UINT_MAX;
        }

    static std::string key_to_str(const byte* k)
        {
            //for strings
            return std::string((char*)k);
            //for int
            /*
            std::ostringstream ostr;
            ostr << *((int32_t*)k);            
            return ostr.str();
            */
        }

    //returns the length of the byte array representation
    int32_t byte_length() const{
        static const size_t isize = sizeof(uint32_t);
        if(isDelete())
            return isize + *keylen + isize; 
        else
            return isize + *keylen + isize + (*datalen);
    }

    //format: key length _   data length _ key _ data
    byte * to_bytes() const {
        static const size_t isize = sizeof(uint32_t);
        byte * ret;
        if(!isDelete())
            ret = (byte*) malloc(isize + *keylen + isize + *datalen);
        else
            ret = (byte*) malloc(isize + *keylen + isize);
        
        memcpy(ret, (byte*)(keylen), isize);        
        memcpy(ret+isize, (byte*)(datalen), isize);
        memcpy(ret+isize+isize, key, *keylen);
        if(!isDelete())
            memcpy(ret+isize+isize+*keylen, data, *datalen);
        return ret;
    }

    //does not copy the data again
    //just sets the pointers in the datatuple to
    //right positions in the given arr
    
    static datatuple* from_bytes(const byte * arr)
        {
            static const size_t isize = sizeof(uint32_t);
            datatuple *dt = (datatuple*) malloc(sizeof(datatuple));

            dt->keylen = (uint32_t*) arr;
            dt->datalen = (uint32_t*) (arr+isize);
            dt->key = (key_t) (arr+isize+isize);
            if(!dt->isDelete())
                dt->data = (data_t) (arr+isize+isize+ *(dt->keylen));
            else
                dt->data = 0;

            return dt;
        }
    /*
    static datatuple form_tuple(const byte * arr)
        {
            static const size_t isize = sizeof(uint32_t);
            datatuple dt;

            dt.keylen = (uint32_t*) arr;
            dt.datalen = (uint32_t*) (arr+isize);
            dt.key = (key_t) (arr+isize+isize);
            if(!dt.isDelete())
                dt.data = (data_t) (arr+isize+isize+ *(dt.keylen));
            else
                dt.data = 0;

            return dt;
        }
    */
    
    byte * get_key() { return (byte*) key; }
    byte * get_data() { return (byte*) data; }

    //releases only the tuple
    static void release(datatuple *dt)
        {
            free(dt);
        }
    
} datatuple;


#endif
