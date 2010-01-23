#include "tuplemerger.h"
#include "logstore.h"

datatuple* tuplemerger::merge(datatuple *t1, datatuple *t2)
{
    assert(!t1->isDelete() || !t2->isDelete()); //both cannot be delete

    datatuple *t;

    if(t1->isDelete()) //delete - t2
    {
        t = datatuple::from_bytes(t2->to_bytes());
    }
    else if(t2->isDelete())
    {
        t = datatuple::from_bytes(t2->to_bytes());
    }
    else //neither is a delete
    {
        t = (*merge_fp)(t1,t2);
    }
    
    return t;
    
}

/**
 * appends the data in t2 to data from t1
 * 
 * deletes are handled by the tuplemerger::merge function
 * so here neither t1 nor t2 is a delete datatuple
 **/
datatuple* append_merger(datatuple *t1, datatuple *t2)
{
    static const size_t isize = sizeof(uint32_t);
    struct datatuple *t = (datatuple*) malloc(sizeof(datatuple));

    byte *arr = (byte*)malloc(t1->byte_length() + *t2->datalen);

    t->keylen = (uint32_t*) arr;
    *(t->keylen) = *(t1->keylen);

    t->datalen = (uint32_t*) (arr+isize);
    *(t->datalen) = *(t1->datalen) + *(t2->datalen);
    
    t->key = (datatuple::key_t) (arr+isize+isize);
    memcpy((byte*)t->key, (byte*)t1->key, *(t1->keylen));
    
    t->data = (datatuple::data_t) (arr+isize+isize+ *(t1->keylen));
    memcpy((byte*)t->data, (byte*)t1->data, *(t1->datalen));
    memcpy(((byte*)t->data) + *(t1->datalen), (byte*)t2->data, *(t2->datalen));
        
    return t;

}

/**
 * replaces the data with data from t2
 * 
 * deletes are handled by the tuplemerger::merge function
 * so here neither t1 nor t2 is a delete datatuple
 **/
datatuple* replace_merger(datatuple *t1, datatuple *t2)
{
    static const size_t isize = sizeof(uint32_t);
    struct datatuple *t = (datatuple*) malloc(sizeof(datatuple));

    byte *arr = (byte*)malloc(t2->byte_length());

    t->keylen = (uint32_t*) arr;
    *(t->keylen) = *(t2->keylen);

    t->datalen = (uint32_t*) (arr+isize);
    *(t->datalen) = *(t2->datalen);
    
    t->key = (datatuple::key_t) (arr+isize+isize);
    memcpy((byte*)t->key, (byte*)t2->key, *(t2->keylen));
    
    t->data = (datatuple::data_t) (arr+isize+isize+ *(t2->keylen));
    memcpy((byte*)t->data, (byte*)t2->data, *(t2->datalen));
        
    return t;

}
