#include "tuplemerger.h"
#include "logstore.h"

// XXX make the imputs 'const'
// XXX test / reason about this...
datatuple* tuplemerger::merge(datatuple *t1, datatuple *t2)
{
    assert(!t1->isDelete() || !t2->isDelete()); //both cannot be delete

    datatuple *t;

    if(t1->isDelete()) //delete - t2
    {
        t = t2->create_copy();
    }
    else if(t2->isDelete())
    {
        t = t2->create_copy();
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

	assert(!(t1->isDelete() || t2->isDelete()));
    datatuple::len_t keylen = t1->keylen();
    datatuple::len_t datalen = t1->datalen() + t2->datalen();
    byte * data = (byte*)malloc(datalen);
    memcpy(data, t1->data(), t1->datalen());
    memcpy(data + t1->datalen(), t2->data(), t2->datalen());

	return datatuple::create(t1->key(), keylen, data, datalen);
}

/**
 * replaces the data with data from t2
 * 
 * deletes are handled by the tuplemerger::merge function
 * so here neither t1 nor t2 is a delete datatuple
 **/
datatuple* replace_merger(datatuple *t1, datatuple *t2)
{
	return t2->create_copy();
}
