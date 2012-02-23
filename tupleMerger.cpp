/*
 * tuplemerger.cpp
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
 */
#include "tupleMerger.h"
#include "bLSM.h"

// t2 is the newer tuple.
// we return deletes here.  our caller decides what to do with them.
dataTuple* tupleMerger::merge(const dataTuple *t1, const dataTuple *t2)
{
  if(!(t1->isDelete() || t2->isDelete())) {
    return (*merge_fp)(t1,t2);
  } else {
    // if there is at least one tombstone, we return t2 intact.
    // t1 tombstone -> ignore it, and return t2.
    // t2 tombstone -> return a tombstone (like t2).
    return t2->create_copy();
  }
}
/**
 * appends the data in t2 to data from t1
 * 
 * deletes are handled by the tuplemerger::merge function
 * so here neither t1 nor t2 is a delete datatuple
 **/
dataTuple* append_merger(const dataTuple *t1, const dataTuple *t2)
{
	assert(!(t1->isDelete() || t2->isDelete()));
    len_t rawkeylen = t1->rawkeylen();
    len_t datalen = t1->datalen() + t2->datalen();
    byte * data = (byte*)malloc(datalen);
    memcpy(data, t1->data(), t1->datalen());
    memcpy(data + t1->datalen(), t2->data(), t2->datalen());

	return dataTuple::create(t1->rawkey(), rawkeylen, data, datalen);
}

/**
 * replaces the data with data from t2
 * 
 * deletes are handled by the tuplemerger::merge function
 * so here neither t1 nor t2 is a delete datatuple
 **/
dataTuple* replace_merger(const dataTuple *t1, const dataTuple *t2)
{
	return t2->create_copy();
}
