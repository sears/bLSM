/*
 * merger.cpp
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
#ifndef _TUPLE_MERGER_H_
#define _TUPLE_MERGER_H_

struct datatuple;

typedef datatuple* (*merge_fn_t) (const datatuple*, const datatuple *);

datatuple* append_merger(const datatuple *t1, const datatuple *t2);
datatuple* replace_merger(const datatuple *t1, const datatuple *t2);


class tuplemerger
{

public:

    tuplemerger(merge_fn_t merge_fp) 
        {
            this->merge_fp = merge_fp;
        }

    
    datatuple* merge(const datatuple *t1, const datatuple *t2);

private:

    merge_fn_t merge_fp;

};



#endif
