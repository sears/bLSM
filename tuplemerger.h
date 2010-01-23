#ifndef _TUPLE_MERGER_H_
#define _TUPLE_MERGER_H_

struct datatuple;

typedef datatuple* (*merge_fn_t) (datatuple*, datatuple *);

datatuple* append_merger(datatuple *t1, datatuple *t2);

datatuple* replace_merger(datatuple *t1, datatuple *t2);


class tuplemerger
{

public:

    tuplemerger(merge_fn_t merge_fp) 
        {
            this->merge_fp = merge_fp;
        }

    
    datatuple* merge(datatuple *t1, datatuple *t2);

private:

    merge_fn_t merge_fp;

};



#endif
