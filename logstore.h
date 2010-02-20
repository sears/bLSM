#ifndef _LOGSTORE_H_
#define _LOGSTORE_H_

#undef end
#undef begin

#include <string>
#include <set>
#include <sstream>
#include <iostream>
#include <queue>
#include <vector>

#include "logserver.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <pthread.h>



#include <stasis/transactional.h>

#include <stasis/operations.h>
#include <stasis/bufferManager.h>
#include <stasis/allocationPolicy.h>
#include <stasis/blobManager.h>
#include <stasis/page.h>
#include <stasis/truncation.h>

#include "diskTreeComponent.h"

#include "datapage.h"
#include "tuplemerger.h"
#include "datatuple.h"

#include "logiterators.h"

typedef std::set<datatuple*, datatuple> rbtree_t;
typedef rbtree_t* rbtree_ptr_t;

#include "merger.h"

class logtable
{
public:
    logtable();
    ~logtable();

    //user access functions
    datatuple * findTuple(int xid, const datatuple::key_t key, size_t keySize);

    datatuple * findTuple_first(int xid, datatuple::key_t key, size_t keySize);
    
    void insertTuple(struct datatuple *tuple);

    //other class functions
    recordid allocTable(int xid);

    void flushTable();    
    
    static void tearDownTree(rbtree_ptr_t t);

    DataPage<datatuple>* insertTuple(int xid, datatuple *tuple,diskTreeComponent *ltree);

    datatuple * findTuple(int xid, const datatuple::key_t key, size_t keySize,  diskTreeComponent *ltree);

    inline recordid & get_table_rec(){return table_rec;}  // TODO This is called by merger.cpp for no good reason.  (remove the calls)
    
    inline uint64_t get_epoch() { return epoch; }


    inline diskTreeComponent * get_tree_c2(){return tree_c2;}
    inline diskTreeComponent * get_tree_c1(){return tree_c1;}
    inline diskTreeComponent * get_tree_c1_mergeable(){return tree_c1_mergeable;}

    inline void set_tree_c1(diskTreeComponent *t){tree_c1=t;                      epoch++; }
    inline void set_tree_c1_mergeable(diskTreeComponent *t){tree_c1_mergeable=t;  epoch++; }
    inline void set_tree_c2(diskTreeComponent *t){tree_c2=t;                      epoch++; }
    
    inline rbtree_ptr_t get_tree_c0(){return tree_c0;}
    inline rbtree_ptr_t get_tree_c0_mergeable(){return tree_c0_mergeable;}
    void set_tree_c0(rbtree_ptr_t newtree){tree_c0 = newtree;                     epoch++; }
    void set_tree_c0_mergeable(rbtree_ptr_t newtree){tree_c0_mergeable = newtree; epoch++; }

    int get_fixed_page_count(){return fixed_page_count;}
    void set_fixed_page_count(int count){fixed_page_count = count;}

    void setMergeData(logtable_mergedata * mdata) { this->mergedata = mdata;      epoch++; }
    logtable_mergedata* getMergeData(){return mergedata;}

    inline tuplemerger * gettuplemerger(){return tmerger;}
    
public:

    struct table_header {
        recordid c2_root;     //tree root record --> points to the root of the b-tree
        recordid c2_state;    //tree state --> describes the regions used by the index tree
        recordid c2_dp_state; //data pages state --> regions used by the data pages
        recordid c1_root;
        recordid c1_state;
        recordid c1_dp_state;
    };

    const static RegionAllocConf_t DATAPAGE_REGION_ALLOC_STATIC_INITIALIZER;

    logtable_mergedata * mergedata;
    rwl * header_lock;
    
    int64_t max_c0_size;

    inline bool is_still_running() { return still_running_; }
    inline void stop() {
    	still_running_ = false;
		// XXX must need to do other things!
    }

private:    
    recordid table_rec;
    struct table_header tbl_header;
    uint64_t epoch;
    diskTreeComponent *tree_c2; //big tree
    diskTreeComponent *tree_c1; //small tree
    diskTreeComponent *tree_c1_mergeable; //small tree: ready to be merged with c2
    rbtree_ptr_t tree_c0; // in-mem red black tree
    rbtree_ptr_t tree_c0_mergeable; // in-mem red black tree: ready to be merged with c1.

    int tsize; //number of tuples
    int64_t tree_bytes; //number of bytes

    
    //DATA PAGE SETTINGS
    int fixed_page_count;//number of pages in a datapage

    tuplemerger *tmerger;

    bool still_running_;
};

template<class ITRA, class ITRN, class TUPLE>
class mergeManyIterator {
public:
  explicit mergeManyIterator(ITRA* a, ITRN** iters, int num_iters, TUPLE*(*merge)(const TUPLE*,const TUPLE*), int (*cmp)(const TUPLE*,const TUPLE*)) :
    num_iters_(num_iters+1),
    first_iter_(a),
	iters_((ITRN**)malloc(sizeof(*iters_) * num_iters)),          // exactly the number passed in
    current_((TUPLE**)malloc(sizeof(*current_) * (num_iters_))),  // one more than was passed in
    last_iter_(-1),
    cmp_(cmp),
	merge_(merge),
	dups((int*)malloc(sizeof(*dups)*num_iters_))
	{
    current_[0] = first_iter_->getnext();
    for(int i = 1; i < num_iters_; i++) {
      iters_[i-1] = iters[i-1];
      current_[i] = iters_[i-1]->getnext();
    }
  }
  ~mergeManyIterator() {
    delete(first_iter_);
    for(int i = 0; i < num_iters_; i++) {
    	if(i != last_iter_) {
    		if(current_[i]) TUPLE::freetuple(current_[i]);
    	}
    }
    for(int i = 1; i < num_iters_; i++) {
      delete iters_[i-1];
    }
    free(current_);
    free(iters_);
    free(dups);
  }
  TUPLE * getnext() {
    int num_dups = 0;
    if(last_iter_ != -1) {
  	  // get the value after the one we just returned to the user
  	  //TUPLE::freetuple(current_[last_iter_]); // should never be null
      if(last_iter_ == 0) {
		  current_[last_iter_] = first_iter_->getnext();
      } else {
    	  current_[last_iter_] = iters_[last_iter_-1]->getnext();
      }
    }
    // find the first non-empty iterator.  (Don't need to special-case ITRA since we're looking at current.)
    int min = 0;
    while(min < num_iters_ && !current_[min]) {
      min++;
    }
    if(min == num_iters_) { return NULL; }
    // examine current to decide which tuple to return.
    for(int i = min+1; i < num_iters_; i++) {
      if(current_[i]) {
    	int res = cmp_(current_[min], current_[i]);
		if(res > 0) { // min > i
		  min = i;
		  num_dups = 0;
		} else if(res == 0) { // min == i
		  dups[num_dups] = i;
		  num_dups++;
		}
      }
    }
    TUPLE * ret;
    if(!merge_) {
    	ret = current_[min];
    } else {
    	// use merge function to build a new ret.
    	abort();
    }
    // advance the iterators that match the tuple we're returning.
    for(int i = 0; i < num_dups; i++) {
    	TUPLE::freetuple(current_[dups[i]]); // should never be null
    	current_[dups[i]] = iters_[dups[i]-1]->getnext();
    }
    last_iter_ = min; // mark the min iter to be advance at the next invocation of next().  This saves us a copy in the non-merging case.
    return ret;

  }
private:
  int      num_iters_;
  ITRA  *  first_iter_;
  ITRN  ** iters_;
  TUPLE ** current_;
  int      last_iter_;


  int  (*cmp_)(const TUPLE*,const TUPLE*);
  TUPLE*(*merge_)(const TUPLE*,const TUPLE*);

  // temporary variables initiaized once for effiency
  int * dups;
};

template<class TUPLE>
class logtableIterator {
public:
    explicit logtableIterator(logtable* ltable)
    : ltable(ltable),
      epoch(ltable->get_epoch()),
	  merge_it_(NULL),
	  last_returned(NULL),
      key(NULL) {
    	readlock(ltable->header_lock, 0);
    	validate();
		unlock(ltable->header_lock);
	}

    explicit logtableIterator(logtable* ltable,TUPLE *key)
    : ltable(ltable),
      epoch(ltable->get_epoch()),
	  merge_it_(NULL),
	  last_returned(NULL),
	  key(key) {
    	readlock(ltable->header_lock, 0);
		validate();
		unlock(ltable->header_lock);
	}

    ~logtableIterator() {
    	invalidate();
    }

    TUPLE * getnext() {
    	readlock(ltable->header_lock, 0);
    	revalidate();
    	last_returned = merge_it_->getnext();
    	unlock(ltable->header_lock);
    	return last_returned;

    }

private:
    inline void init_helper();

  explicit logtableIterator() { abort(); }
  void operator=(logtableIterator<TUPLE> & t) { abort(); }
  int operator-(logtableIterator<TUPLE> & t) { abort(); }

private:
  static const int C1           = 0;
  static const int C1_MERGEABLE = 1;
  static const int C2           = 2;
    logtable * ltable;
    uint64_t epoch;
    typedef mergeManyIterator<changingMemTreeIterator<rbtree_t, TUPLE>, memTreeIterator<rbtree_t, TUPLE>, TUPLE> inner_merge_it_t;
//    typedef mergeManyIterator<memTreeIterator<rbtree_t, TUPLE>, diskTreeIterator<TUPLE>, TUPLE> merge_it_t;
    typedef mergeManyIterator<inner_merge_it_t, diskTreeIterator<TUPLE>, TUPLE> merge_it_t;

    merge_it_t* merge_it_;

    TUPLE * last_returned;
    TUPLE * key;

    void revalidate() {
    	if(ltable->get_epoch() != epoch) {
    		TUPLE* delme = last_returned = last_returned->create_copy();
    		invalidate();
    		validate();
    		TUPLE::freetuple(delme);
    	}
    }


	void invalidate() {
		delete merge_it_;
	}
	void validate() {
	    changingMemTreeIterator<rbtree_t, TUPLE> * c0_it;
		memTreeIterator<rbtree_t, TUPLE>  * c0_mergeable_it[1];
		diskTreeIterator<TUPLE> * disk_it[3];
		epoch = ltable->get_epoch();
		if(last_returned) {
			c0_it              = new changingMemTreeIterator<rbtree_t, TUPLE>(ltable->get_tree_c0(), ltable->getMergeData()->rbtree_mut,  last_returned);
			c0_mergeable_it[0] = new memTreeIterator<rbtree_t, TUPLE>        (ltable->get_tree_c0_mergeable(),                            last_returned);
			disk_it[0]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1(),                                     *last_returned);
			disk_it[1]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1_mergeable(),                           *last_returned);
			disk_it[2]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c2(),                                     *last_returned);
		} else if(key) {
			c0_it              = new changingMemTreeIterator<rbtree_t, TUPLE>(ltable->get_tree_c0(), ltable->getMergeData()->rbtree_mut,  key);
			c0_mergeable_it[0] = new memTreeIterator<rbtree_t, TUPLE>        (ltable->get_tree_c0_mergeable(),                            key);
			disk_it[0]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1(),                                     *key);
			disk_it[1]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1_mergeable(),                           *key);
			disk_it[2]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c2(),                                     *key);
		} else {
			c0_it              = new changingMemTreeIterator<rbtree_t, TUPLE>(ltable->get_tree_c0(), ltable->getMergeData()->rbtree_mut  );
			c0_mergeable_it[0] = new memTreeIterator<rbtree_t, TUPLE>        (ltable->get_tree_c0_mergeable()                            );
			disk_it[0]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1()                                      );
			disk_it[1]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c1_mergeable()                            );
			disk_it[2]         = new diskTreeIterator<TUPLE>                 (ltable->get_tree_c2()                                      );
		}

		inner_merge_it_t * inner_merge_it =
			   new inner_merge_it_t(c0_it, c0_mergeable_it, 1, NULL, TUPLE::compare_obj);
		merge_it_ = new merge_it_t(inner_merge_it, disk_it, 3, NULL, TUPLE::compare_obj); // XXX Hardcodes comparator, and does not handle merges
	}

};

#endif
