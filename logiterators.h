#ifndef _LOG_ITERATORS_H_
#define _LOG_ITERATORS_H_

#include <assert.h>
#include <stasis/iterator.h>

#undef begin
#undef end

template <class TUPLE>
class DataPage;

template <class MEMTREE, class TUPLE>
class changingMemTreeIterator
{
private:
    typedef typename MEMTREE::const_iterator MTITER;

public:
    changingMemTreeIterator( MEMTREE *s, pthread_mutex_t * rb_mut ) : s_(s), mut_(rb_mut) {
    	pthread_mutex_lock(mut_);
    	if(s_->begin() == s_->end()) {
    		next_ret_ = NULL;
    	} else {
    		next_ret_ = (*s->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
    	}
    	pthread_mutex_unlock(mut_);
    }
    changingMemTreeIterator( MEMTREE *s, pthread_mutex_t * rb_mut, TUPLE *&key ) {
    	pthread_mutex_lock(mut_);
    	if(s_->find(key) != s_->end()) {
    		next_ret_ = (*(s_->find(key)))->create_copy();
    	} else if(s_->upper_bound(key) != s->end()) {
    		next_ret_ = (*(s_->upper_bound(key)))->create_copy();
    	} else {
    		next_ret_ = NULL;
    	}
    	pthread_mutex_unlock(mut_);
    }

    ~changingMemTreeIterator() { if(next_ret_) delete next_ret_; }

    TUPLE* getnext() {
    	pthread_mutex_lock(mut_);
    	TUPLE * ret = next_ret_;
    	if(next_ret_) {
    		if(s_->upper_bound(next_ret_) == s_->end()) {
    			next_ret_ = 0;
    		} else {
    			next_ret_ = (*s_->upper_bound(next_ret_))->create_copy();
    		}
    	}
    	pthread_mutex_unlock(mut_);
    	return ret;
    }


private:
  explicit changingMemTreeIterator() { abort(); }
  void operator=(changingMemTreeIterator & t) { abort(); }
  int operator-(changingMemTreeIterator & t) { abort(); }
private:
  MEMTREE *s_;
  TUPLE * next_ret_;
  pthread_mutex_t * mut_;
};



//////////////////////////////////////////////////////////////
// memTreeIterator
/////////////////////////////////////////////////////////////

template <class MEMTREE, class TUPLE>
class memTreeIterator
{
private:
    typedef typename MEMTREE::const_iterator MTITER;

public:
    memTreeIterator( MEMTREE *s )
    : first_(true),
      done_(s == NULL) {
    	init_iterators(s, NULL, NULL);
    }

    memTreeIterator( MEMTREE *s, TUPLE *&key )
    : first_(true), done_(s == NULL) {
    	init_iterators(s, key, NULL);
    }

    ~memTreeIterator() {
    	delete it_;
    	delete itend_;
    }

    TUPLE* getnext() {
       	if(done_) { return NULL; }
       	if(first_) { first_ = 0;} else { (*it_)++; }
		if(*it_==*itend_) { done_= true; return NULL; }

    	return (*(*it_))->create_copy();
    }


private:
  void init_iterators(MEMTREE * s, TUPLE * key1, TUPLE * key2) {
  	if(s) {
  		it_    = key1 ? new MTITER(s->find(key1))  : new MTITER(s->begin());
  		itend_ = key2 ? new MTITER(s->find(key2)) : new MTITER(s->end());
  	} else {
  		it_ = NULL;
  		itend_ = NULL;
  	}
  }
  explicit memTreeIterator() { abort(); }
  void operator=(memTreeIterator & t) { abort(); }
  int operator-(memTreeIterator & t) { abort(); }
private:
  bool first_;
  bool done_;
  MTITER *it_;
  MTITER *itend_;
};

/////////////////////////////////////////////////////////////////

template <class TUPLE>
class diskTreeIterator
{

public:
    explicit diskTreeIterator(recordid tree);

    explicit diskTreeIterator(recordid tree,TUPLE &key);

    explicit diskTreeIterator(diskTreeComponent *tree);

    explicit diskTreeIterator(diskTreeComponent *tree,TUPLE &key);

    ~diskTreeIterator();
    
    TUPLE * getnext();
    
private:
    void init_iterators(TUPLE * key1, TUPLE * key2);
    inline void init_helper();

  explicit diskTreeIterator() { abort(); }
  void operator=(diskTreeIterator & t) { abort(); }
  int operator-(diskTreeIterator & t) { abort(); }
    
private:
    recordid tree_; //root of the tree
    
    lladdIterator_t * lsmIterator_; //diskTreeComponent iterator
    
    pageid_t curr_pageid; //current page id
    DataPage<TUPLE>    *curr_page;   //current page
    typedef typename DataPage<TUPLE>::RecordIterator DPITR_T;
    DPITR_T *dp_itr;
    TUPLE    *curr_tuple;  //current tuple
};

#endif

