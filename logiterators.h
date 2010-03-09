#ifndef _LOG_ITERATORS_H_
#define _LOG_ITERATORS_H_

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
    		next_ret_ = (*s_->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
    	}
    	pthread_mutex_unlock(mut_);
    }
    changingMemTreeIterator( MEMTREE *s, pthread_mutex_t * rb_mut, TUPLE *&key ) : s_(s), mut_(rb_mut) {
    	pthread_mutex_lock(mut_);
	if(key) {
	  if(s_->find(key) != s_->end()) {
	    next_ret_ = (*(s_->find(key)))->create_copy();
	  } else if(s_->upper_bound(key) != s_->end()) {
	    next_ret_ = (*(s_->upper_bound(key)))->create_copy();
	  } else {
	    next_ret_ = NULL;
	  }
	} else {
	  if(s_->begin() == s_->end()) {
    		next_ret_ = NULL;
	  } else {
	    next_ret_ = (*s_->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
	  }
	}
	DEBUG("changing mem next ret = %s key = %s\n", next_ret_ ?  (const char*)next_ret_->key() : "NONE", key ? (const char*)key->key() : "NULL");
    	pthread_mutex_unlock(mut_);
    }

    ~changingMemTreeIterator() {
    	if(next_ret_) datatuple::freetuple(next_ret_);
    }

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
    inline void init_helper(TUPLE* key1);

  explicit diskTreeIterator() { abort(); }
  void operator=(diskTreeIterator & t) { abort(); }
  int operator-(diskTreeIterator & t) { abort(); }
    
private:
    recordid tree_; //root of the tree
    
    diskTreeComponent::iterator* lsmIterator_;
    
    pageid_t curr_pageid; //current page id
    DataPage<TUPLE>    *curr_page;   //current page
    typedef typename DataPage<TUPLE>::iterator DPITR_T;
    DPITR_T *dp_itr;
};

#endif
