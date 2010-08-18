#ifndef _MEMTREECOMPONENT_H_
#define _MEMTREECOMPONENT_H_
#include <set>
#include <assert.h>

template<class TUPLE>
class memTreeComponent {
public:
  typedef std::set<TUPLE*, TUPLE> rbtree_t;
  typedef rbtree_t* rbtree_ptr_t;

  static void tearDownTree(rbtree_ptr_t t);

///////////////////////////////////////////////////////////////
// Plain iterator; cannot cope with changes to underlying tree
///////////////////////////////////////////////////////////////

  class iterator
  {
  private:
    typedef typename rbtree_t::const_iterator MTITER;

  public:
    iterator( rbtree_t *s )
      : first_(true),
	done_(s == NULL) {
      init_iterators(s, NULL, NULL);
    }

    iterator( rbtree_t *s, TUPLE *&key )
      : first_(true), done_(s == NULL) {
      init_iterators(s, key, NULL);
    }

    ~iterator() {
      delete it_;
      delete itend_;
    }

    TUPLE* next_callerFrees() {
      if(done_) { return NULL; }
      if(first_) { first_ = 0;} else { (*it_)++; }
      if(*it_==*itend_) { done_= true; return NULL; }

      return (*(*it_))->create_copy();
    }


  private:
    void init_iterators(rbtree_t * s, TUPLE * key1, TUPLE * key2) {
      if(s) {
        it_    = key1 ? new MTITER(s->find(key1))  : new MTITER(s->begin());
        itend_ = key2 ? new MTITER(s->find(key2)) : new MTITER(s->end());
        if(*it_ == *itend_) { done_ = true; }
        if(key1) {
          if(done_) {
            //	    DEBUG("memtree opened at eot\n");
          } else {
            //	    DEBUG("memtree opened key = %s\n", (**it_)->key());
          }
        }
      } else {
        it_ = NULL;
        itend_ = NULL;
      }
    }
    explicit iterator() { abort(); }
    void operator=(iterator & t) { abort(); }
    int operator-(iterator & t) { abort(); }
  private:
    bool first_;
    bool done_;
    MTITER *it_;
    MTITER *itend_;
  };

  ///////////////////////////////////////////////////////////////
  // Revalidating iterator; automatically copes with changes to underlying tree
  ///////////////////////////////////////////////////////////////


  class revalidatingIterator
  {
  private:
    typedef typename rbtree_t::const_iterator MTITER;

  public:
    revalidatingIterator( rbtree_t *s, pthread_mutex_t * rb_mut ) : s_(s), mut_(rb_mut) {
      if(mut_) pthread_mutex_lock(mut_);
      if(s_->begin() == s_->end()) {
        next_ret_ = NULL;
      } else {
        next_ret_ = (*s_->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
      }
      if(mut_) pthread_mutex_unlock(mut_);
    }
    revalidatingIterator( rbtree_t *s, pthread_mutex_t * rb_mut, TUPLE *&key ) : s_(s), mut_(rb_mut) {
      if(mut_) pthread_mutex_lock(mut_);
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
      //      DEBUG("changing mem next ret = %s key = %s\n", next_ret_ ?  (const char*)next_ret_->key() : "NONE", key ? (const char*)key->key() : "NULL");
      if(mut_) pthread_mutex_unlock(mut_);
    }

    ~revalidatingIterator() {
      if(next_ret_) TUPLE::freetuple(next_ret_);
    }

    TUPLE* next_callerFrees() {
      if(mut_) pthread_mutex_lock(mut_);
      TUPLE * ret = next_ret_;
      if(next_ret_) {
        if(s_->upper_bound(next_ret_) == s_->end()) {
          next_ret_ = 0;
        } else {
          next_ret_ = (*s_->upper_bound(next_ret_))->create_copy();
        }
      }
      if(mut_) pthread_mutex_unlock(mut_);
      return ret;
    }

  private:
    explicit revalidatingIterator() { abort(); }
    void operator=(revalidatingIterator & t) { abort(); }
    int  operator-(revalidatingIterator & t) { abort(); }

    rbtree_t *s_;
    TUPLE * next_ret_;
    pthread_mutex_t * mut_;
  };

  ///////////////////////////////////////////////////////////////
  // Revalidating iterator; automatically copes with changes to underlying tree
  ///////////////////////////////////////////////////////////////


  class batchedRevalidatingIterator
  {
  private:
    typedef typename rbtree_t::const_iterator MTITER;


    void populate_next_ret_impl(std::_Rb_tree_const_iterator<TUPLE*>/*MTITER*/ it) {
      num_batched_ = 0;
      cur_off_ = 0;
      while(it != s_->end() && num_batched_ < batch_size_) {
        next_ret_[num_batched_] = (*it)->create_copy();
        num_batched_++;
        it++;
      }
    }
    void populate_next_ret(TUPLE *key=NULL) {
      if(cur_off_ == num_batched_) {
        if(mut_) pthread_mutex_lock(mut_);
        if(key) {
          populate_next_ret_impl(s_->upper_bound(key));
        } else {
          populate_next_ret_impl(s_->begin());
        }
        if(mut_) pthread_mutex_unlock(mut_);
      }
    }

  public:
    batchedRevalidatingIterator( rbtree_t *s, int batch_size, pthread_mutex_t * rb_mut ) : s_(s), batch_size_(batch_size), num_batched_(batch_size), cur_off_(batch_size), mut_(rb_mut) {
      next_ret_ = (TUPLE**)malloc(sizeof(next_ret_[0]) * batch_size_);
      populate_next_ret();
/*      if(mut_) pthread_mutex_lock(mut_);
      if(s_->begin() == s_->end()) {
        next_ret_ = NULL;
      } else {
        next_ret_ = (*s_->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
      }
      if(mut_) pthread_mutex_unlock(mut_); */
    }
    batchedRevalidatingIterator( rbtree_t *s, int batch_size, pthread_mutex_t * rb_mut, TUPLE *&key ) : s_(s), batch_size_(batch_size), num_batched_(batch_size), cur_off_(batch_size), mut_(rb_mut) {
      next_ret_ = (TUPLE**)malloc(sizeof(next_ret_[0]) * batch_size_);
      populate_next_ret(key);
/*      if(mut_) pthread_mutex_lock(mut_);
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
      //      DEBUG("changing mem next ret = %s key = %s\n", next_ret_ ?  (const char*)next_ret_->key() : "NONE", key ? (const char*)key->key() : "NULL");
      if(mut_) pthread_mutex_unlock(mut_); */
    }

    ~batchedRevalidatingIterator() {
      for(int i = cur_off_; i < num_batched_; i++) {
        TUPLE::freetuple(next_ret_[cur_off_]);
      }
      free(next_ret_);
//      if(next_ret_) TUPLE::freetuple(next_ret_);
    }

    TUPLE* next_callerFrees() {
/*      if(mut_) pthread_mutex_lock(mut_);
      TUPLE * ret = next_ret_;
      if(next_ret_) {
        if(s_->upper_bound(next_ret_) == s_->end()) {
          next_ret_ = 0;
        } else {
          next_ret_ = (*s_->upper_bound(next_ret_))->create_copy();
        }
      }
      if(mut_) pthread_mutex_unlock(mut_); */
      if(cur_off_ == num_batched_) { return NULL; } // the last thing we did is call populate_next_ret_(), which only leaves us in this state at the end of the iterator.
      TUPLE * ret = next_ret_[cur_off_];
      cur_off_++;
      populate_next_ret(ret);
      return ret;
    }

  private:
    explicit batchedRevalidatingIterator() { abort(); }
    void operator=(batchedRevalidatingIterator & t) { abort(); }
    int  operator-(batchedRevalidatingIterator & t) { abort(); }

    rbtree_t *s_;
    TUPLE ** next_ret_;
    int batch_size_;
    int num_batched_;
    int cur_off_;
    pthread_mutex_t * mut_;
  };

};

#endif //_MEMTREECOMPONENT_H_
