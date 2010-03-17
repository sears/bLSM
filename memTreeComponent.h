#ifndef _MEMTREECOMPONENT_H_
#define _MEMTREECOMPONENT_H_
#include <set>

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
      pthread_mutex_lock(mut_);
      if(s_->begin() == s_->end()) {
        next_ret_ = NULL;
      } else {
        next_ret_ = (*s_->begin())->create_copy();  // the create_copy() calls have to happen before we release mut_...
      }
      pthread_mutex_unlock(mut_);
    }
    revalidatingIterator( rbtree_t *s, pthread_mutex_t * rb_mut, TUPLE *&key ) : s_(s), mut_(rb_mut) {
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
      //      DEBUG("changing mem next ret = %s key = %s\n", next_ret_ ?  (const char*)next_ret_->key() : "NONE", key ? (const char*)key->key() : "NULL");
      pthread_mutex_unlock(mut_);
    }

    ~revalidatingIterator() {
      if(next_ret_) TUPLE::freetuple(next_ret_);
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
    explicit revalidatingIterator() { abort(); }
    void operator=(revalidatingIterator & t) { abort(); }
    int  operator-(revalidatingIterator & t) { abort(); }

    rbtree_t *s_;
    TUPLE * next_ret_;
    pthread_mutex_t * mut_;
  };
};

#endif //_MEMTREECOMPONENT_H_
