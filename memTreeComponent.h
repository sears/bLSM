#ifndef _MEMTREECOMPONENT_H_
#define _MEMTREECOMPONENT_H_
#include <set>
#include "datatuple.h"
template<class TUPLE>
class memTreeComponent {
public:
  typedef std::set<TUPLE*, TUPLE> rbtree_t;
  typedef rbtree_t* rbtree_ptr_t;

  static void tearDownTree(rbtree_ptr_t t);

//////////////////////////////////////////////////////////////
// memTreeIterator
/////////////////////////////////////////////////////////////

  class memTreeIterator
  {
  private:
    typedef typename rbtree_t::const_iterator MTITER;

  public:
    memTreeIterator( rbtree_t *s )
      : first_(true),
	done_(s == NULL) {
      init_iterators(s, NULL, NULL);
    }

    memTreeIterator( rbtree_t *s, TUPLE *&key )
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
};

#endif //_MEMTREECOMPONENT_H_
