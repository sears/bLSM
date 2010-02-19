#ifndef _LOG_ITERATORS_H_
#define _LOG_ITERATORS_H_

#include <assert.h>
#include <stasis/iterator.h>

#undef begin
#undef end

template <class TUPLE>
class DataPage;

//////////////////////////////////////////////////////////////
// memTreeIterator
/////////////////////////////////////////////////////////////

template <class MEMTREE, class TUPLE>
class memTreeIterator
{
private:
    typedef typename MEMTREE::const_iterator MTITER;

public:
    memTreeIterator( MEMTREE *s ) : first_(true), done_(false), it_(s->begin()), itend_(s->end()) { }
    memTreeIterator( MEMTREE *s, TUPLE &key ) : first_(true), done_(false), it_(s->find(key)), itend_(s->end()) { }

    ~memTreeIterator() { }

    TUPLE* getnext() {
       	if(done_) { return NULL; }
       	if(first_) { first_ = 0;} else { it_++; }
		if(it_==itend_) { done_= true; return NULL; }

    	return (*it_)->create_copy();
    }


private:
  explicit memTreeIterator() { abort(); }
  void operator=(memTreeIterator & t) { abort(); }
  int operator-(memTreeIterator & t) { abort(); }
private:
  bool first_;
  bool done_;
  MTITER it_;
  MTITER itend_;
};

/////////////////////////////////////////////////////////////////

template <class TUPLE>
class treeIterator
{

public:
    explicit treeIterator(recordid tree);

    explicit treeIterator(recordid tree,TUPLE &key);
    
    ~treeIterator();
    
    TUPLE * getnext();
    
private:
    inline void init_helper();

  explicit treeIterator() { abort(); }
  void operator=(treeIterator & t) { abort(); }
  int operator-(treeIterator & t) { abort(); }
    
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

