#ifndef _LOG_ITERATORS_H_
#define _LOG_ITERATORS_H_

#include <assert.h>
#include <stasis/iterator.h>

#undef begin
#undef end

template <class MEMTREE, class TUPLE> class memTreeIterator;

template <class MEMTREE, class TUPLE>
const byte* toByteArray(memTreeIterator<MEMTREE,TUPLE> * const t);

template <class TUPLE>
class DataPage;

//////////////////////////////////////////////////////////////
// memTreeIterator
/////////////////////////////////////////////////////////////

template<class MEMTREE, class TUPLE>
class memTreeIterator{

private:
    typedef typename MEMTREE::const_iterator MTITER;
    
public:    
    memTreeIterator( MEMTREE *s )
        {
            it_ = s->begin();
            itend_ = s->end();
        }
    
    
    memTreeIterator( MTITER& it, MTITER& itend )
        {
            it_ = it;
            itend_ = itend;
        }
   
    explicit memTreeIterator(memTreeIterator &i)
        {
            it_ = i.it_;
            itend_ = i.itend_;
        }

    const TUPLE& operator* ()
        {
            return *it_;
        }

    void seekEnd()
        {
            it_ = itend_;
        }

    
    memTreeIterator * end()
        {
            return new memTreeIterator<MEMTREE,TUPLE>(itend_,itend_);
        }
    
    inline bool operator==(const memTreeIterator &o) const {
        return it_ == o.it_;
    }
    inline bool operator!=(const memTreeIterator &o) const {
        return !(*this == o);
    }
    inline void operator++() {
        ++it_;
    }
    inline void operator--() {
        --it_;
    }

    inline int  operator-(memTreeIterator &i) {
        return it_ - i.it_;
    }

    inline void operator=(memTreeIterator const &i)
        {
            it_ = i.it_;
            itend_ = i.itend_;
        }

public:
    typedef MEMTREE* handle;
    
private:

    MTITER it_;
    MTITER itend_;
    
    friend const byte* toByteArray<MEMTREE,TUPLE>(memTreeIterator<MEMTREE,TUPLE> * const t);

};

template <class MEMTREE, class TUPLE>
const byte* toByteArray(memTreeIterator<MEMTREE,TUPLE> * const t)
{
    return (*(t->it_)).to_bytes();//toByteArray();
}

/////////////////////////////////////////////////////////////////

/**
   Scans through an LSM tree's leaf pages, each tuple in the tree, in
   order.  This iterator is designed for maximum forward scan
   performance, and does not support all STL operations.
**/
template <class TUPLE>
class treeIterator
{

 public:
    //  typedef recordid handle;
    class treeIteratorHandle
    {
    public:
        treeIteratorHandle() : r_(NULLRID) {}
        treeIteratorHandle(const recordid r) : r_(r) {}
        
        treeIteratorHandle * operator=(const recordid &r) {
            r_ = r;
            return this;
        }
        
        recordid r_;
    };
    
    typedef treeIteratorHandle* handle;

    explicit treeIterator(recordid tree);

    explicit treeIterator(recordid tree,TUPLE &key);
    
    //explicit treeIterator(treeIteratorHandle* tree, TUPLE& key);
    
    //explicit treeIterator(treeIteratorHandle* tree);
    
    //explicit treeIterator(treeIterator& t);

    ~treeIterator();
    
    TUPLE * getnext();

    //void advance(int count=1);
    
private:
    inline void init_helper();

  explicit treeIterator() { abort(); }
  void operator=(treeIterator & t) { abort(); }
  int operator-(treeIterator & t) { abort(); }    
    
private:
    recordid tree_; //root of the tree
    
    lladdIterator_t * lsmIterator_; //logtree iterator
    
    pageid_t curr_pageid; //current page id
    DataPage<TUPLE>    *curr_page;   //current page
    typedef typename DataPage<TUPLE>::RecordIterator DPITR_T;
    DPITR_T *dp_itr;
    TUPLE    *curr_tuple;  //current tuple
};




#endif

