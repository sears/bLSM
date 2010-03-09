#ifndef _LOG_ITERATORS_H_
#define _LOG_ITERATORS_H_

template <class TUPLE>
class DataPage;

/////////////////////////////////////////////////////////////////

template <class TUPLE>
class diskTreeIterator
{

public:
    explicit diskTreeIterator(recordid tree);

    explicit diskTreeIterator(recordid tree,TUPLE &key);

    explicit diskTreeIterator(diskTreeComponent::internalNodes *tree);

    explicit diskTreeIterator(diskTreeComponent::internalNodes *tree,TUPLE &key);

    ~diskTreeIterator();
    
    TUPLE * next_callerFrees();
    
private:
    void init_iterators(TUPLE * key1, TUPLE * key2);
    inline void init_helper(TUPLE* key1);

  explicit diskTreeIterator() { abort(); }
  void operator=(diskTreeIterator & t) { abort(); }
  int operator-(diskTreeIterator & t) { abort(); }
    
private:
    recordid tree_; //root of the tree
    
    diskTreeComponent::internalNodes::iterator* lsmIterator_;
    
    pageid_t curr_pageid; //current page id
    DataPage<TUPLE>    *curr_page;   //current page
    typedef typename DataPage<TUPLE>::iterator DPITR_T;
    DPITR_T *dp_itr;
};

#endif
