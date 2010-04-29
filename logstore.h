#ifndef _LOGSTORE_H_
#define _LOGSTORE_H_

#include <stasis/common.h>
#undef try
#undef end

#include <vector>

#include "diskTreeComponent.h"
#include "memTreeComponent.h"

#include "tuplemerger.h"

struct logtable_mergedata;

template<class TUPLE>
class logtable {
public:

  class iterator;

  // We want datapages to be as small as possible, assuming they don't force an extra seek to traverse the bottom level of internal nodes.
  // Internal b-tree mem requirements:
  //  - Assume keys are small (compared to stasis pages) so we can ignore all but the bottom level of the tree.
  //
  //  |internal nodes| ~= (|key| * |tree|) / (datapage_size * |stasis PAGE_SIZE|)
  //
  // Plugging in the numbers today:
  //
  //  6GB ~= 100B * 500 GB / (datapage_size * 4KB)
  //  (100B * 500GB) / (6GB * 4KB) = 2.035
  logtable(pageid_t internal_region_size = 1000, pageid_t datapage_region_size = 10000, pageid_t datapage_size = 2);

    ~logtable();

    //user access functions
    datatuple * findTuple(int xid, const datatuple::key_t key, size_t keySize);

    datatuple * findTuple_first(int xid, datatuple::key_t key, size_t keySize);
    
    void insertTuple(struct datatuple *tuple);

    //other class functions
    recordid allocTable(int xid);
    void openTable(int xid, recordid rid);
    void flushTable();    

    static void init_stasis();
    static void deinit_stasis();

    inline uint64_t get_epoch() { return epoch; }

    void registerIterator(iterator * it);
    void forgetIterator(iterator * it);
    void bump_epoch() ;

    inline diskTreeComponent * get_tree_c2(){return tree_c2;}
    inline diskTreeComponent * get_tree_c1(){return tree_c1;}
    inline diskTreeComponent * get_tree_c1_mergeable(){return tree_c1_mergeable;}

    inline void set_tree_c1(diskTreeComponent *t){tree_c1=t;                      bump_epoch(); }
    inline void set_tree_c1_mergeable(diskTreeComponent *t){tree_c1_mergeable=t;  bump_epoch(); }
    inline void set_tree_c2(diskTreeComponent *t){tree_c2=t;                      bump_epoch(); }
    
    inline memTreeComponent<datatuple>::rbtree_ptr_t get_tree_c0(){return tree_c0;}
    inline memTreeComponent<datatuple>::rbtree_ptr_t get_tree_c0_mergeable(){return tree_c0_mergeable;}
    void set_tree_c0(memTreeComponent<datatuple>::rbtree_ptr_t newtree){tree_c0 = newtree;                     bump_epoch(); }
    void set_tree_c0_mergeable(memTreeComponent<datatuple>::rbtree_ptr_t newtree){tree_c0_mergeable = newtree; bump_epoch(); }

    void update_persistent_header(int xid);

    void setMergeData(logtable_mergedata * mdata);
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

    logtable_mergedata * mergedata;
    rwl * header_lock;
    
    int64_t max_c0_size;

    mergeManager * merge_mgr;

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
    memTreeComponent<datatuple>::rbtree_ptr_t tree_c0; // in-mem red black tree
    memTreeComponent<datatuple>::rbtree_ptr_t tree_c0_mergeable; // in-mem red black tree: ready to be merged with c1.

    int tsize; //number of tuples
    int64_t tree_bytes; //number of bytes

    //DATA PAGE SETTINGS
    pageid_t internal_region_size; // in number of pages
    pageid_t datapage_region_size; // "
    pageid_t datapage_size;        // "

    tuplemerger *tmerger;

    std::vector<iterator *> its;

    bool still_running_;
public:

    template<class ITRA, class ITRN>
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
          current_[i] = iters_[i-1] ? iters_[i-1]->next_callerFrees() : NULL;
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
      TUPLE * peek() {
          TUPLE * ret = getnext();
          last_iter_ = -1; // don't advance iterator on next peek() or getnext() call.
          return ret;
      }
      TUPLE * getnext() {
        int num_dups = 0;
        if(last_iter_ != -1) {
          // get the value after the one we just returned to the user
          //TUPLE::freetuple(current_[last_iter_]); // should never be null
          if(last_iter_ == 0) {
              current_[last_iter_] = first_iter_->getnext();
          } else if(last_iter_ != -1){
              current_[last_iter_] = iters_[last_iter_-1]->next_callerFrees();
          } else {
              // last call was 'peek'
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
            // XXX use merge function to build a new ret.
            abort();
        }
        // advance the iterators that match the tuple we're returning.
        for(int i = 0; i < num_dups; i++) {
            TUPLE::freetuple(current_[dups[i]]); // should never be null
            current_[dups[i]] = iters_[dups[i]-1]->next_callerFrees();
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


    class iterator {
  public:
      explicit iterator(logtable* ltable)
      : ltable(ltable),
        epoch(ltable->get_epoch()),
        merge_it_(NULL),
        last_returned(NULL),
        key(NULL),
        valid(false) {
        writelock(ltable->header_lock, 0);
        ltable->registerIterator(this);
        validate();
        unlock(ltable->header_lock);
      }

      explicit iterator(logtable* ltable,TUPLE *key)
      : ltable(ltable),
        epoch(ltable->get_epoch()),
        merge_it_(NULL),
        last_returned(NULL),
        key(key),
        valid(false)
      {
        writelock(ltable->header_lock, 0);
        ltable->registerIterator(this);
        validate();
        unlock(ltable->header_lock);
      }

      ~iterator() {
        writelock(ltable->header_lock,0);
        ltable->forgetIterator(this);
        invalidate();
        if(last_returned) TUPLE::freetuple(last_returned);
        unlock(ltable->header_lock);
      }
  private:
      TUPLE * getnextHelper() {
          TUPLE * tmp = merge_it_->getnext();
          if(last_returned && tmp) {
              assert(TUPLE::compare(last_returned->key(), last_returned->keylen(), tmp->key(), tmp->keylen()) < 0);
              TUPLE::freetuple(last_returned);
          }
          last_returned = tmp;
          return last_returned;
      }
  public:
      TUPLE * getnextIncludingTombstones() {
          readlock(ltable->header_lock, 0);
          revalidate();
          TUPLE * ret = getnextHelper();
          unlock(ltable->header_lock);
          return ret ? ret->create_copy() : NULL;
      }

      TUPLE * getnext() {
          readlock(ltable->header_lock, 0);
          revalidate();
          TUPLE * ret;
          while((ret = getnextHelper()) && ret->isDelete()) { }  // getNextHelper handles its own memory.
          unlock(ltable->header_lock);
          return ret ? ret->create_copy() : NULL; // XXX hate making copy!  Caller should not manage our memory.
      }

      void invalidate() {
        assert(!trywritelock(ltable->header_lock,0));
        if(valid) {
          delete merge_it_;
          merge_it_ = NULL;
          valid = false;
        }
      }

  private:
      inline void init_helper();

    explicit iterator() { abort(); }
    void operator=(iterator & t) { abort(); }
    int operator-(iterator & t) { abort(); }

  private:
    static const int C1           = 0;
    static const int C1_MERGEABLE = 1;
    static const int C2           = 2;
      logtable * ltable;
      uint64_t epoch;
      typedef mergeManyIterator<
        typename memTreeComponent<TUPLE>::revalidatingIterator,
        typename memTreeComponent<TUPLE>::iterator> inner_merge_it_t;
      typedef mergeManyIterator<
        inner_merge_it_t,
        diskTreeComponent::iterator> merge_it_t;

      merge_it_t* merge_it_;

      TUPLE * last_returned;
      TUPLE * key;
      bool valid;
      void revalidate() {
        if(!valid) {
          validate();
        } else {
          assert(epoch == ltable->get_epoch());
        }
      }


      void validate() {
        typename memTreeComponent<TUPLE>::revalidatingIterator * c0_it;
        typename memTreeComponent<TUPLE>::iterator *c0_mergeable_it[1];
        diskTreeComponent::iterator * disk_it[3];
        epoch = ltable->get_epoch();

        datatuple *t;
        if(last_returned) {
          t = last_returned;
        } else if(key) {
          t = key;
        } else {
          t = NULL;
        }

        c0_it              = new typename memTreeComponent<TUPLE>::revalidatingIterator(ltable->get_tree_c0(), ltable->getMergeData()->rbtree_mut,  t);
        c0_mergeable_it[0] = new typename memTreeComponent<TUPLE>::iterator            (ltable->get_tree_c0_mergeable(),                            t);
        disk_it[0]         = ltable->get_tree_c1()->open_iterator(t);
        if(ltable->get_tree_c1_mergeable()) {
          disk_it[1]         = ltable->get_tree_c1_mergeable()->open_iterator(t);
        } else {
          disk_it[1] = NULL;
        }
        disk_it[2]         = ltable->get_tree_c2()->open_iterator(t);

        inner_merge_it_t * inner_merge_it =
               new inner_merge_it_t(c0_it, c0_mergeable_it, 1, NULL, TUPLE::compare_obj);
        merge_it_ = new merge_it_t(inner_merge_it, disk_it, 3, NULL, TUPLE::compare_obj); // XXX Hardcodes comparator, and does not handle merges
        if(last_returned) {
          TUPLE * junk = merge_it_->peek();
          if(junk && !TUPLE::compare(junk->key(), junk->keylen(), last_returned->key(), last_returned->keylen())) {
            // we already returned junk
            TUPLE::freetuple(merge_it_->getnext());
          }
        }
        valid = true;
      }
  };

};

#endif
