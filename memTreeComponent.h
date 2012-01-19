/*
 * memTreeComponent.h
 *
 * Copyright 2009-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#ifndef _MEMTREECOMPONENT_H_
#define _MEMTREECOMPONENT_H_
#include <set>
#include <assert.h>
#include <mergeStats.h>
#include <stasis/util/stlslab.h>

class memTreeComponent {
public:
//  typedef std::set<datatuple*, datatuple, stlslab<datatuple*> > rbtree_t;
  typedef std::set<datatuple*, datatuple> rbtree_t;
  typedef rbtree_t* rbtree_ptr_t;

  static void tearDownTree(rbtree_ptr_t t);

///////////////////////////////////////////////////////////////
// Plain iterator; cannot cope with changes to underlying tree
///////////////////////////////////////////////////////////////

  class iterator
  {
  private:
    typedef rbtree_t::const_iterator MTITER;

  public:
    iterator( rbtree_t *s )
      : first_(true),
	done_(s == NULL) {
      init_iterators(s, NULL, NULL);
    }

    iterator( rbtree_t *s, datatuple *&key )
      : first_(true), done_(s == NULL) {
      init_iterators(s, key, NULL);
    }

    ~iterator() {
      delete it_;
      delete itend_;
    }

    datatuple* next_callerFrees() {
      if(done_) { return NULL; }
      if(first_) { first_ = 0;} else { (*it_)++; }
      if(*it_==*itend_) { done_= true; return NULL; }

      return (*(*it_))->create_copy();
    }


  private:
    void init_iterators(rbtree_t * s, datatuple * key1, datatuple * key2) {
      if(s) {
        it_    = key1 ? new MTITER(s->lower_bound(key1))  : new MTITER(s->begin());
        itend_ = key2 ? new MTITER(s->upper_bound(key2)) : new MTITER(s->end());
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
    typedef rbtree_t::const_iterator MTITER;

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
    revalidatingIterator( rbtree_t *s, pthread_mutex_t * rb_mut, datatuple *&key ) : s_(s), mut_(rb_mut) {
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
      if(next_ret_) datatuple::freetuple(next_ret_);
    }

    datatuple* next_callerFrees() {
      if(mut_) pthread_mutex_lock(mut_);
      datatuple * ret = next_ret_;
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
    datatuple * next_ret_;
    pthread_mutex_t * mut_;
  };

  ///////////////////////////////////////////////////////////////
  // Revalidating iterator; automatically copes with changes to underlying tree
  ///////////////////////////////////////////////////////////////


  class batchedRevalidatingIterator
  {
  private:
    typedef rbtree_t::const_iterator MTITER;


    void populate_next_ret_impl(std::_Rb_tree_const_iterator<datatuple*>/*MTITER*/ it) {
      num_batched_ = 0;
      cur_off_ = 0;
      while(it != s_->end() && num_batched_ < batch_size_) {
        next_ret_[num_batched_] = (*it)->create_copy();
        num_batched_++;
        it++;
      }
    }
    void populate_next_ret(datatuple *key=NULL, bool include_key=false) {
      if(cur_off_ == num_batched_) {
        if(mut_) pthread_mutex_lock(mut_);
        if(mgr_) {
          while(mgr_->get_merge_stats(0)->get_current_size() < (0.8 * (double)target_size_) && ! *flushing_) {  // TODO: how to pick this threshold?  Too high, and the disk is idle.  Too low, and we waste ram.
            pthread_mutex_unlock(mut_);
            struct timespec ts;
            mergeManager::double_to_ts(&ts, 0.1);
            nanosleep(&ts, 0);
            pthread_mutex_lock(mut_);
          }
        }
        if(key) {
          populate_next_ret_impl(include_key ? s_->lower_bound(key) : s_->upper_bound(key));
        } else {
          populate_next_ret_impl(s_->begin());
        }
        if(mut_) pthread_mutex_unlock(mut_);
      }
    }

  public:
    batchedRevalidatingIterator( rbtree_t *s, mergeManager * mgr, int64_t target_size, bool * flushing, int batch_size, pthread_mutex_t * rb_mut ) : s_(s), mgr_(mgr), target_size_(target_size), flushing_(flushing), batch_size_(batch_size), num_batched_(batch_size), cur_off_(batch_size), mut_(rb_mut) {
      next_ret_ = (datatuple**)malloc(sizeof(next_ret_[0]) * batch_size_);
      populate_next_ret();
    }
      batchedRevalidatingIterator( rbtree_t *s, int batch_size, pthread_mutex_t * rb_mut, datatuple *&key ) : s_(s), mgr_(NULL), target_size_(0), flushing_(0), batch_size_(batch_size), num_batched_(batch_size), cur_off_(batch_size), mut_(rb_mut) {
      next_ret_ = (datatuple**)malloc(sizeof(next_ret_[0]) * batch_size_);
      populate_next_ret(key, true);
    }

    ~batchedRevalidatingIterator() {
      for(int i = cur_off_; i < num_batched_; i++) {
        datatuple::freetuple(next_ret_[i]);
      }
      free(next_ret_);
    }

    datatuple* next_callerFrees() {
      if(cur_off_ == num_batched_) { return NULL; } // the last thing we did is call populate_next_ret_(), which only leaves us in this state at the end of the iterator.
      datatuple * ret = next_ret_[cur_off_];
      cur_off_++;
      populate_next_ret(ret);
      return ret;
    }

  private:
    explicit batchedRevalidatingIterator() { abort(); }
    void operator=(batchedRevalidatingIterator & t) { abort(); }
    int  operator-(batchedRevalidatingIterator & t) { abort(); }

    rbtree_t *s_;
    datatuple ** next_ret_;
    mergeManager * mgr_;
    int64_t target_size_; // the low-water size for the tree.  If cur_size_ is not null, and *cur_size_ < C * target_size_, we sleep.
    bool* flushing_; // never block if *flushing is true.
    int batch_size_;
    int num_batched_;
    int cur_off_;
    pthread_mutex_t * mut_;
  };

};

#endif //_MEMTREECOMPONENT_H_
