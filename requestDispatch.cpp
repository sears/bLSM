/*
 * requestDispatch.cpp
 *
 * Copyright 2010-2012 Yahoo! Inc.
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
 *  Created on: Aug 11, 2010
 *      Author: sears
 */
#include "requestDispatch.h"
#include "regionAllocator.h"

template<class HANDLE>
inline int requestDispatch<HANDLE>::op_insert(blsm * ltable, HANDLE fd, datatuple * tuple) {
    //insert/update/delete
    ltable->insertTuple(tuple);
    //step 4: send response
    return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_test_and_set(blsm * ltable, HANDLE fd, datatuple * tuple, datatuple * tuple2) {
    //insert/update/delete
    bool succ = ltable->testAndSetTuple(tuple, tuple2);
    //step 4: send response
    return writeoptosocket(fd, succ ? LOGSTORE_RESPONSE_SUCCESS : LOGSTORE_RESPONSE_FAIL);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_bulk_insert(blsm *ltable, HANDLE fd) {
  int err = writeoptosocket(fd, LOGSTORE_RESPONSE_RECEIVING_TUPLES);
  datatuple ** tups = (datatuple **) malloc(sizeof(tups[0]) * 100);
  int tups_size = 100;
  int cur_tup_count = 0;
  while((tups[cur_tup_count] = readtuplefromsocket(fd, &err))) {
    cur_tup_count++;
    if(cur_tup_count == tups_size) {
      ltable->insertManyTuples(tups, cur_tup_count);
      for(int i = 0; i < cur_tup_count; i++) {
        datatuple::freetuple(tups[i]);
      }
      cur_tup_count = 0;
    }
  }
  ltable->insertManyTuples(tups, cur_tup_count);
  for(int i = 0; i < cur_tup_count; i++) {
    datatuple::freetuple(tups[i]);
  }
  free(tups);
  if(!err) err = writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
  return err;
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_find(blsm * ltable, HANDLE fd, datatuple * tuple) {
    //find the tuple
    datatuple *dt = ltable->findTuple_first(-1, tuple->strippedkey(), tuple->strippedkeylen());

    #ifdef STATS_ENABLED

    if(dt == 0) {
        DEBUG("key not found:\t%s\n", datatuple::key_to_str(tuple.key()).c_str());
    } else if( dt->datalen() != 1024) {
        DEBUG("data len for\t%s:\t%d\n", datatuple::key_to_str(tuple.key()).c_str(),
               dt->datalen);
        if(datatuple::compare(tuple->key(), tuple->keylen(), dt->key(), dt->keylen()) != 0) {
            DEBUG("key not equal:\t%s\t%s\n", datatuple::key_to_str(tuple.key()).c_str(),
                   datatuple::key_to_str(dt->key).c_str());
        }

    }
    #endif

    bool dt_needs_free;
    if(dt == 0)  //tuple does not exist.
    {
        dt = tuple;
        dt->setDelete();
        dt_needs_free = false;
    } else {
        dt_needs_free = true;
    }
    DEBUG(stderr, "find result: %s\n", dt->isDelete() ? "not found" : "found");
    //send the reply code
    int err = writeoptosocket(fd, LOGSTORE_RESPONSE_SENDING_TUPLES);
    if(!err) {
        //send the tuple
        err = writetupletosocket(fd, dt);
    }
    if(!err) {
        writeendofiteratortosocket(fd);
    }
    //free datatuple
    if(dt_needs_free) {
        datatuple::freetuple(dt);
    }
    return err;
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_scan(blsm * ltable, HANDLE fd, datatuple * tuple, datatuple * tuple2, size_t limit) {
    size_t count = 0;
    int err = writeoptosocket(fd, LOGSTORE_RESPONSE_SENDING_TUPLES);

    if(!err) {
        blsm::iterator * itr = new blsm::iterator(ltable, tuple);
        datatuple * t;
        while(!err && (t = itr->getnext())) {
            if(tuple2) {  // are we at the end of range?
                if(datatuple::compare_obj(t, tuple2) >= 0) {
                    datatuple::freetuple(t);
                    break;
                }
            }
            err = writetupletosocket(fd, t);
            datatuple::freetuple(t);
            count ++;
            if(count == limit) { break; }  // did we hit limit?
        }
        delete itr;
    }
    if(!err) { writeendofiteratortosocket(fd); }
    return err;
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_flush(blsm * ltable, HANDLE fd) {
    ltable->flushTable();
    return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_shutdown(blsm * ltable, HANDLE fd) {
    ltable->accepting_new_requests = false;
    return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_stat_space_usage(blsm * ltable, HANDLE fd) {


    int xid = Tbegin();

    rwlc_readlock(ltable->header_mut);

    /*  pageid_t datapage_c1_region_length, datapage_c1_mergeable_region_length = 0, datapage_c2_region_length;
    pageid_t datapage_c1_region_count,  datapage_c1_mergeable_region_count = 0, datapage_c2_region_count;
    pageid_t tree_c1_region_length, tree_c1_mergeable_region_length = 0, tree_c2_region_length;
    pageid_t tree_c1_region_count,  tree_c1_mergeable_region_count = 0, tree_c2_region_count;

    pageid_t * datapage_c1_regions = ltable->get_tree_c1()->get_datapage_alloc()->list_regions(xid, &datapage_c1_region_length, &datapage_c1_region_count);

    pageid_t * datapage_c1_mergeable_regions = NULL;
    if(ltable->get_tree_c1_mergeable()) {
      datapage_c1_mergeable_regions = ltable->get_tree_c1_mergeable()->get_datapage_alloc()->list_regions(xid, &datapage_c1_mergeable_region_length, &datapage_c1_mergeable_region_count);
    }
    pageid_t * datapage_c2_regions = ltable->get_tree_c2()->get_datapage_alloc()->list_regions(xid, &datapage_c2_region_length, &datapage_c2_region_count);

    pageid_t * tree_c1_regions = ltable->get_tree_c1()->get_internal_node_alloc()->list_regions(xid, &tree_c1_region_length, &tree_c1_region_count);

    pageid_t * tree_c1_mergeable_regions = NULL;
    if(ltable->get_tree_c1_mergeable()) {
      tree_c1_mergeable_regions = ltable->get_tree_c1_mergeable()->get_internal_node_alloc()->list_regions(xid, &tree_c1_mergeable_region_length, &tree_c1_mergeable_region_count);
    }

    pageid_t * tree_c2_regions = ltable->get_tree_c2()->get_internal_node_alloc()->list_regions(xid, &tree_c2_region_length, &tree_c2_region_count);
    */

    pageid_t internal_c1_region_length, internal_c1_mergeable_region_length = 0, internal_c2_region_length;
    pageid_t internal_c1_region_count,  internal_c1_mergeable_region_count = 0, internal_c2_region_count;
    pageid_t *internal_c1_regions, *internal_c1_mergeable_regions = NULL, *internal_c2_regions;

    pageid_t datapage_c1_region_length, datapage_c1_mergeable_region_length = 0, datapage_c2_region_length;
    pageid_t datapage_c1_region_count,  datapage_c1_mergeable_region_count = 0, datapage_c2_region_count;
    pageid_t *datapage_c1_regions, *datapage_c1_mergeable_regions = NULL, *datapage_c2_regions;

    ltable->get_tree_c1()->list_regions(xid,
                          &internal_c1_region_length, &internal_c1_region_count, &internal_c1_regions,
                          &datapage_c1_region_length, &datapage_c1_region_count, &datapage_c1_regions);
    if(ltable->get_tree_c1_mergeable()) {
      ltable->get_tree_c1_mergeable()->list_regions(xid,
                            &internal_c1_mergeable_region_length, &internal_c1_mergeable_region_count, &internal_c1_mergeable_regions,
                            &datapage_c1_mergeable_region_length, &datapage_c1_mergeable_region_count, &datapage_c1_mergeable_regions);

    }
    ltable->get_tree_c2()->list_regions(xid,
                          &internal_c2_region_length, &internal_c2_region_count, &internal_c2_regions,
                          &datapage_c2_region_length, &datapage_c2_region_count, &datapage_c2_regions);


    free(datapage_c1_regions);
    free(datapage_c1_mergeable_regions);
    free(datapage_c2_regions);

    free(internal_c1_regions);
    free(internal_c1_mergeable_regions);
    free(internal_c2_regions);


    uint64_t treesize = PAGE_SIZE *
                ( ( datapage_c1_region_count           * datapage_c1_region_length )
                + ( datapage_c1_mergeable_region_count * datapage_c1_mergeable_region_length )
                + ( datapage_c2_region_count           * datapage_c2_region_length)
                + ( internal_c1_region_count           * internal_c1_region_length )
                + ( internal_c1_mergeable_region_count * internal_c1_mergeable_region_length )
                + ( internal_c2_region_count           * internal_c2_region_length) );

    boundary_tag tag;
    pageid_t pid = ROOT_RECORD.page;
    TregionReadBoundaryTag(xid, pid, &tag);
    uint64_t max_off = 0;
    do {
        max_off = pid + tag.size;
        ;
    } while(TregionNextBoundaryTag(xid, &pid, &tag, 0/*all allocation managers*/));

    rwlc_unlock(ltable->header_mut);

    Tcommit(xid);

    uint64_t filesize = max_off * PAGE_SIZE;
    datatuple *tup = datatuple::create(&treesize, sizeof(treesize), &filesize, sizeof(filesize));

    DEBUG("tree size: %lld, filesize %lld\n", treesize, filesize);

    int err = 0;
    if(!err){ err = writeoptosocket(fd, LOGSTORE_RESPONSE_SENDING_TUPLES); }
    if(!err){ err = writetupletosocket(fd, tup);                           }
    if(!err){ err = writeendofiteratortosocket(fd);                        }


    datatuple::freetuple(tup);

    return err;
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_stat_perf_report(blsm * ltable, HANDLE fd) {

}


template<class HANDLE>
inline int requestDispatch<HANDLE>::op_stat_histogram(blsm * ltable, HANDLE fd, size_t limit) {

    if(limit < 3) {
        return writeoptosocket(fd, LOGSTORE_PROTOCOL_ERROR);
    }

    int xid = Tbegin();
    RegionAllocator * ro_alloc = new RegionAllocator();
    diskTreeComponent::internalNodes::iterator * it = new diskTreeComponent::internalNodes::iterator(xid, ro_alloc, ltable->get_tree_c2()->get_root_rid());
    size_t count = 0;
    int err = 0;

    while(it->next()) { count++; }
    it->close();
    delete(it);

    uint64_t stride;

    if(count > limit) {
        stride = count / (limit-1);
        stride++;  // this way, we truncate the last bucket instead of occasionally creating a tiny last bucket.
    } else {
        stride = 1;
    }

    datatuple * tup = datatuple::create(&stride, sizeof(stride));

    if(!err) { err = writeoptosocket(fd, LOGSTORE_RESPONSE_SENDING_TUPLES); }
    if(!err) { err = writetupletosocket(fd, tup);                           }

    datatuple::freetuple(tup);

    size_t cur_stride = 0;
    size_t i = 0;
    it = new diskTreeComponent::internalNodes::iterator(xid, ro_alloc, ltable->get_tree_c2()->get_root_rid()); // TODO make this method private?
    while(it->next()) {
        i++;
        if(i == count || !cur_stride) {  // do we want to send this key? (this matches the first, last and interior keys)
            byte * key;
            size_t keylen= it->key(&key);
            tup = datatuple::create(key, keylen);

            if(!err) { err = writetupletosocket(fd, tup);                   }

            datatuple::freetuple(tup);
            cur_stride = stride;
        }
        cur_stride--;
    }

    it->close();
    delete(it);
    delete(ro_alloc);
    if(!err){ err = writeendofiteratortosocket(fd);                         }
    Tcommit(xid);
    return err;
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_dbg_blockmap(blsm * ltable, HANDLE fd) {
    // produce a list of stasis regions
    int xid = Tbegin();

    rwlc_readlock(ltable->header_mut);

    // produce a list of regions used by current tree components
    /*  pageid_t datapage_c1_region_length, datapage_c1_mergeable_region_length = 0, datapage_c2_region_length;
    pageid_t datapage_c1_region_count,  datapage_c1_mergeable_region_count = 0, datapage_c2_region_count;
    pageid_t * datapage_c1_regions = ltable->get_tree_c1()->get_datapage_alloc()->list_regions(xid, &datapage_c1_region_length, &datapage_c1_region_count);
    pageid_t * datapage_c1_mergeable_regions = NULL;
    if(ltable->get_tree_c1_mergeable()) {
      datapage_c1_mergeable_regions = ltable->get_tree_c1_mergeable()->get_datapage_alloc()->list_regions(xid, &datapage_c1_mergeable_region_length, &datapage_c1_mergeable_region_count);
    }
    pageid_t * datapage_c2_regions = ltable->get_tree_c2()->get_datapage_alloc()->list_regions(xid, &datapage_c2_region_length, &datapage_c2_region_count); */


    /*  pageid_t * tree_c1_regions = ltable->get_tree_c1()->get_internal_node_alloc()->list_regions(xid, &tree_c1_region_length, &tree_c1_region_count);

    pageid_t * tree_c1_mergeable_regions = NULL;
    if(ltable->get_tree_c1_mergeable()) {
      tree_c1_mergeable_regions = ltable->get_tree_c1_mergeable()->get_internal_node_alloc()->list_regions(xid, &tree_c1_mergeable_region_length, &tree_c1_mergeable_region_count);
    }
    pageid_t * tree_c2_regions = ltable->get_tree_c2()->get_internal_node_alloc()->list_regions(xid, &tree_c2_region_length, &tree_c2_region_count); */

    pageid_t internal_c1_region_length, internal_c1_mergeable_region_length = 0, internal_c2_region_length;
    pageid_t internal_c1_region_count,  internal_c1_mergeable_region_count = 0, internal_c2_region_count;
    pageid_t *internal_c1_regions, *internal_c1_mergeable_regions = NULL, *internal_c2_regions;

    pageid_t datapage_c1_region_length, datapage_c1_mergeable_region_length = 0, datapage_c2_region_length;
    pageid_t datapage_c1_region_count,  datapage_c1_mergeable_region_count = 0, datapage_c2_region_count;
    pageid_t *datapage_c1_regions, *datapage_c1_mergeable_regions = NULL, *datapage_c2_regions;

    ltable->get_tree_c1()->list_regions(xid,
                          &internal_c1_region_length, &internal_c1_region_count, &internal_c1_regions,
                          &datapage_c1_region_length, &datapage_c1_region_count, &datapage_c1_regions);
    if(ltable->get_tree_c1_mergeable()) {
      ltable->get_tree_c1_mergeable()->list_regions(xid,
                            &internal_c1_mergeable_region_length, &internal_c1_mergeable_region_count, &internal_c1_mergeable_regions,
                            &datapage_c1_mergeable_region_length, &datapage_c1_mergeable_region_count, &datapage_c1_mergeable_regions);

    }
    ltable->get_tree_c2()->list_regions(xid,
                          &internal_c2_region_length, &internal_c2_region_count, &internal_c2_regions,
                          &datapage_c2_region_length, &datapage_c2_region_count, &datapage_c2_regions);

    rwlc_unlock(ltable->header_mut);

    Tcommit(xid);

    printf("C1 Datapage Regions (each is %lld pages long):\n", datapage_c1_region_length);
    for(pageid_t i = 0; i < datapage_c1_region_count; i++) {
        printf("%lld ", datapage_c1_regions[i]);
    }

    printf("\nC1 Internal Node Regions (each is %lld pages long):\n", internal_c1_region_length);
    for(pageid_t i = 0; i < internal_c1_region_count; i++) {
        printf("%lld ", internal_c1_regions[i]);
    }

    printf("\nC2 Datapage Regions (each is %lld pages long):\n", datapage_c2_region_length);
    for(pageid_t i = 0; i < datapage_c2_region_count; i++) {
        printf("%lld ", datapage_c2_regions[i]);
    }

    printf("\nC2 Internal Node Regions (each is %lld pages long):\n", internal_c2_region_length);
    for(pageid_t i = 0; i < internal_c2_region_count; i++) {
        printf("%lld ", internal_c2_regions[i]);
    }
    printf("\nStasis Region Map\n");

    boundary_tag tag;
    pageid_t pid = ROOT_RECORD.page;
    TregionReadBoundaryTag(xid, pid, &tag);
    pageid_t max_off = 0;
    bool done;
    do {
        max_off = pid + tag.size;
        // print tag.
        printf("\tPage %lld\tSize %lld\tAllocationManager %d\n", (long long)pid, (long long)tag.size, (int)tag.allocation_manager);
        done = ! TregionNextBoundaryTag(xid, &pid, &tag, 0/*all allocation managers*/);
    } while(!done);

    printf("\n");

    printf("Tree components are using %lld megabytes.  File is using %lld megabytes.\n",
       PAGE_SIZE * (internal_c1_region_length * internal_c1_region_count
            + internal_c1_mergeable_region_length * internal_c1_mergeable_region_count
            + internal_c2_region_length * internal_c2_region_count
            + datapage_c1_region_length * datapage_c1_region_count
            + datapage_c1_mergeable_region_length * datapage_c1_mergeable_region_count
            + datapage_c2_region_length * datapage_c2_region_count) / (1024 * 1024),
       (PAGE_SIZE * max_off) / (1024*1024));

    free(datapage_c1_regions);
    if(datapage_c1_mergeable_regions) free(datapage_c1_mergeable_regions);
    free(datapage_c2_regions);
    free(internal_c1_regions);
    if(internal_c1_mergeable_regions) free(internal_c1_mergeable_regions);
    free(internal_c2_regions);
    return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}

template<class HANDLE>
inline int requestDispatch<HANDLE>::op_dbg_drop_database(blsm * ltable, HANDLE fd) {
    blsm::iterator * itr = new blsm::iterator(ltable);
    datatuple * del;
    fprintf(stderr, "DROPPING DATABASE...\n");
    long long n = 0;
    while((del = itr->getnext())) {
      if(!del->isDelete()) {
        del->setDelete();
        ltable->insertTuple(del);
        n++;
        if(!(n % 1000)) {
          printf("X %lld %s\n", n, (char*)del->rawkey()); fflush(stdout);
        }
      } else {
        n++;
        if(!(n % 1000)) {
          printf("? %lld %s\n", n, (char*)del->rawkey()); fflush(stdout);
        }
      }
      datatuple::freetuple(del);
    }
    delete itr;
    fprintf(stderr, "...DROP DATABASE COMPLETE\n");
    return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_dbg_noop(blsm * ltable, HANDLE fd) {
  return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
}
template<class HANDLE>
inline int requestDispatch<HANDLE>::op_dbg_set_log_mode(blsm * ltable, HANDLE fd, datatuple * tuple) {
  if(tuple->rawkeylen() != sizeof(int)) {
	  abort();
	  return writeoptosocket(fd, LOGSTORE_PROTOCOL_ERROR);
  } else {
	  int old_mode = ltable->log_mode;
	  ltable->log_mode = *(int*)tuple->rawkey();
	  fprintf(stderr, "\n\nChanged log mode from %d to %d\n\n", old_mode, ltable->log_mode);
	  return writeoptosocket(fd, LOGSTORE_RESPONSE_SUCCESS);
  }
}
template<class HANDLE>
int requestDispatch<HANDLE>::dispatch_request(HANDLE f, blsm *ltable) {
  //step 1: read the opcode
  network_op_t opcode = readopfromsocket(f, LOGSTORE_CLIENT_REQUEST);
  if(opcode == LOGSTORE_CONN_CLOSED_ERROR) {
      opcode = OP_DONE;
      printf("Broken client closed connection uncleanly\n");
  }

  int err = opcode == OP_DONE || opiserror(opcode); //close the conn on failure

  //step 2: read the first tuple from client
  datatuple *tuple = 0, *tuple2 = 0;
  if(!err) { tuple  = readtuplefromsocket(f, &err); }
  //        read the second tuple from client
  if(!err) { tuple2 = readtuplefromsocket(f, &err); }

  //step 3: process the tuple
  if(!err) { err = dispatch_request(opcode, tuple, tuple2, ltable, f); }

  //free the tuple
  if(tuple)  datatuple::freetuple(tuple);
  if(tuple2) datatuple::freetuple(tuple2);

  // Deal with old work_queue item by freeing it or putting it back in the queue.

  if(err) {
    if(opcode != OP_DONE) {
      perror("network error. conn closed");
    } else {
//              printf("client done. conn closed. (%d, %d)\n",
//                     *(item->data->workitem), item->data->work_queue->size());
    }
  }
  return err;

}
template<class HANDLE>
int requestDispatch<HANDLE>::dispatch_request(network_op_t opcode, datatuple * tuple, datatuple * tuple2, blsm * ltable, HANDLE fd) {
    int err = 0;
#if 0
    if(tuple) {
        char * printme = (char*)malloc(tuple->rawkeylen()+1);
        memcpy(printme, tuple->rawkey(), tuple->rawkeylen());
        printme[tuple->rawkeylen()] = 0;
        printf("\nop = %d, key = ->%s<-, isdelete = %d\n", opcode, printme, tuple->isDelete());
        free(printme);
    }
#endif
    if(opcode == OP_INSERT)
    {
        err = op_insert(ltable, fd, tuple);
    }
    else if(opcode == OP_TEST_AND_SET)
    {
        err = op_test_and_set(ltable, fd, tuple, tuple2);
    }
    else if(opcode == OP_FIND)
    {
        err = op_find(ltable, fd, tuple);
    }
    else if(opcode == OP_SCAN)
    {
        size_t limit = readcountfromsocket(fd, &err);
        if(!err) {  err = op_scan(ltable, fd, tuple, tuple2, limit); }
    }
    else if(opcode == OP_BULK_INSERT) {
        err = op_bulk_insert(ltable, fd);
    }
    else if(opcode == OP_FLUSH)
    {
        err = op_flush(ltable, fd);
    }
    else if(opcode == OP_SHUTDOWN)
    {
        err = op_shutdown(ltable, fd);
    }
    else if(opcode == OP_STAT_SPACE_USAGE)
    {
        err = op_stat_space_usage(ltable, fd);
    }
    else if(opcode == OP_STAT_PERF_REPORT)
    {
        err = op_stat_perf_report(ltable, fd);
    }
    else if(opcode == OP_STAT_HISTOGRAM)
    {
        size_t limit = readcountfromsocket(fd, &err);
        err = op_stat_histogram(ltable, fd, limit);
    }
    else if(opcode == OP_DBG_BLOCKMAP)
    {
        err = op_dbg_blockmap(ltable, fd);
    }
    else if(opcode == OP_DBG_DROP_DATABASE)
    {
        err = op_dbg_drop_database(ltable, fd);
    }
    else if(opcode == OP_DBG_NOOP) {
      err = op_dbg_noop(ltable, fd);
    }
    else if(opcode == OP_DBG_SET_LOG_MODE) {
      err = op_dbg_set_log_mode(ltable, fd, tuple);
    }
    return err;
}

template class requestDispatch<int>;
template class requestDispatch<FILE*>;
