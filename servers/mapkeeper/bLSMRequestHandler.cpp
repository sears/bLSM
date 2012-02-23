/*
 * LSMServerHandler.cc
 *
 * Copyright 2011-2012 Yahoo! Inc.
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
 *      Author: sears
 */
#include <stasis/transactional.h>
#include <stasis/logger/safeWrites.h>

#include <iostream>
#include <signal.h>
#include "mergeScheduler.h"
#include "bLSM.h"
#include "bLSMRequestHandler.h"

int blind_update = 0; // updates check preimage by default.

FILE* trace = 0;

LSMServerHandler::
LSMServerHandler(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    // how big the in-memory tree should be (512MB).
    int64_t c0_size = 1024 * 1024 * 512 * 1;
    // write-ahead log
    // 1 -> sync on each commit
    // 2 -> sync on each 2 commits
    // ...
    int log_mode = 0; // do not log by default.
    int64_t expiry_delta = 0;  // do not gc by default
    port = 9090;
    char * tracefile = 0;
    stasis_buffer_manager_size = 1 * 1024 * 1024 * 1024 / PAGE_SIZE;  // 1.5GB total

    for(int i = 1; i < argc; i++) {
        if(!strcmp(argv[i], "--test")) {
            stasis_buffer_manager_size = 3 * 1024 * 1024 * 128 / PAGE_SIZE;  // 228MB total
            c0_size = 1024 * 1024 * 100;
            printf("warning: running w/ tiny c0 for testing\n"); // XXX build a separate test server and deployment server?
        } else if(!strcmp(argv[i], "--benchmark")) {
            stasis_buffer_manager_size = (1024LL * 1024LL * 1024LL * 2LL) / PAGE_SIZE;  // 10GB total
            c0_size =                     1024LL * 1024LL * 1024LL * 8LL;
            printf("note: running w/ 10GB of memory for benchmarking\n"); // XXX build a separate test server and deployment server?
        } else if(!strcmp(argv[i], "--benchmark-small")) {
            stasis_buffer_manager_size = (1024LL * 1024LL * 1024LL * 2LL) / PAGE_SIZE;  // 5GB total
            c0_size =                     1024LL * 1024LL * 1024LL * 3LL;
            printf("note: running w/ 5GB of memory for benchmarking on small box\n");
        } else if(!strcmp(argv[i], "--log-mode")) {
            i++;
            log_mode = atoi(argv[i]);
        } else if(!strcmp(argv[i], "--port")) {
            i++;
            port = atoi(argv[i]);
        } else if(!strcmp(argv[i], "--trace")) {
            i++;
            tracefile = argv[i];
        } else if(!strcmp(argv[i], "--blind-update")) {
            blind_update = 1;
        } else if(!strcmp(argv[i], "--expiry-delta")) {
            i++;
            expiry_delta = atoi(argv[i]);
        } else {
            fprintf(stderr, "Usage: %s [--test|--benchmark|--benchmark-small] [--log-mode <int>] [--expiry-delta <int>]", argv[0]);
            abort();
        }
    }

    if(tracefile) {
      trace = fopen(tracefile, "w");
      if(trace == 0) {
        perror("Couldn't open trace file!");
        abort();
      }
    }

    pthread_mutex_init(&mutex_, 0);
    bLSM::init_stasis();

    int xid = Tbegin();


    recordid table_root = ROOT_RECORD;
    {
        ltable_ = new bLSM(log_mode, c0_size);
        ltable_->expiry = expiry_delta;

        if(TrecordType(xid, ROOT_RECORD) == INVALID_SLOT) {
            printf("Creating empty logstore\n");
            table_root = ltable_->allocTable(xid);
            assert(table_root.page == ROOT_RECORD.page &&
                    table_root.slot == ROOT_RECORD.slot);
        } else {
            printf("Opened existing logstore\n");
            table_root.size = TrecordSize(xid, ROOT_RECORD);
            ltable_->openTable(xid, table_root);
        }

        Tcommit(xid);
        mergeScheduler * mscheduler = new mergeScheduler(ltable_);
        mscheduler->start();
        ltable_->replayLog();

/*
        printf("Stopping merge threads...\n");
        mscheduler->shutdown();
        delete mscheduler;

        printf("Deinitializing stasis...\n");
        fflush(stdout);
 */
    }
    //logtable::deinit_stasis();
    initNextDatabaseId();
}

void LSMServerHandler::
initNextDatabaseId() 
{
    nextDatabaseId_ = 1;
    uint32_t id = 0;
    dataTuple* start = buildTuple(id, "");
    dataTuple* end = buildTuple(id + 1, "");
    bLSM::iterator* itr = new bLSM::iterator(ltable_, start);
    dataTuple* current;
    while ((current = itr->getnext())) {
        // are we at the end of range?
        if (dataTuple::compare_obj(current, end) >= 0) {
            dataTuple::freetuple(current);
            break;
        }
        uint32_t currentId = *((uint32_t*)(current->data()));
        if (currentId > nextDatabaseId_) {
            nextDatabaseId_ = currentId;
        }
        dataTuple::freetuple(current);
    }
    nextDatabaseId_++;
    delete itr;
}

uint32_t LSMServerHandler::
nextDatabaseId()
{
    uint32_t id;
    pthread_mutex_lock(&mutex_);
    nextDatabaseId_++;
    id = nextDatabaseId_;
    pthread_mutex_unlock(&mutex_);
    return id;
}

ResponseCode::type LSMServerHandler::
ping() 
{
  if(trace) { fprintf(trace, "Success = ping()\n"); fflush(trace); }
    return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
shutdown()
{
  if(trace) { fprintf(trace, "Success = shutdown()\n"); fflush(trace); }
  exit(0); // xxx hack
  return mapkeeper::ResponseCode::Success;
}
std::string pp_tuple(dataTuple * tuple) {
  std::string key((const char*)tuple->rawkey(), (size_t)tuple->rawkeylen());
  return key;
}
ResponseCode::type LSMServerHandler::
insert(dataTuple* tuple)
{
    ltable_->insertTuple(tuple);
    dataTuple::freetuple(tuple);
    return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
addMap(const std::string& databaseName) 
{
    uint32_t id = nextDatabaseId();
    dataTuple* tup = buildTuple(0, databaseName, (void*)&id, (uint32_t)(sizeof(id)));
    dataTuple* ret = get(tup);
    if (ret) {
        dataTuple::freetuple(ret);
        if(trace) { fprintf(trace, "MapExists = addMap(%s)\n", databaseName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::MapExists;
    }
    if(trace) { fprintf(trace, "Success = addMap(%s)\n", databaseName.c_str()); fflush(trace); }
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
dropMap(const std::string& databaseName) 
{
  uint32_t id = getDatabaseId(databaseName);
  if(id == 0) {
    if(trace) { fprintf(trace, "MapNotFound = dropMap(%s)\n", databaseName.c_str()); fflush(trace); }
    return mapkeeper::ResponseCode::MapNotFound;
  }
  dataTuple * tup = buildTuple(0, databaseName);
  dataTuple * exists = get(tup);

  if(exists) {
    dataTuple::freetuple(exists);

    dataTuple * startKey = buildTuple(id, "");
    bLSM::iterator * itr = new bLSM::iterator(ltable_, startKey);
    dataTuple::freetuple(startKey);
    dataTuple * current;

    // insert tombstone; deletes metadata entry for map; frees tup
    insert(tup);

    while(NULL != (current = itr->getnext())) {
      if(*((uint32_t*)current->strippedkey()) != id) {
        dataTuple::freetuple(current);
        break;
      }
      dataTuple * del = dataTuple::create(current->strippedkey(), current->strippedkeylen());
      ltable_->insertTuple(del);
      dataTuple::freetuple(del);
      dataTuple::freetuple(current);
    }
    delete itr;
    if(trace) { fprintf(trace, "Success = dropMap(%s)\n", databaseName.c_str()); fflush(trace); }
    return mapkeeper::ResponseCode::Success;
  } else {
    dataTuple::freetuple(tup);
    if(trace) { fprintf(trace, "MapNotFound = dropMap(%s)\n", databaseName.c_str()); fflush(trace); }
    return mapkeeper::ResponseCode::MapNotFound;
  }
}

void LSMServerHandler::
listMaps(StringListResponse& _return) 
{
  dataTuple * startKey = buildTuple(0, "");
  bLSM::iterator * itr = new bLSM::iterator(ltable_, startKey);
  dataTuple::freetuple(startKey);
  dataTuple * current;
  while(NULL != (current = itr->getnext())) {
    if(*((uint32_t*)current->strippedkey()) != 0) {
      dataTuple::freetuple(current);
      break;
    }
    _return.values.push_back(
        std::string((char*)(current->strippedkey()) + sizeof(uint32_t),
                    current->strippedkeylen() - sizeof(uint32_t)));
    dataTuple::freetuple(current);
  }
  delete itr;
  if(trace) { fprintf(trace, "... = listMaps()\n"); fflush(trace); }
    _return.responseCode = mapkeeper::ResponseCode::Success;
}

void LSMServerHandler::
scan(RecordListResponse& _return, const std::string& databaseName, const ScanOrder::type order, 
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes)
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        // database not found
        if(trace) { fprintf(trace, "MapNotFound = scan(...)\n"); fflush(trace); }
        _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
        return;
    }
 
    dataTuple* start = buildTuple(id, startKey);
    dataTuple* end;
    if (endKey.empty()) {
        end = buildTuple(id + 1, endKey);
    } else {
        end = buildTuple(id, endKey);
    }
    bLSM::iterator* itr = new bLSM::iterator(ltable_, start);

    int32_t resultSize = 0;

    while ((maxRecords == 0 || (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        dataTuple* current = itr->getnext();
        if (current == NULL) {
            _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
            if(trace) { fprintf(trace, "ScanEnded = scan(...)\n"); fflush(trace); }
            break;
        }

        int cmp = dataTuple::compare_obj(current, start);
        if ((!startKeyIncluded) && cmp == 0) {
            dataTuple::freetuple(current);
            continue;
        } 

        // are we at the end of range?
        cmp = dataTuple::compare_obj(current, end);
        if ((!endKeyIncluded && cmp >= 0) ||
                (endKeyIncluded && cmp > 0)) {
            dataTuple::freetuple(current);
            _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
            if(trace) { fprintf(trace, "ScanEnded = scan(...)\n"); fflush(trace); }
            break;
        } 

        Record rec;
        int32_t keySize =  current->strippedkeylen() - sizeof(id);
        int32_t dataSize = current->datalen();

        rec.key.assign((char*)(current->strippedkey()) + sizeof(id), keySize);
        rec.value.assign((char*)(current->data()), dataSize);
        _return.records.push_back(rec);
        resultSize += keySize + dataSize;
        dataTuple::freetuple(current);
    }
    delete itr;
}

dataTuple* LSMServerHandler::
get(dataTuple* tuple)
{
    // -1 is invalid txn id
    dataTuple* tup = ltable_->findTuple_first(-1, tuple->rawkey(), tuple->rawkeylen());
    return tup;
}

void LSMServerHandler::
get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        // database not found
        if(trace) { fprintf(trace, "MapNotFound = get(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
        return;
    }
    
    dataTuple* recordBody = get(id, recordName);
    if (recordBody == NULL) {
        // record not found
        if(trace) { fprintf(trace, "RecordNotFound = get(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        _return.responseCode = mapkeeper::ResponseCode::RecordNotFound;
        return;
    }
    if(trace) { fprintf(trace, "Success = get(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
    _return.responseCode = mapkeeper::ResponseCode::Success;
    _return.value.assign((const char*)(recordBody->data()), recordBody->datalen());
    dataTuple::freetuple(recordBody);
}

uint32_t LSMServerHandler::
getDatabaseId(const std::string& databaseName)
{
    dataTuple* tup = buildTuple(0, databaseName);
    dataTuple* databaseId = get(tup);
    dataTuple::freetuple(tup);
    if (databaseId == NULL) {
        // database not found
        std::cout << "db not found" << std::endl;
        return 0;
    }
    uint32_t id = *((uint32_t*)(databaseId->data()));
    dataTuple::freetuple(databaseId);
    return id;
}


dataTuple* LSMServerHandler::
get(uint32_t databaseId, const std::string& recordName)
{
    dataTuple* recordKey = buildTuple(databaseId, recordName);
    dataTuple* ret = get(recordKey);
    dataTuple::freetuple(recordKey);
    return ret;
}

ResponseCode::type LSMServerHandler::
put(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
  uint32_t id = getDatabaseId(databaseName);
  if (id == 0) {
      if(trace) { fprintf(trace, "MapNotFound = put(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
      return mapkeeper::ResponseCode::MapNotFound;
  }
  dataTuple* tup = buildTuple(id, recordName, recordBody);
  if(trace) { fprintf(trace, "Success = put(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
  return insert(tup);
}

ResponseCode::type LSMServerHandler::
insert(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        if(trace) { fprintf(trace, "MapNotFound = insert(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::MapNotFound;
    }
    if(!blind_update) {
      dataTuple* oldRecordBody = get(id, recordName);
      if (oldRecordBody != NULL) {
        if(oldRecordBody->isDelete()) {
          dataTuple::freetuple(oldRecordBody);
        } else {
          dataTuple::freetuple(oldRecordBody);
          if(trace) { fprintf(trace, "RecordExists = insert(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
          return mapkeeper::ResponseCode::RecordExists;
        }
      }
    }

    dataTuple* tup = buildTuple(id, recordName, recordBody);
    if(trace) { fprintf(trace, "Success = insert(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
insertMany(const std::string& databaseName, const std::vector<Record> & records)
{
    if(trace) { fprintf(trace, "Error = insertMany(%s, ...) (not supported)\n", databaseName.c_str()); fflush(trace); }
    return mapkeeper::ResponseCode::Error;
}

ResponseCode::type LSMServerHandler::
update(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        if(trace) { fprintf(trace, "MapNotFound = update(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::MapNotFound;
    }
    if(!blind_update) {
      dataTuple* oldRecordBody = get(id, recordName);
      if (oldRecordBody == NULL) {
        if(trace) { fprintf(trace, "RecordNotFound = update(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::RecordNotFound;
      }
      dataTuple::freetuple(oldRecordBody);
    }
    dataTuple* tup = buildTuple(id, recordName, recordBody);
    if(trace) { fprintf(trace, "Success = update(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
remove(const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        if(trace) { fprintf(trace, "MapNotFound = remove(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::MapNotFound;
    }
    dataTuple* oldRecordBody = get(id, recordName);
    if (oldRecordBody == NULL) {
        if(trace) { fprintf(trace, "RecordNotFound = remove(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
        return mapkeeper::ResponseCode::RecordNotFound;
    }
    dataTuple::freetuple(oldRecordBody);
    dataTuple* tup = buildTuple(id, recordName);
    if(trace) { fprintf(trace, "Success = remove(%s, %s)\n", databaseName.c_str(), recordName.c_str()); fflush(trace); }
    return insert(tup);
}

dataTuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName)
{
    return buildTuple(databaseId, recordName, NULL, DELETE);
}

dataTuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName, const std::string& recordBody)
{
    return buildTuple(databaseId, recordName, recordBody.c_str(), recordBody.size());
}

dataTuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName, const void* body, uint32_t bodySize)
{
    uint32_t keySize = sizeof(databaseId) + recordName.size();
    unsigned char* key = (unsigned char*)malloc(keySize);
    *(uint32_t*)key = htonl(databaseId);
    memcpy(((uint32_t*)key) + 1, recordName.c_str(), recordName.size());
    dataTuple *tup = dataTuple::create(key, keySize, body, bodySize);
    free(key);
    return tup;
}
