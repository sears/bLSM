#include <stasis/transactional.h>
#include <stasis/logger/safeWrites.h>
#undef end
#undef try
#undef begin

#include <iostream>
#include <signal.h>
#include "merger.h"
#include "logstore.h"
#include "LSMServerHandler.h"

int blind_update = 0; // updates check preimage by default.

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
        } else if(!strcmp(argv[i], "--log-mode")) {
            i++;
            log_mode = atoi(argv[i]);
	} else if(!strcmp(argv[i], "--blind-update")) {
	  blind_update = 1;
        } else if(!strcmp(argv[i], "--expiry-delta")) {
            i++;
            expiry_delta = atoi(argv[i]);
        } else {
            fprintf(stderr, "Usage: %s [--test|--benchmark] [--log-mode <int>] [--expiry-delta <int>]", argv[0]);
            abort();
        }
    }

    pthread_mutex_init(&mutex_, 0);
    logtable::init_stasis();

    int xid = Tbegin();


    recordid table_root = ROOT_RECORD;
    {
        ltable_ = new logtable(log_mode, c0_size);
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
        merge_scheduler * mscheduler = new merge_scheduler(ltable_);
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
    datatuple* start = buildTuple(id, "");
    datatuple* end = buildTuple(id + 1, "");
    logtable::iterator* itr = new logtable::iterator(ltable_, start);
    datatuple* current;
    while ((current = itr->getnext())) {
        // are we at the end of range?
        if (datatuple::compare_obj(current, end) >= 0) {
            datatuple::freetuple(current);
            break;
        }
        uint32_t currentId = *((uint32_t*)(current->data()));
        if (currentId > nextDatabaseId_) {
            nextDatabaseId_ = currentId;
        }
        datatuple::freetuple(current);
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
    return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
shutdown()
{
  exit(0); // xxx hack
  return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
insert(datatuple* tuple)
{
    ltable_->insertTuple(tuple);
    datatuple::freetuple(tuple);
    return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
addMap(const std::string& databaseName) 
{
    uint32_t id = nextDatabaseId();
    datatuple* tup = buildTuple(0, databaseName, (void*)&id, (uint32_t)(sizeof(id)));
    datatuple* ret = get(tup);
    if (ret) {
        datatuple::freetuple(ret);
        return mapkeeper::ResponseCode::MapExists;
    }
    return insert(tup);
}

/**
 * TODO:
 * Don't just remove database from databaseIds. You need to delete
 * all the records!
 */
ResponseCode::type LSMServerHandler::
dropMap(const std::string& databaseName) 
{
#if 0
    Bdb::ResponseCode rc = databaseIds_.remove(databaseName);
    if (rc == Bdb::KeyNotFound) {
        return mapkeeper::ResponseCode::MapNotFound;
    } else if (rc != Bdb::Success) {
        return mapkeeper::ResponseCode::Error;
    } else {
        return mapkeeper::ResponseCode::Success;
    }
#endif
        return mapkeeper::ResponseCode::Success;
}

void LSMServerHandler::
listMaps(StringListResponse& _return) 
{
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
        _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
        return;
    }
 
    datatuple* start = buildTuple(id, startKey);
    datatuple* end;
    if (endKey.empty()) {
        end = buildTuple(id + 1, endKey);
    } else {
        end = buildTuple(id, endKey);
    }
    logtable::iterator* itr = new logtable::iterator(ltable_, start);

    int32_t resultSize = 0;

    while ((maxRecords == 0 || (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        datatuple* current = itr->getnext();
        if (current == NULL) {
            _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
            break;
        }

        int cmp = datatuple::compare_obj(current, start);
        if ((!startKeyIncluded) && cmp == 0) {
            datatuple::freetuple(current);
            continue;
        } 

        // are we at the end of range?
        cmp = datatuple::compare_obj(current, end);
        if ((!endKeyIncluded && cmp >= 0) ||
                (endKeyIncluded && cmp > 0)) {
            datatuple::freetuple(current);
            _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
            break;
        } 

        Record rec;
        int32_t keySize =  current->strippedkeylen() - sizeof(id);
        int32_t dataSize = current->datalen();

        rec.key.assign((char*)(current->strippedkey()) + sizeof(id), keySize);
        rec.value.assign((char*)(current->data()), dataSize);
        _return.records.push_back(rec);
        resultSize += keySize + dataSize;
        datatuple::freetuple(current);
    }
    delete itr;
}

datatuple* LSMServerHandler::
get(datatuple* tuple)
{
    // -1 is invalid txn id
    datatuple* tup = ltable_->findTuple_first(-1, tuple->rawkey(), tuple->rawkeylen());
    return tup;
}

void LSMServerHandler::
get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        // database not found
        _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
        return;
    }
    
    datatuple* recordBody = get(id, recordName);
    if (recordBody == NULL) {
        // record not found
        _return.responseCode = mapkeeper::ResponseCode::RecordNotFound;
        return;
    }
    _return.responseCode = mapkeeper::ResponseCode::Success;
    _return.value.assign((const char*)(recordBody->data()), recordBody->datalen());
    datatuple::freetuple(recordBody);
}

uint32_t LSMServerHandler::
getDatabaseId(const std::string& databaseName)
{
    datatuple* tup = buildTuple(0, databaseName);
    datatuple* databaseId = get(tup);
    datatuple::freetuple(tup);
    if (databaseId == NULL) {
        // database not found
        std::cout << "db not found" << std::endl;
        return 0;
    }
    uint32_t id = *((uint32_t*)(databaseId->data()));
    datatuple::freetuple(databaseId);
    return id;
}


datatuple* LSMServerHandler::
get(uint32_t databaseId, const std::string& recordName)
{
    datatuple* recordKey = buildTuple(databaseId, recordName);
    datatuple* ret = get(recordKey);
    datatuple::freetuple(recordKey);
    return ret;
}

ResponseCode::type LSMServerHandler::
put(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    return mapkeeper::ResponseCode::Success;
}

ResponseCode::type LSMServerHandler::
insert(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return mapkeeper::ResponseCode::MapNotFound;
    }
    if(!blind_update) {
      datatuple* oldRecordBody = get(id, recordName);
      if (oldRecordBody != NULL) {
        if(oldRecordBody->isDelete()) {
          datatuple::freetuple(oldRecordBody);
        } else {
          datatuple::freetuple(oldRecordBody);
          return mapkeeper::ResponseCode::RecordExists;
        }
      }
    }

    datatuple* tup = buildTuple(id, recordName, recordBody);
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
insertMany(const std::string& databaseName, const std::vector<Record> & records)
{
    return mapkeeper::ResponseCode::Error;
}

ResponseCode::type LSMServerHandler::
update(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return mapkeeper::ResponseCode::MapNotFound;
    }
    if(!blind_update) {
      datatuple* oldRecordBody = get(id, recordName);
      if (oldRecordBody == NULL) {
        return mapkeeper::ResponseCode::RecordNotFound;
      }
      datatuple::freetuple(oldRecordBody);
    }
    datatuple* tup = buildTuple(id, recordName, recordBody);
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
remove(const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return mapkeeper::ResponseCode::MapNotFound;
    }
    datatuple* oldRecordBody = get(id, recordName);
    if (oldRecordBody == NULL) {
        return mapkeeper::ResponseCode::RecordNotFound;
    }
    datatuple::freetuple(oldRecordBody);
    datatuple* tup = buildTuple(id, recordName);
    return insert(tup);
}

datatuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName)
{
    return buildTuple(databaseId, recordName, NULL, DELETE);
}

datatuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName, const std::string& recordBody)
{
    return buildTuple(databaseId, recordName, recordBody.c_str(), recordBody.size());
}

datatuple* LSMServerHandler::
buildTuple(uint32_t databaseId, const std::string& recordName, const void* body, uint32_t bodySize)
{
    uint32_t keySize = sizeof(databaseId) + recordName.size();
    unsigned char* key = (unsigned char*)malloc(keySize);
    *(uint32_t*)key = htonl(databaseId);
    memcpy(((uint32_t*)key) + 1, recordName.c_str(), recordName.size());
    datatuple *tup = datatuple::create(key, keySize, body, bodySize);
    free(key);
    return tup;
}
