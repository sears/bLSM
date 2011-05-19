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
            stasis_buffer_manager_size = (1024LL * 1024LL * 1024LL * 2LL) / PAGE_SIZE;  // 4GB total
            c0_size =                     1024LL * 1024LL * 1024LL * 2LL;
            printf("note: running w/ 2GB c0 for benchmarking\n"); // XXX build a separate test server and deployment server?
        } else if(!strcmp(argv[i], "--log-mode")) {
            i++;
            log_mode = atoi(argv[i]);
        } else if(!strcmp(argv[i], "--expiry-delta")) {
            i++;
            expiry_delta = atoi(argv[i]);
        } else {
            fprintf(stderr, "Usage: %s [--test|--benchmark] [--log-mode <int>] [--expiry-delta <int>]", argv[0]);
            abort();
        }
    }

    pthread_mutex_init(&mutex_, 0);
    logtable<datatuple>::init_stasis();

    int xid = Tbegin();


    recordid table_root = ROOT_RECORD;
    {
        ltable_ = new logtable<datatuple>(log_mode, c0_size);
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
    //logtable<datatuple>::deinit_stasis();
    initNextDatabaseId();
}

void LSMServerHandler::
initNextDatabaseId() 
{
    nextDatabaseId_ = 1;
    uint32_t id = 0;
    datatuple* start = buildTuple(id, "");
    datatuple* end = buildTuple(id + 1, "");
    logtable<datatuple>::iterator* itr = new logtable<datatuple>::iterator(ltable_, start);
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
    return sherpa::ResponseCode::Ok;
}

ResponseCode::type LSMServerHandler::
insert(datatuple* tuple)
{
    ltable_->insertTuple(tuple);
    datatuple::freetuple(tuple);
    return sherpa::ResponseCode::Ok;
}

ResponseCode::type LSMServerHandler::
addDatabase(const std::string& databaseName) 
{
    uint32_t id = nextDatabaseId();
    datatuple* tup = buildTuple(0, databaseName, (void*)&id, (uint32_t)(sizeof(id)));
    datatuple* ret = get(tup);
    if (ret) {
        datatuple::freetuple(ret);
        return sherpa::ResponseCode::DatabaseExists;
    }
    return insert(tup);
}

/**
 * TODO:
 * Don't just remove database from databaseIds. You need to delete
 * all the records!
 */
ResponseCode::type LSMServerHandler::
dropDatabase(const std::string& databaseName) 
{
#if 0
    Bdb::ResponseCode rc = databaseIds_.remove(databaseName);
    if (rc == Bdb::KeyNotFound) {
        return sherpa::ResponseCode::DatabaseNotFound;
    } else if (rc != Bdb::Ok) {
        return sherpa::ResponseCode::Error;
    } else {
        return sherpa::ResponseCode::Ok;
    }
#endif
        return sherpa::ResponseCode::Ok;
}

void LSMServerHandler::
listDatabases(StringListResponse& _return) 
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
        _return.responseCode = sherpa::ResponseCode::DatabaseNotFound;
        return;
    }
 
    datatuple* start = buildTuple(id, startKey);
    datatuple* end;
    if (endKey.empty()) {
        end = buildTuple(id + 1, endKey);
    } else {
        end = buildTuple(id, endKey);
    }
    logtable<datatuple>::iterator* itr = new logtable<datatuple>::iterator(ltable_, start);

    int32_t resultSize = 0;

    while ((maxRecords == 0 || (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        datatuple* current = itr->getnext();
        if (current == NULL) {
            _return.responseCode = sherpa::ResponseCode::ScanEnded;
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
            _return.responseCode = sherpa::ResponseCode::ScanEnded;
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
    //return ltable_->findTuple_first(-1, tuple->strippedkey(), tuple->strippedkeylen());
    datatuple* tup = ltable_->findTuple_first(-1, tuple->rawkey(), tuple->rawkeylen());
    return tup;
}

void LSMServerHandler::
get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        // database not found
        _return.responseCode = sherpa::ResponseCode::DatabaseNotFound;
        return;
    }
    
    datatuple* recordBody = get(id, recordName);
    if (recordBody == NULL) {
        // record not found
        _return.responseCode = sherpa::ResponseCode::RecordNotFound;
        return;
    }
    _return.responseCode = sherpa::ResponseCode::Ok;
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
insert(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return sherpa::ResponseCode::DatabaseNotFound;
    }
    datatuple* oldRecordBody = get(id, recordName);
    if (oldRecordBody != NULL) {
        datatuple::freetuple(oldRecordBody);
        return sherpa::ResponseCode::RecordExists;
    }

    datatuple* tup = buildTuple(id, recordName, recordBody);
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
insertMany(const std::string& databaseName, const std::vector<Record> & records)
{
    return sherpa::ResponseCode::Error;
}

ResponseCode::type LSMServerHandler::
update(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return sherpa::ResponseCode::DatabaseNotFound;
    }
    datatuple* oldRecordBody = get(id, recordName);
    if (oldRecordBody == NULL) {
        return sherpa::ResponseCode::RecordNotFound;
    }
    datatuple::freetuple(oldRecordBody);
    datatuple* tup = buildTuple(id, recordName, recordBody);
    return insert(tup);
}

ResponseCode::type LSMServerHandler::
remove(const std::string& databaseName, const std::string& recordName) 
{
    uint32_t id = getDatabaseId(databaseName);
    if (id == 0) {
        return sherpa::ResponseCode::DatabaseNotFound;
    }
    datatuple* oldRecordBody = get(id, recordName);
    if (oldRecordBody == NULL) {
        return sherpa::ResponseCode::RecordNotFound;
    }
    datatuple::freetuple(oldRecordBody);
    datatuple* tup = buildTuple(id, recordName);
    return insert(tup);
}

/*
void BdbServerHandler::
insertDatabaseId(std::string& str, uint32_t id)
{
    LOG_DEBUG("prepending id: " << id);
    uint32_t newid = htonl(id);
    LOG_DEBUG(
            (int)(((uint8_t*)(&newid))[0]) << " " << 
            (int)(((uint8_t*)(&newid))[1]) << " " << 
            (int)(((uint8_t*)(&newid))[2]) <<" " << 
            (int)(((uint8_t*)(&newid))[3])
    );
    str.insert(0, (const char*)&newid, sizeof(newid));
}
*/

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
