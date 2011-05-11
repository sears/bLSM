#include <stasis/transactional.h>
#include <stasis/logger/safeWrites.h>
#undef end
#undef try
#undef begin

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
}

ResponseCode::type LSMServerHandler::
ping() 
{
    return sherpa::ResponseCode::Ok;
}

ResponseCode::type LSMServerHandler::
addDatabase(const std::string& databaseName) 
{
#if 0
    LOG_DEBUG(__FUNCTION__);
    uint32_t databaseId;
    seq_.get(databaseId);
    databaseId = databaseId;
    std::stringstream out;
    out << databaseId;
    LOG_DEBUG("ID for tablet " << databaseName<< ": " << databaseId);
    Bdb::ResponseCode rc = databaseIds_.insert(databaseName, out.str());
    if (rc == Bdb::KeyExists) {
        LOG_DEBUG("Database " << databaseName << " already exists");
        return sherpa::ResponseCode::DatabaseExists;
    }
#endif
    return sherpa::ResponseCode::Ok;
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
#if 0
    BdbIterator itr;
    uint32_t id;
    _return.responseCode = getDatabaseId(databaseName, id);
    if (_return.responseCode != sherpa::ResponseCode::Ok) {
        return;
    }
    boost::thread_specific_ptr<RecordBuffer> buffer;
    if (buffer.get() == NULL) {
        buffer.reset(new RecordBuffer(keyBufferSizeBytes_, valueBufferSizeBytes_));
    }
    if (endKey.empty()) {
        insertDatabaseId(const_cast<std::string&>(endKey), id + 1);
    } else {
        insertDatabaseId(const_cast<std::string&>(endKey), id);
    }
    insertDatabaseId(const_cast<std::string&>(startKey), id);
    itr.init(db_[id % numPartitions_], const_cast<std::string&>(startKey), startKeyIncluded, const_cast<std::string&>(endKey), endKeyIncluded, order, *buffer);

    int32_t resultSize = 0;
    _return.responseCode = sherpa::ResponseCode::Ok;
    while ((maxRecords == 0 || (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        BdbIterator::ResponseCode rc = itr.next(*buffer);
        if (rc == BdbIterator::ScanEnded) {
            _return.responseCode = sherpa::ResponseCode::ScanEnded;
            break;
        } else if (rc != BdbIterator::Ok) {
            _return.responseCode = sherpa::ResponseCode::Error;
            break;
        }
        Record rec;
        rec.key.assign(buffer->getKeyBuffer() + sizeof(id), buffer->getKeySize() - sizeof(id));
        rec.value.assign(buffer->getValueBuffer(), buffer->getValueSize());
        _return.records.push_back(rec);
        resultSize += buffer->getKeySize() + buffer->getValueSize();
    } 
#endif
}

void LSMServerHandler::
get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName) 
{
#if 0
    LOG_DEBUG(__FUNCTION__);
    uint32_t id;
    _return.responseCode = getDatabaseId(databaseName, id);
    if (_return.responseCode != sherpa::ResponseCode::Ok) {
        return;
    }
    insertDatabaseId(const_cast<std::string&>(recordName), id);

    boost::thread_specific_ptr<RecordBuffer> buffer;
    if (buffer.get() == NULL) {
        buffer.reset(new RecordBuffer(keyBufferSizeBytes_, valueBufferSizeBytes_));
    }
    Bdb::ResponseCode dbrc = db_[id % numPartitions_]->get(recordName, _return.value, *buffer);
    if (dbrc == Bdb::Ok) {
        _return.responseCode = sherpa::ResponseCode::Ok;
    } else if (dbrc == Bdb::KeyNotFound) {
        _return.responseCode = sherpa::ResponseCode::RecordNotFound;
    } else {
        _return.responseCode = sherpa::ResponseCode::Error;
    }
#endif
    _return.responseCode = sherpa::ResponseCode::Error;
}

/*
ResponseCode::type LSMServerHandler::
getDatabaseId(const std::string& databaseName, uint32_t& id)
{
    std::string idString;
    boost::thread_specific_ptr<RecordBuffer> buffer;
    if (buffer.get() == NULL) {
        buffer.reset(new RecordBuffer(keyBufferSizeBytes_, valueBufferSizeBytes_));
    }
    Bdb::ResponseCode rc = databaseIds_.get(databaseName, idString, *buffer);
    if (rc == Bdb::KeyNotFound) {
        return sherpa::ResponseCode::DatabaseNotFound;
    }
    std::istringstream iss(idString);
    if ((iss >> id).fail()) {
        return sherpa::ResponseCode::Error;
    }
    LOG_DEBUG("database id for " << databaseName << "=" << id);
    return sherpa::ResponseCode::Ok;
}
*/

ResponseCode::type LSMServerHandler::
insert(const std::string& databaseName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
/*
    LOG_DEBUG(__FUNCTION__);
    uint32_t id;
    ResponseCode::type rc = getDatabaseId(databaseName, id);
    if (rc != sherpa::ResponseCode::Ok) {
        return rc;
    }
    insertDatabaseId(const_cast<std::string&>(recordName), id);
    LOG_DEBUG("id=" << id);
    LOG_DEBUG("numPartitions=" << numPartitions_);
    LOG_DEBUG("id % numPartitions=" << id % numPartitions_);
    Bdb::ResponseCode dbrc = db_[id % numPartitions_]->insert(recordName, recordBody);
    if (dbrc == Bdb::KeyExists) {
        return sherpa::ResponseCode::RecordExists;
    } else if (dbrc != Bdb::Ok) {
        return sherpa::ResponseCode::Error;
    }
    */
    return sherpa::ResponseCode::Ok;
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
/*
    uint32_t id;
    ResponseCode::type rc = getDatabaseId(databaseName, id);
    if (rc != sherpa::ResponseCode::Ok) {
        return rc;
    }

    insertDatabaseId(const_cast<std::string&>(recordName), id);
    Bdb::ResponseCode dbrc = db_[id % numPartitions_]->update(recordName, recordBody);
    if (dbrc == Bdb::Ok) {
        return sherpa::ResponseCode::Ok;
    } else if (dbrc == Bdb::KeyNotFound) {
        return sherpa::ResponseCode::RecordNotFound;
    } else {
        return sherpa::ResponseCode::Error;
    }
    */
    return sherpa::ResponseCode::Error;
}

ResponseCode::type LSMServerHandler::
remove(const std::string& databaseName, const std::string& recordName) 
{
/*
    uint32_t id;
    ResponseCode::type rc = getDatabaseId(databaseName, id);
    if (rc != sherpa::ResponseCode::Ok) {
        return rc;
    }
    insertDatabaseId(const_cast<std::string&>(recordName), id);
    Bdb::ResponseCode dbrc = db_[id % numPartitions_]->remove(recordName);
    if (dbrc == Bdb::Ok) {
        return sherpa::ResponseCode::Ok;
    } else if (dbrc == Bdb::KeyNotFound) {
        return sherpa::ResponseCode::RecordNotFound;
    } else {
    }
*/
    return sherpa::ResponseCode::Error;
}
