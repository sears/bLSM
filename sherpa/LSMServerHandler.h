/*
 * LSMServerHandler.h
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
 */
#include "MapKeeper.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using namespace mapkeeper;
using boost::shared_ptr;

class LSMServerHandler : virtual public MapKeeperIf {
public:
    LSMServerHandler(int argc, char **argv);
    ResponseCode::type ping();
    ResponseCode::type shutdown();
    ResponseCode::type addMap(const std::string& databaseName);
    ResponseCode::type dropMap(const std::string& databaseName);
    void listMaps(StringListResponse& _return);
    void scan(RecordListResponse& _return, const std::string& databaseName, const ScanOrder::type order, 
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);
    void get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName);
    ResponseCode::type put(const std::string& databaseName, const std::string& recordName, const std::string& recordBody);
    ResponseCode::type insert(const std::string& databaseName, const std::string& recordName, const std::string& recordBody);
    ResponseCode::type insertMany(const std::string& databaseName, const std::vector<Record> & records);
    ResponseCode::type update(const std::string& databaseName, const std::string& recordName, const std::string& recordBody);
    ResponseCode::type remove(const std::string& databaseName, const std::string& recordName);
    short port;

private:
    ResponseCode::type insert(datatuple* tuple);
    uint32_t getDatabaseId(const std::string& databaseName);
    uint32_t nextDatabaseId();
    datatuple* get(uint32_t databaseId, const std::string& recordName);
    datatuple* get(datatuple* tuple);
    datatuple* buildTuple(uint32_t databaseId, const std::string& recordName);
    datatuple* buildTuple(uint32_t databaseId, const std::string& recordName, const std::string& recordBody);
    datatuple* buildTuple(uint32_t databaseId, const std::string& recordName, const void* body, uint32_t bodySize);
    void initNextDatabaseId();
    blsm* ltable_;
    uint32_t nextDatabaseId_;
    pthread_mutex_t mutex_;
};
