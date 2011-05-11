#include <dht_persistent_store/PersistentStore.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using namespace sherpa;
using boost::shared_ptr;

class LSMServerHandler : virtual public PersistentStoreIf {
public:
    LSMServerHandler(int argc, char **argv);
    ResponseCode::type ping();
    ResponseCode::type addDatabase(const std::string& databaseName);
    ResponseCode::type dropDatabase(const std::string& databaseName);
    void listDatabases(StringListResponse& _return);
    void scan(RecordListResponse& _return, const std::string& databaseName, const ScanOrder::type order, 
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);
    void get(BinaryResponse& _return, const std::string& databaseName, const std::string& recordName);
    ResponseCode::type insert(const std::string& databaseName, const std::string& recordName, const std::string& recordBody);
    ResponseCode::type insertMany(const std::string& databaseName, const std::vector<Record> & records);
    ResponseCode::type update(const std::string& databaseName, const std::string& recordName, const std::string& recordBody);
    ResponseCode::type remove(const std::string& databaseName, const std::string& recordName);

private:
    logtable<datatuple>* ltable_;

};
