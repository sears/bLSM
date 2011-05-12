#include <dht_persistent_store/PersistentStore.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <iostream>
using namespace std;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

int main(int argc, char **argv) {
    boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9090));
    boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

    sherpa::PersistentStoreClient client(protocol);
    transport->open();
    socket->setNoDelay(true);
    sherpa::BinaryResponse getResponse;
    sherpa::RecordListResponse scanResponse;
    std::string db = "db";
    std::string db1 = "db1";
    cout << client.addDatabase(db) << endl;;
    cout << client.insert(db, "kkkkkkkkkkkkk1", "v1") << endl;
    cout << client.insert(db, "kkkkkkkkkkkkk2", "v2") << endl;
    cout << client.insert(db, "kkkkkkkkkkkkk3", "v3") << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk1");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk2");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk3");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;

    cout << client.update(db, "k0", "v11") << endl;
    cout << client.update(db, "kkkkkkkkkkkkk1", "v11") << endl;
    cout << client.update(db, "kkkkkkkkkkkkk2", "v12") << endl;
    cout << client.update(db, "kkkkkkkkkkkkk3", "v13") << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk1");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk2");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, db, "kkkkkkkkkkkkk3");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;

/*
    client.get(getResponse, "fdsafdasfdasfdsaf", "kkkkkkkkkkkkk3");
    cout << getResponse.responseCode << endl;

    client.get(getResponse, db, "k4");
    cout << getResponse.responseCode << endl;
    */


    client.scan(scanResponse, db, sherpa::ScanOrder::Ascending, "", true, "", true, 100, 100);
    std::vector<sherpa::Record>::iterator itr;
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }
    std::cout << std::endl;


    client.scan(scanResponse, db, sherpa::ScanOrder::Ascending, "kkkkkkkkkkkkk1", false, "kkkkkkkkkkkkk3", false, 100, 100);
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }
    std::cout << std::endl;

    client.scan(scanResponse, db, sherpa::ScanOrder::Ascending, "kkkkkkkkkkkkk1", true, "kkkkkkkkkkkkk3", true, 100, 100);
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }
    std::cout << std::endl;

    client.scan(scanResponse, db, sherpa::ScanOrder::Ascending, "kkkkkkkkkkkkk2", true, "kkkkkkkkkkkkk2", true, 100, 100);
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }
    std::cout << std::endl;

    client.scan(scanResponse, db, sherpa::ScanOrder::Ascending, "k", true, "kkkkkkkkkkkkk4", true, 100, 100);
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }
    std::cout << std::endl;


    cout << "adding db one more time" << endl;
    cout << client.addDatabase(db) << endl;;

    cout << client.addDatabase(db1) << endl;;
    cout << client.insert(db1, "new key", "new value") << endl;
    client.get(getResponse, db1, "new key");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;

    client.get(getResponse, db, "new key");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    return 0;

    /*
    cout << client.remove("michi", "k1") << endl;
    client.get(getResponse, "michi", "k1");
    cout << getResponse.responseCode << endl;
    transport->close();
    */
    return 0;
}
