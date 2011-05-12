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

    cout << client.addDatabase("michi0") << endl;;
    cout << client.insert("michi0", "k1", "v1") << endl;
    cout << client.insert("michi0", "k2", "v1") << endl;
    cout << client.insert("michi0", "k3", "v1") << endl;
    client.get(getResponse, "michi0", "k1");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, "michi0", "k2");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;
    client.get(getResponse, "michi0", "k3");
    cout << getResponse.responseCode << endl;
    cout << getResponse.value << endl;

    client.get(getResponse, "fdsafdasfdasfdsaf", "k3");
    cout << getResponse.responseCode << endl;

    client.get(getResponse, "michi0", "k4");
    cout << getResponse.responseCode << endl;
    return 0;


    client.scan(scanResponse, "michi", sherpa::ScanOrder::Ascending, "", true, "", true, 100, 100);
    std::vector<sherpa::Record>::iterator itr;
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }

    client.scan(scanResponse, "michi", sherpa::ScanOrder::Descending, "", true, "", true, 100, 100);
    for (itr = scanResponse.records.begin(); itr != scanResponse.records.end(); itr++) {
        cout << itr->key << " " << itr->value << endl;
    }


    cout << client.remove("michi", "k1") << endl;
    client.get(getResponse, "michi", "k1");
    cout << getResponse.responseCode << endl;
    transport->close();
    return 0;
}
