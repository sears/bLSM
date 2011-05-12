#include <dht_persistent_store/PersistentStore.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include "logstore.h"
#include "datatuple.h"
#include "LSMServerHandler.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;

using namespace sherpa;

int main(int argc, char **argv) {
    shared_ptr<LSMServerHandler> handler(new LSMServerHandler(argc, argv));
    shared_ptr<TProcessor> processor(new PersistentStoreProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(9090));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(32);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadPoolServer server(processor, serverTransport, transportFactory, protocolFactory, threadManager);
    server.serve();
    return 0;
}
