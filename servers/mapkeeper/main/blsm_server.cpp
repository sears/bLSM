#include "MapKeeper.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include "blsm.h"
#include "datatuple.h"
#include "blsmRequestHandler.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;

using namespace mapkeeper;

int main(int argc, char **argv) {
    shared_ptr<LSMServerHandler> handler(new LSMServerHandler(argc, argv));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(handler->port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(32);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    //threadManager->threadFactory(threadFactory);
    //threadManager->start();
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    printf("I'm using tthreaded server!");
    server.serve();
    return 0;
}
