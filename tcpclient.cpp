/*
 * tcpclient.cpp
 *
 *  Created on: Feb 2, 2010
 *      Author: sears
 */

#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <assert.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "tcpclient.h"
#include "datatuple.h"
#include "network.h"

//	const char *appid;
//	const char *region;
struct logstore_handle_t {
	char *host;
	int portnum;
	int timeout;
	struct sockaddr_in serveraddr;
	struct hostent* server;
	int server_socket;
};

//LogStoreDBImpl::LogStoreDBImpl(const TestSettings & testSettings):
//	const char *appid, int timeout, const char *region, int portnum){
//    host_(testSettings.host()),
//    appid_(testSettings.appID()),
//    timeout_(testSettings.timeout()),
//    region_(testSettings.myRegion()),
//    routerLatency_(0.0),
//    suLatency_(0.0)
//    const std::string& appid_;
//    const int timeout_;
//    const std::string& region_;
//
//    int portnum;
//
//    int server_socket;
//
//    struct sockaddr_in serveraddr;
//    struct hostent *server;
//    ret->server_socket = -1;
//    portnum = 32432; //this should be an argument.

logstore_handle_t * logstore_client_open(const char *host, int portnum, int timeout) {
	logstore_handle_t *ret = (logstore_handle_t*) malloc(sizeof(*ret));
	ret->host = strdup(host);
	ret->portnum = portnum;
	if(ret->portnum == 0) { ret->portnum = 32432; }
	ret->timeout = timeout;
	ret->server_socket = -1;

    ret->server = gethostbyname(ret->host);
    if (ret->server == NULL) {
        fprintf(stderr,"ERROR, no such host as %s\n", ret->host);
        free(ret->host); free(ret); return 0;
    }

    /* build the server's Internet address */
    bzero((char *) &ret->serveraddr, sizeof(ret->serveraddr));
    ret->serveraddr.sin_family = AF_INET;
    bcopy((char *)ret->server->h_addr,
	  (char *)&ret->serveraddr.sin_addr.s_addr, ret->server->h_length);
    ret->serveraddr.sin_port = htons(ret->portnum);

    printf("LogStore start\n");

    return ret;
}

datatuple *
logstore_client_op(logstore_handle_t *l,
//		  int *server_socket,
//          struct sockaddr_in serveraddr,
//          struct hostent *server,
          uint8_t opcode,  datatuple &tuple)
{

    if(l->server_socket < 0)
    {
        l->server_socket = socket(AF_INET, SOCK_STREAM, 0);

        if (l->server_socket < 0)
        {
            printf("ERROR opening socket.\n");
        return 0;
        }


        int flag = 1;
        int result = setsockopt(l->server_socket,            /* socket affected */
                                IPPROTO_TCP,     /* set option at TCP level */
                                TCP_NODELAY,     /* name of option */
                                (char *) &flag,  /* the cast is historical
                                                    cruft */
                                sizeof(int));    /* length of option value */
        if (result < 0)
        {
            printf("ERROR on setting socket option TCP_NODELAY.\n");
            return 0;
        }


        /* connect: create a connection with the server */
        if (connect(l->server_socket, (sockaddr*) &(l->serveraddr), sizeof(l->serveraddr)) < 0)
        {
            printf("ERROR connecting\n");
            return 0;
        }

        printf("sock opened %d\n", l->server_socket);
    }


    //send the opcode
    int n = write(l->server_socket, (byte*) &opcode, sizeof(uint8_t));
    assert(n == sizeof(uint8_t));

    //send the tuple
    n = write(l->server_socket, (byte*) tuple.keylen, sizeof(uint32_t));
    assert( n == sizeof(uint32_t));

    n = write(l->server_socket, (byte*) tuple.datalen, sizeof(uint32_t));
    assert( n == sizeof(uint32_t));

    writetosocket(l->server_socket, (char*) tuple.key, *tuple.keylen);
    if(!tuple.isDelete() && *tuple.datalen != 0)
        writetosocket(l->server_socket, (char*) tuple.data, *tuple.datalen);

    //printf("\nssocket %d ", *server_socket);
    //read the reply code
    uint8_t rcode;
    n = read(l->server_socket, (byte*) &rcode, sizeof(uint8_t));
    if( n <= 0 )
    {
        printf("read err.. conn closed.\n");
        close(l->server_socket); //close the connection
        l->server_socket = -1;
        return 0;
    }

    //printf("rdone\n");
    datatuple * ret;
    if(rcode == OP_SENDING_TUPLE)
    {
        datatuple *rcvdtuple = (datatuple*)malloc(sizeof(datatuple));
        //read the keylen
        rcvdtuple->keylen = (uint32_t*) malloc(sizeof(uint32_t));
        n = read(l->server_socket, (char*) rcvdtuple->keylen, sizeof(uint32_t));
        assert(n == sizeof(uint32_t));
        //read the datalen
        rcvdtuple->datalen = (uint32_t*) malloc(sizeof(uint32_t));
        n = read(l->server_socket, (byte*) rcvdtuple->datalen, sizeof(uint32_t));
        assert(n == sizeof(uint32_t));
        //read key
        rcvdtuple->key = (byte*) malloc(*rcvdtuple->keylen);
        readfromsocket(l->server_socket, (char*) rcvdtuple->key, *rcvdtuple->keylen);
        if(!rcvdtuple->isDelete())
        {
            //read key
            rcvdtuple->data = (byte*) malloc(*rcvdtuple->datalen);
            readfromsocket(l->server_socket, (char*) rcvdtuple->data, *rcvdtuple->datalen);
        }

        ret = rcvdtuple;
    } else if(rcode == OP_SUCCESS) {
    	ret = &tuple;
    } else {
    	ret = 0;
    }

    return ret;
}

int logstore_client_close(logstore_handle_t* l) {
    if(l->server_socket > 0)
    {
        writetosocket(l->server_socket, (char*) &OP_DONE, sizeof(uint8_t));

        close(l->server_socket);
        printf("socket closed %d\n.", l->server_socket);
    }
    free(l->host);
    free(l);
    return 0;
}
