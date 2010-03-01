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
extern "C" {
	#define DEBUG(...) /* */
}

struct logstore_handle_t {
	char *host;
	int portnum;
	int timeout;
	struct sockaddr_in serveraddr;
	struct hostent* server;
	int server_socket;
};

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

    DEBUG("LogStore start\n");

    return ret;
}

static inline void close_conn(logstore_handle_t *l) {
	perror("read/write err.. conn closed.\n");
	close(l->server_socket); //close the connection
    l->server_socket = -1;
}
datatuple *
logstore_client_op(logstore_handle_t *l,
          uint8_t opcode,  datatuple * tuple, datatuple * tuple2, uint64_t count)
{

    if(l->server_socket < 0)
    {
        l->server_socket = socket(AF_INET, SOCK_STREAM, 0);

        if (l->server_socket < 0)
        {
            perror("ERROR opening socket.\n");
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
            perror("ERROR on setting socket option TCP_NODELAY.\n");
            return 0;
        }


        /* connect: create a connection with the server */
        if (connect(l->server_socket, (sockaddr*) &(l->serveraddr), sizeof(l->serveraddr)) < 0)
        {
            perror("ERROR connecting\n");
            return 0;
        }

        DEBUG("sock opened %d\n", l->server_socket);
    }



    //send the opcode
    if( writetosocket(l->server_socket, &opcode, sizeof(opcode))  ) { close_conn(l); return 0; }

    //send the first tuple
    if( writetupletosocket(l->server_socket, tuple)               ) { close_conn(l); return 0; }

    //send the second tuple
    if( writetupletosocket(l->server_socket, tuple2)              ) { close_conn(l); return 0; }

    if( count != (uint64_t)-1) {
    	if( writecounttosocket(l->server_socket, count)           ) { close_conn(l); return 0; }
    }


    network_op_t rcode = readopfromsocket(l->server_socket,LOGSTORE_SERVER_RESPONSE);

    if( opiserror(rcode)                                          ) { close_conn(l); return 0; }

    datatuple * ret = 0;

    if(rcode == LOGSTORE_RESPONSE_SENDING_TUPLES)
    {	int err;
		uint64_t count = 0; // XXX
		datatuple *nxt;
    	while(( nxt = readtuplefromsocket(l->server_socket, &err) )) {
    		if(ret) datatuple::freetuple(ret); // XXX
    		ret = nxt;
    		if(err) { close_conn(l); return 0; }
    		count++;
    	}
    	if(count > 1) { fprintf(stderr, "XXX return count: %lld but iterators are not handled by the logstore_client_op api\n", count); }
    } else if(rcode == LOGSTORE_RESPONSE_SUCCESS) {
    	ret = tuple ? tuple : datatuple::create("", 1);
    } else {
    	assert(rcode == LOGSTORE_RESPONSE_FAIL); // if this is an invalid response, we should have noticed above
    	ret = 0;
    }

    return ret;
}

int logstore_client_close(logstore_handle_t* l) {
    if(l->server_socket > 0)
    {
        writetosocket(l->server_socket, (char*) &OP_DONE, sizeof(uint8_t));

        close(l->server_socket);
        DEBUG("socket closed %d\n.", l->server_socket);
    }
    free(l->host);
    free(l);
    return 0;
}
