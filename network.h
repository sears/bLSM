/*
 * network.h
 *
 *  Created on: Feb 2, 2010
 *      Author: sears
 */

#ifndef NETWORK_H_
#define NETWORK_H_

#include <stdio.h>
#include <errno.h>

#include <string>
typedef unsigned char byte;
#include <cstring>
#include <assert.h>

typedef uint8_t network_op_t;

typedef uint32_t len_t ;
static const len_t DELETE = ((len_t)0) - 1;

#include <datatuple.h>

//server codes
static const network_op_t LOGSTORE_FIRST_RESPONSE_CODE = 1;
static const network_op_t LOGSTORE_RESPONSE_SUCCESS = 1;
static const network_op_t LOGSTORE_RESPONSE_FAIL = 2;
static const network_op_t LOGSTORE_RESPONSE_SENDING_TUPLES = 3;
static const network_op_t LOGSTORE_LAST_RESPONSE_CODE = 3;

//client codes
static const network_op_t LOGSTORE_FIRST_REQUEST_CODE  = 8;
static const network_op_t OP_INSERT              = 8;   // Create, Update, Delete
static const network_op_t OP_FIND                = 9;   // Read
static const network_op_t OP_SCAN                = 11;
static const network_op_t OP_DONE                = 12;  // Please close the connection.

static const network_op_t OP_DBG_BLOCKMAP            = 13;

static const network_op_t LOGSTORE_LAST_REQUEST_CODE   = 13;

//error codes
static const network_op_t LOGSTORE_FIRST_ERROR  = 28;
static const network_op_t LOGSTORE_CONN_CLOSED_ERROR = 28;   // Unexpected EOF
static const network_op_t LOGSTORE_SOCKET_ERROR = 29;   // The OS returned an error.
static const network_op_t LOGSTORE_REMOTE_ERROR = 30;   // The other side didn't like our request
static const network_op_t LOGSTORE_PROTOCOL_ERROR = 31; // The other side responeded with gibberish.
static const network_op_t LOGSTORE_LAST_ERROR   = 31;
static const network_op_t OP_INVALID = 32;

typedef enum {
  LOGSTORE_CLIENT_REQUEST,
  LOGSTORE_SERVER_RESPONSE
} logstore_opcode_type;

static inline int readfromsocket(int sockd, void *buf, ssize_t count)
{

	ssize_t n = 0;

	while( n < count )
	{
		ssize_t i = read( sockd, ((byte*)buf) + n, count - n);
		if(i == -1) {
			perror("readfromsocket failed");
			return errno;
		} else if(i == 0) {
			errno = EOF;
			return errno;
		}
		n += i;
	}
	return 0;

}

static inline int writetosocket(int sockd, const void *buf, ssize_t count)
{
	ssize_t n = 0;

	while( n < count )
	{
		ssize_t i = write( sockd, ((byte*)buf) + n, count - n);
		if(i == -1) {
			perror("writetosocket failed");
			return errno;
		} else if(i == 0) {
			errno = EOF;
			return errno;
		}
		n += i;
	}
	return 0;
}

static inline bool opiserror(network_op_t op) {
  return (LOGSTORE_FIRST_ERROR <= op && op <= LOGSTORE_LAST_ERROR);
}
static inline bool opisrequest(network_op_t op) {
  return (LOGSTORE_FIRST_REQUEST_CODE <= op && op <= LOGSTORE_LAST_REQUEST_CODE);
}
static inline bool opisresponse(network_op_t op) {
  return (LOGSTORE_FIRST_RESPONSE_CODE <= op && op <= LOGSTORE_LAST_RESPONSE_CODE);
}

static inline network_op_t readopfromsocket(int sockd, logstore_opcode_type type) {
  network_op_t ret;
  ssize_t n = read(sockd, &ret, sizeof(network_op_t));
  if(n == sizeof(network_op_t)) {
    // done.
  } else if(n == 0) { // EOF
    perror("Socket closed mid request.");
    return LOGSTORE_CONN_CLOSED_ERROR;
  } else {
    assert(n == -1); // sizeof(network_op_t) is 1, so short reads are impossible.
    perror("Could not read opcode from socket");
    return LOGSTORE_SOCKET_ERROR;
  }
  // sanity checking
  switch(type) {
  case LOGSTORE_CLIENT_REQUEST: {
    if(!(opisrequest(ret) || opiserror(ret))) {
      fprintf(stderr, "Read invalid request code %d\n", (int)ret);
      if(opisresponse(ret)) {
	fprintf(stderr, "(also, the request code is a valid response code)\n");
      }
      ret = LOGSTORE_PROTOCOL_ERROR;
    }
  } break;
  case LOGSTORE_SERVER_RESPONSE: {
    if(!(opisresponse(ret) || opiserror(ret))) {
      fprintf(stderr, "Read invalid response code %d\n", (int)ret);
      if(opisrequest(ret)) {
	fprintf(stderr, "(also, the response code is a valid request code)\n");
      }
      ret = LOGSTORE_PROTOCOL_ERROR;
    }

  }
  }
  return ret;
}
static inline int writeoptosocket(int sockd, network_op_t op) {
  assert(opiserror(op) || opisrequest(op) || opisresponse(op));
  return writetosocket(sockd, &op, sizeof(network_op_t));
}

/**
	Iterator wire format:

	  LOGSTORE_RESPONSE_SENDING_TUPLES
	  TUPLE
	  TUPLE
	  TUPLE
	  datatuple::DELETE

 */

/**
    @param sockd The socket.
    @param error will be set to zero on succes, a logstore error number on failure
    @return a datatuple, or NULL.
 */
static inline datatuple* readtuplefromsocket(int sockd, int * err) {

	len_t keylen, datalen, buflen;

    if(( *err = readfromsocket(sockd, &keylen, sizeof(keylen))   )) return NULL;
    if(keylen == DELETE) return NULL; // *err is zero.
    if(( *err = readfromsocket(sockd, &datalen, sizeof(datalen)) )) return NULL;

    buflen = datatuple::length_from_header(keylen, datalen);
    byte*  bytes = (byte*) malloc(buflen);

    if(( *err = readfromsocket(sockd, bytes, buflen)             )) return NULL;

	return datatuple::from_bytes(keylen, datalen, bytes);   // from_bytes consumes the buffer.
}

static inline int writeendofiteratortosocket(int sockd) {
	return writetosocket(sockd, &DELETE, sizeof(DELETE));
}
static inline int writetupletosocket(int sockd, const datatuple* tup) {
	len_t keylen, datalen;
	int err;

	if(tup == NULL) {
		if(( err = writeendofiteratortosocket(sockd)                                         )) return err;
	} else {
		const byte* buf = tup->get_bytes(&keylen, &datalen);
		if(( err = writetosocket(sockd, &keylen, sizeof(keylen))                             )) return err;
		if(( err = writetosocket(sockd, &datalen, sizeof(datalen))                           )) return err;
		if(( err = writetosocket(sockd, buf, datatuple::length_from_header(keylen, datalen)) )) return err;
	}
	return 0;

}
static inline uint64_t readcountfromsocket(int sockd, int *err) {
	uint64_t ret;
	*err = readfromsocket(sockd, &ret, sizeof(ret));
	return ret;
}
static inline int writecounttosocket(int sockd, uint64_t count) {
	return writetosocket(sockd, &count, sizeof(count));
}

#endif /* NETWORK_H_ */
