/*
 * tcpclient.h
 *
 *  Created on: Feb 2, 2010
 *      Author: sears
 */

#ifndef TCPCLIENT_H_
#define TCPCLIENT_H_

#include "datatuple.h"

typedef struct logstore_handle_t logstore_handle_t;

logstore_handle_t * logstore_client_open(const char *host, int portnum, int timeout);

datatuple * logstore_client_op(logstore_handle_t* l,
					  uint8_t opcode,
					  datatuple *tuple = NULL, datatuple *tuple2 = NULL,
					  uint64_t count = (uint64_t)-1);

int logstore_client_close(logstore_handle_t* l);


#endif /* TCPCLIENT_H_ */
