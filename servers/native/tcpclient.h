/*
 * tcpclient.h
 *
 * Copyright 2010-2012 Yahoo! Inc.
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
 *
 *  Created on: Feb 2, 2010
 *      Author: sears
 */

#ifndef TCPCLIENT_H_
#define TCPCLIENT_H_

#include "datatuple.h"

typedef struct logstore_handle_t logstore_handle_t;

logstore_handle_t * logstore_client_open(const char *host, int portnum, int timeout);

dataTuple * logstore_client_op(logstore_handle_t* l,
					uint8_t opcode,
					dataTuple *tuple = NULL, dataTuple *tuple2 = NULL,
					uint64_t count = (uint64_t)-1);

uint8_t logstore_client_op_returns_many(logstore_handle_t *l,
					uint8_t opcode,
					dataTuple * tuple = NULL, dataTuple * tuple2 = NULL,
					uint64_t count = (uint64_t)-1);

dataTuple * logstore_client_next_tuple(logstore_handle_t *l);
uint8_t logstore_client_send_tuple(logstore_handle_t *l, dataTuple *tuple = NULL);
int logstore_client_close(logstore_handle_t* l);


#endif /* TCPCLIENT_H_ */
