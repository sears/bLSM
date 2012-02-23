/*
 * dump_blockmap.cc
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
 *  Created on: Feb 16, 2010
 *      Author: sears
 */

#include "../tcpclient.h"
#include "../network.h"
#include "../datatuple.h"

void usage(char * argv[]) {
	fprintf(stderr, "usage %s [host [port]]\n", argv[0]);
}
#include "util_main.h"
int main(int argc, char * argv[]) {
	int op = OP_DBG_BLOCKMAP;
	logstore_handle_t * l = util_open_conn(argc, argv);

    dataTuple * ret = logstore_client_op(l, op);
    if(ret == NULL) {
    	perror("Dump blockmap failed."); return 3;
    } else {
    	dataTuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Dump blockmap succeeded\n");
    return 0;
}
