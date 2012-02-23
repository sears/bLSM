/*
 * change_log_mode.cpp
 *
 * Copyright 2011-2012 Yahoo! Inc.
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
 *  Created on: Apr 20, 2011
 *      Author: sears
 */


#include "../tcpclient.h"
#include "../network.h"
#include "../datatuple.h"

void usage(char * argv[]) {
    fprintf(stderr, "usage %s mode [host [port]]\n", argv[0]);
}
#include "util_main.h"
int main(int argc, char * argv[]) {
	int mode = -1;
	char ** orig_argv = argv;
	if(argc > 1) {
		char * end;
		int n = strtol(argv[1], &end, 10);
		if(argv[1][0] && !*end) {
			mode = n;
			argc--;
			argv++;
		}
	}
	if(mode == -1) { usage(orig_argv); return 1; }

    logstore_handle_t * l = util_open_conn(argc, argv);

    dataTuple * tup = dataTuple::create(&mode, sizeof(mode));

    dataTuple * ret = logstore_client_op(l, OP_DBG_SET_LOG_MODE, tup);

    if(ret == NULL) {
      perror("Changing log mode failed.."); return 3;
    } else {
      dataTuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Log mode changed.\n");
    return 0;
}
