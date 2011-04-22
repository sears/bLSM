/*
 * change_log_mode.cpp
 *
 *  Created on: Apr 20, 2011
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

    datatuple * tup = datatuple::create(&mode, sizeof(mode));

    datatuple * ret = logstore_client_op(l, OP_DBG_SET_LOG_MODE, tup);

    if(ret == NULL) {
      perror("Changing log mode failed.."); return 3;
    } else {
      datatuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Log mode changed.\n");
    return 0;
}
