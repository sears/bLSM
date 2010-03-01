/*
 * dump_blockmap.cc
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

    datatuple * ret = logstore_client_op(l, op);
    if(ret == NULL) {
    	perror("Dump blockmap failed."); return 3;
    } else {
    	datatuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Dump blockmap succeeded\n");
    return 0;
}
