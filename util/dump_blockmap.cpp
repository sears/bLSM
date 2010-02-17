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

int main(int argc, char * argv[]) {
	bool ok = true;
	int svrport = 32432;
	char * svrname = "localhost";
	if(argc == 3) {
		svrport = atoi(argv[2]);
	}
	if(argc == 2 || argc == 3) {
		svrname = argv[1];
	}
	if(!ok || argc > 3) {
		usage(argv); return 1;
	}

    logstore_handle_t * l = logstore_client_open(svrname, svrport, 100);

    if(l == NULL) { perror("Couldn't open connection"); return 2; }

    datatuple * t = datatuple::create("", 1);
    datatuple * ret = logstore_client_op(l, OP_DBG_BLOCKMAP, t);
    if(ret == NULL) {
    	perror("Blockmap request failed."); return 3;
    }
    logstore_client_close(l);
    printf("Success\n");
    return 0;
}
