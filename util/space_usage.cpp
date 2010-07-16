/*
 * space_usage.cpp
 *
 *  Created on: Mar 1, 2010
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
	logstore_handle_t * l = util_open_conn(argc, argv);

    datatuple * ret = logstore_client_op(l, OP_STAT_SPACE_USAGE);

	if(ret == NULL) {
    	perror("Space usage failed."); return 3;
    }

    logstore_client_close(l);
    assert(ret->keylen() == sizeof(uint64_t));
    assert(ret->datalen() == sizeof(uint64_t));
    printf("Tree is %llu MB Store file is %llu MB\n", (unsigned long long)(*(uint64_t*)ret->key()) / (1024*1024), (unsigned long long)(*(uint64_t*)ret->data()) / (1024*1024));
	datatuple::freetuple(ret);
    		;
    return 0;
}
