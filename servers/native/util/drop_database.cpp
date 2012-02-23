/*
 * drop_database.cpp
 *
 *  Created on: Feb 23, 2010
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

    datatuple * ret = logstore_client_op(l, OP_DBG_DROP_DATABASE);
    if(ret == NULL) {
    	perror("Drop database failed"); return 3;
    } else {
    	datatuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Drop database succeeded\n");
    return 0;

}

