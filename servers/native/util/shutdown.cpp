/*
 * shtudown.cpp
 *
 *  Created on: Aug 16, 2010
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

    dataTuple * ret = logstore_client_op(l, OP_SHUTDOWN);

    if(ret == NULL) {
      perror("Shutdown failed."); return 3;
    } else {
      dataTuple::freetuple(ret);
    }
    logstore_client_close(l);
    printf("Shutdown in progress\n");
    return 0;
}
