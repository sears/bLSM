#include <stasis/transactional.h>
#undef end
#undef try
#undef begin

#include "merger.h"
#include "logstore.h"
#include "simpleServer.h"

/*
 * newserver.cpp
 *
 *  Created on: Aug 11, 2010
 *      Author: sears
 */

int main(int argc, char *argv[])
{

  logtable<datatuple>::init_stasis();

    int xid = Tbegin();

    merge_scheduler * mscheduler = new merge_scheduler;

    logtable<datatuple> ltable;

    recordid table_root = ROOT_RECORD;

    int64_t c0_size = 1024 * 1024 * 512 * 1;

    if(argc == 2 && !strcmp(argv[1], "--test")) {
      c0_size = 1024 * 1024 * 100;
      printf("warning: running w/ tiny c0 for testing\n"); // XXX build a separate test server and deployment server?
    }

    if(argc == 2 && !strcmp(argv[1], "--benchmark")) {
      c0_size = 1024 * 1024 * 768 * 1;
      printf("note: running w/ 2GB c0 for benchmarking\n"); // XXX build a separate test server and deployment server?
    }

    if(TrecordType(xid, ROOT_RECORD) == INVALID_SLOT) {
        printf("Creating empty logstore\n");
        table_root = ltable.allocTable(xid);
        assert(table_root.page == ROOT_RECORD.page &&
               table_root.slot == ROOT_RECORD.slot);
    } else {
        printf("Opened existing logstore\n");
        table_root.size = TrecordSize(xid, ROOT_RECORD);
        ltable.openTable(xid, table_root);
    }

    Tcommit(xid);

    int lindex = mscheduler->addlogtable(&ltable);
    ltable.setMergeData(mscheduler->getMergeData(lindex));

    mscheduler->startlogtable(lindex, c0_size);

    simpleServer *lserver = new simpleServer(&ltable);

    lserver->acceptLoop();

    printf ("Stopping server...\n");
    delete lserver;

    printf("Stopping merge threads...\n");
    mscheduler->shutdown();
    delete mscheduler;

    printf("Deinitializing stasis...\n");
    fflush(stdout);
    logtable<datatuple>::deinit_stasis();

    printf("Shutdown complete\n");
}
