#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "logserver.h"
#include "datapage.h"
#include "merger.h"
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include <csignal>

#undef begin
#undef end

logserver *lserver=0;
merge_scheduler *mscheduler=0;

/*void ignore_pipe(int param)
{
  printf("Ignoring SIGPIPE\n");
  }*/
void terminate (int param)
{
	  printf ("Stopping server...\n");
	  lserver->stopserver();
	  delete lserver;

	  printf("Stopping merge threads...\n");
	  mscheduler->shutdown();
	  delete mscheduler;

	  printf("Deinitializing stasis...\n");
	  fflush(stdout);
	  logtable<datatuple>::deinit_stasis();

	  exit(0);
}

int main(int argc, char *argv[])
{
    //signal handling
    void (*prev_fn)(int);
    //    void (*prev_pipe)(int);

    prev_fn = signal (SIGINT,terminate);

    logtable<datatuple>::init_stasis();

    int xid = Tbegin();

    mscheduler = new merge_scheduler;

    logtable<datatuple> ltable;

    recordid table_root = ROOT_RECORD;
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

    int64_t c0_size = 1024 * 1024 * 512 * 1;

    if(argc == 2 && !strcmp(argv[1], "--test")) {

      c0_size = 1024 * 1024 * 10;
      printf("warning: running w/ tiny c0 for testing"); // XXX build a separate test server and deployment server?
    }

    if(argc == 2 && !strcmp(argv[1], "--benchmark")) {

      c0_size = 1024 * 1024 * 1024 * 1;
      printf("note: running w/ 2GB c0 for benchmarking"); // XXX build a separate test server and deployment server?
    }

    mscheduler->startlogtable(lindex, c0_size);

    lserver = new logserver(100, 32432);

    lserver->startserver(&ltable);

    abort(); // can't get here.
}
