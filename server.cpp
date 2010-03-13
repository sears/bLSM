#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include "logstore.h"
#include "datapage.h"
#include "logiterators.h"
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
	  diskTreeComponent::internalNodes::deinit_stasis();

	  exit(0);
}

void initialize_server()
{
    //signal handling
    void (*prev_fn)(int);
    //    void (*prev_pipe)(int);

    prev_fn = signal (SIGINT,terminate);

    diskTreeComponent::internalNodes::init_stasis();

    int xid = Tbegin();

    mscheduler = new merge_scheduler;

    logtable ltable;

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

    writelock(ltable.header_lock,0);

    int lindex = mscheduler->addlogtable(&ltable);
    ltable.setMergeData(mscheduler->getMergeData(lindex));

    int64_t c0_size = 1024 * 1024 * 10;
    printf("warning: running w/ tiny c0 for testing"); // XXX build a separate test server and deployment server?
    mscheduler->startlogtable(lindex, c0_size);

    unlock(ltable.header_lock);

    lserver = new logserver(10, 32432);

    lserver->startserver(&ltable);
}



/** @test
 */
int main()
{
	initialize_server();
	abort();  // can't get here.
}

