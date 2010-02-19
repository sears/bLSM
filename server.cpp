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
  diskTreeComponent::deinit_stasis();
  
  exit(0);
}

void initialize_server()
{
    //signal handling
    void (*prev_fn)(int);

    diskTreeComponent::init_stasis();

    prev_fn = signal (SIGINT,terminate);
    
    int xid = Tbegin();

    mscheduler = new merge_scheduler;    
    logtable ltable;

    int pcount = 40;
    ltable.set_fixed_page_count(pcount);

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    int lindex = mscheduler->addlogtable(&ltable);
    ltable.setMergeData(mscheduler->getMergeData(lindex));
    
    int64_t c0_size = 1024 * 1024 * 10;
    printf("warning: running w/ tiny c0 for testing"); // XXX
    mscheduler->startlogtable(lindex, c0_size);

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

