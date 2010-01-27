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
  Tdeinit();
  
  exit(0);
}

void insertProbeIter(int  NUM_ENTRIES)
{
    //signal handling
    void (*prev_fn)(int);

    prev_fn = signal (SIGINT,terminate);
    //if (prev_fn==SIG_IGN)
    //signal (SIGTERM,SIG_IGN);

    
    sync();
    
    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    mscheduler = new merge_scheduler;    
    logtable ltable;

    

    int pcount = 40;
    ltable.set_fixed_page_count(pcount);

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    int lindex = mscheduler->addlogtable(&ltable);
    ltable.setMergeData(mscheduler->getMergeData(lindex));
    
    mscheduler->startlogtable(lindex);


    lserver = new logserver(10, 32432);

    lserver->startserver(&ltable);

    
//    Tdeinit();
    
    
}



/** @test
 */
int main()
{
    //insertProbeIter(25000);
    insertProbeIter(10000);
    /*
    insertProbeIter(5000);
    insertProbeIter(2500);
    insertProbeIter(1000);
    insertProbeIter(500);
    insertProbeIter(1000);
    insertProbeIter(100);
    insertProbeIter(10);
    */
    
    return 0;
}

