/*
 * newserver.cpp
 *
 * Copyright 2010-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Created on: Aug 11, 2010
 *      Author: sears
 */
#include <stasis/transactional.h>
#include <stasis/logger/safeWrites.h>

#include <signal.h>


#include "merger.h"
#include "logstore.h"
#include "simpleServer.h"

int main(int argc, char *argv[])
{
    signal(SIGPIPE, SIG_IGN);
    int64_t c0_size = 1024 * 1024 * 512 * 1;
    int log_mode = 0; // do not log by default.
    int64_t expiry_delta = 0;  // do not gc by default
    int port = simpleServer::DEFAULT_PORT;
    stasis_buffer_manager_size = 1 * 1024 * 1024 * 1024 / PAGE_SIZE;  // 1.5GB total

    for(int i = 1; i < argc; i++) {
    	if(!strcmp(argv[i], "--test")) {
    		stasis_buffer_manager_size = 3 * 1024 * 1024 * 128 / PAGE_SIZE;  // 228MB total
    		c0_size = 1024 * 1024 * 100;
    		printf("warning: running w/ tiny c0 for testing\n"); // XXX build a separate test server and deployment server?
    	} else if(!strcmp(argv[i], "--benchmark")) {
    	      stasis_buffer_manager_size = (1024LL * 1024LL * 1024LL * 2LL) / PAGE_SIZE;  // 4GB total
    	      c0_size =                     1024LL * 1024LL * 1024LL * 2LL;
    	      printf("note: running w/ 2GB c0 for benchmarking\n"); // XXX build a separate test server and deployment server?
    	} else if(!strcmp(argv[i], "--log-mode")) {
    		i++;
    		log_mode = atoi(argv[i]);
        } else if(!strcmp(argv[i], "--expiry-delta")) {
            i++;
            expiry_delta = atoi(argv[i]);
        } else if(!strcmp(argv[i], "--port")) {
            i++;
            port = atoi(argv[i]);
    	} else {
    		fprintf(stderr, "Usage: %s [--test|--benchmark] [--log-mode <int>] [--expiry-delta <int>] [--port <int>]", argv[0]);
    		abort();
    	}
    }

    logtable::init_stasis();

      int xid = Tbegin();


      recordid table_root = ROOT_RECORD;
    {
		logtable ltable(log_mode, c0_size);
		ltable.expiry = expiry_delta;

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
		merge_scheduler * mscheduler = new merge_scheduler(&ltable);
		mscheduler->start();
		ltable.replayLog();

		simpleServer *lserver = new simpleServer(&ltable, simpleServer::DEFAULT_THREADS, port);

		lserver->acceptLoop();

		printf ("Stopping server...\n");
		delete lserver;

		printf("Stopping merge threads...\n");
		mscheduler->shutdown();
		delete mscheduler;

		printf("Deinitializing stasis...\n");
		fflush(stdout);
    }
    logtable::deinit_stasis();

    printf("Shutdown complete\n");
}
