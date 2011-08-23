/*
 * lsm_microbenchmarks.cpp
 *
 *  Created on: Aug 22, 2011
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/util/time.h>
#include <stdio.h>
#include <logserver.h>
#include <regionAllocator.h>

BEGIN_C_DECLS

int main(int argc, char * argv[]);

END_C_DECLS

enum run_type {
	ALL = 0
};

int main (int argc, char * argv[]) {
	uint64_t mb = 10000; // size of run, in bytes.
	enum run_type mode = ALL;

	const uint64_t num_pages = mb * ((1024 * 1024) / PAGE_SIZE);

	logtable::init_stasis();

	if(!mode) {
		int xid = Tbegin();
		printf("Starting first write of %lld mb\n", (long long)mb);
		struct timeval start, stop; double elapsed;
		gettimeofday(&start, 0);
		for(uint64_t i = 0; i < num_pages; i++) {
			Page * p = loadUninitializedPage(xid, i);
			stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
			releasePage(p);
		}
		gettimeofday(&stop, 0);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write (unforced) took %f seconds (%f mb/sec)\n", elapsed, ((double)mb)/elapsed);
		printf("Starting write of giant datapage\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages);
		DataPage * dp = new DataPage(xid, 10000, alloc);
		byte * key = (byte*)calloc(100, 1);
		byte * val = (byte*)calloc(900, 1);
		datatuple * tup = datatuple::create(key, 100, val, 900);
		free(key);
		free(val);
		while(1) {
			if(!dp->append(tup)) {
				break;
			}
		}
		alloc->force_regions(xid);
		Tcommit(xid);
		gettimeofday(&stop, 0);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write (forced) took %f seconds (%f mb/sec)\n", elapsed, ((double)mb)/elapsed);
	}

	logtable::deinit_stasis();
}
