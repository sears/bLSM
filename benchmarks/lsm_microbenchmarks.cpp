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
	double MB = 1024 * 1024;
	uint64_t mb = 20000; // size of run, in megabytes.

	enum run_type mode = ALL;

	const uint64_t num_pages = mb * (MB / PAGE_SIZE);

	stasis_buffer_manager_size = (512 * MB) / PAGE_SIZE;

//	stasis_buffer_manager_hint_writes_are_sequential = 1;
//	stasis_dirty_page_table_flush_quantum = (8 * MB) / PAGE_SIZE; // XXX if set to high-> segfault
//	stasis_dirty_page_count_hard_limit = (16 * MB) / PAGE_SIZE;
//	stasis_dirty_page_count_soft_limit = (10 * MB) / PAGE_SIZE;
//	stasis_dirty_page_low_water_mark = (8 * MB) / PAGE_SIZE;

	// Hard disk preferred.
	/*	stasis_dirty_page_table_flush_quantum = (4 * MB) / PAGE_SIZE; // XXX if set to high-> segfault
	stasis_dirty_page_count_hard_limit = (12 * MB) / PAGE_SIZE;
	stasis_dirty_page_count_soft_limit = (8 * MB) / PAGE_SIZE;
	stasis_dirty_page_low_water_mark = (4 * MB) / PAGE_SIZE;*/

	// SSD preferred.
	stasis_dirty_page_table_flush_quantum = (4 * MB) / PAGE_SIZE; // XXX if set to high-> segfault
	stasis_dirty_page_count_hard_limit = (40 * MB) / PAGE_SIZE;
	stasis_dirty_page_count_soft_limit = (32 * MB) / PAGE_SIZE;
	stasis_dirty_page_low_water_mark   = (16 * MB) / PAGE_SIZE;

	stasis_dirty_page_table_flush_quantum = (4 * MB) / PAGE_SIZE; // XXX if set to high-> segfault
	stasis_dirty_page_count_hard_limit = (48 * MB) / PAGE_SIZE;
	stasis_dirty_page_count_soft_limit = (40 * MB) / PAGE_SIZE;
	stasis_dirty_page_low_water_mark   = (32 * MB) / PAGE_SIZE;

	printf("stasis_buffer_manager_size=%lld\n", (long long)stasis_buffer_manager_size * PAGE_SIZE);
	printf("Hard limit=%lld\n", (long long)((stasis_dirty_page_count_hard_limit*PAGE_SIZE)/MB));
	printf("Hard limit is %f pct.\n", 100.0 * ((double)stasis_dirty_page_count_hard_limit)/((double)stasis_buffer_manager_size));

	logtable::init_stasis();

	RegionAllocator * readableAlloc = NULL;
	if(!mode) {
		int xid = Tbegin();
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages);
		printf("Starting first write of %lld mb\n", (long long)mb);
		struct timeval start, start_sync, stop; double elapsed;
		gettimeofday(&start, 0);
		pageid_t extent = alloc->alloc_extent(xid, num_pages);
		for(uint64_t i = 0; i < num_pages; i++) {
			Page * p = loadUninitializedPage(xid, i+extent);
			stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
			releasePage(p);
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		readableAlloc = alloc;
		Tcommit(xid);
//		alloc = new RegionAllocator(xid, num_pages);
		gettimeofday(&stop, 0);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)mb)/elapsed);
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));

	}

	if(!mode) {
		int xid = Tbegin();
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages);
		printf("Starting write with parallel read of %lld mb\n", (long long)mb);
		struct timeval start, start_sync, stop; double elapsed;
		gettimeofday(&start, 0);

		pageid_t region_length;
		pageid_t region_count;
		pageid_t * old_extents = readableAlloc->list_regions(xid, &region_length, &region_count);
		pageid_t extent = alloc->alloc_extent(xid, num_pages);
		assert(region_count == 1);
		for(uint64_t i = 0; i < num_pages/2; i++) {
			Page * p = loadUninitializedPage(xid, i+extent);
			stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
			releasePage(p);
			p = loadPage(xid, i+old_extents[0]);
			releasePage(p);
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		delete alloc;
		Tcommit(xid);
//		alloc = new RegionAllocator(xid, num_pages);
		gettimeofday(&stop, 0);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)mb)/elapsed);
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));

	}

	if(!mode) {
		int xid = Tbegin();
		struct timeval start, start_sync, stop; double elapsed;
		printf("Starting write of giant datapage\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages);
		DataPage * dp = new DataPage(xid, num_pages-1, alloc);
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
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);

		gettimeofday(&stop, 0);
		Tcommit(xid);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)mb)/elapsed);
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));
	}
	if(!mode) {
		int xid = Tbegin();
		struct timeval start, start_sync, stop; double elapsed;
		printf("Starting write of many small datapages\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages);
		byte * key = (byte*)calloc(100, 1);
		byte * val = (byte*)calloc(900, 1);
		datatuple * tup = datatuple::create(key, 100, val, 900);
		free(key);
		free(val);
		DataPage * dp = 0;
		uint64_t this_count = 0;
		uint64_t count  = 0;
		uint64_t dp_count = 0;
		while((count * 1000) < (mb * 1024*1024)) {
			if((!dp) || !dp->append(tup)) {
				dp = new DataPage(xid, 2, alloc);
				dp_count++;
			}
			count++;
			this_count++;
//			if(((this_count * 1000) > (1024 * 1024 * 16))) {
//				alloc->force_regions(xid);
//				this_count = 0;
//				gettimeofday(&stop, 0);
//				elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
//				printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(1024*1024*elapsed));
//			}
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		gettimeofday(&stop, 0);
		Tcommit(xid);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(elapsed*1024*1024));
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));
	}

	if(!mode) {
		int xid = Tbegin();
		struct timeval start, start_sync, stop; double elapsed;
		printf("Starting two parallel writes of many small datapages\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages/2);
		RegionAllocator * alloc2 = new RegionAllocator(xid, num_pages/2);
		byte * key = (byte*)calloc(100, 1);
		byte * val = (byte*)calloc(900, 1);
		datatuple * tup = datatuple::create(key, 100, val, 900);
		free(key);
		free(val);
		DataPage * dp = 0;
		DataPage * dp2 = 0;
		uint64_t this_count = 0;
		uint64_t count  = 0;
		uint64_t dp_count = 0;
		while((count * 1000) < (mb * 1024*1024)) {
			if((!dp) || !dp->append(tup)) {
				dp = new DataPage(xid, 2, alloc);
				dp_count++;
			}
			if((!dp2) || !dp2->append(tup)) {
				dp2 = new DataPage(xid, 2, alloc2);
				//dp_count++;
			}
			count += 2;
			this_count++;
//			if(((this_count * 1000) > (1024 * 1024 * 16))) {
//				alloc->force_regions(xid);
//				this_count = 0;
//				gettimeofday(&stop, 0);
//				elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
//				printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(1024*1024*elapsed));
//			}
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		alloc2->force_regions(xid);
		gettimeofday(&stop, 0);
		Tcommit(xid);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(elapsed*1024*1024));
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));

	}

	RegionAllocator * read_alloc = NULL;
	RegionAllocator * read_alloc2 = NULL;
	RegionAllocator * read_alloc3 = NULL;
	RegionAllocator * read_alloc4 = NULL;

	if(!mode) {
		int xid = Tbegin();
		struct timeval start, start_sync, stop; double elapsed;
		printf("Starting four parallel writes of many small datapages\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc2 = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc3 = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc4 = new RegionAllocator(xid, num_pages/4);
		byte * key = (byte*)calloc(100, 1);
		byte * val = (byte*)calloc(900, 1);
		datatuple * tup = datatuple::create(key, 100, val, 900);
		free(key);
		free(val);
		DataPage * dp = 0;
		DataPage * dp2 = 0;
		DataPage * dp3 = 0;
		DataPage * dp4 = 0;
		uint64_t this_count = 0;
		uint64_t count  = 0;
		uint64_t dp_count = 0;

		while((count * 1000) < (mb * 1024*1024)) {
			if((!dp) || !dp->append(tup)) {
				dp = new DataPage(xid, 2, alloc);
				dp_count++;
			}
			if((!dp2) || !dp2->append(tup)) {
				dp2 = new DataPage(xid, 2, alloc2);
				//dp_count++;
			}
			if((!dp3) || !dp3->append(tup)) {
				dp3 = new DataPage(xid, 2, alloc3);
				//dp_count++;
			}
			if((!dp4) || !dp4->append(tup)) {
				dp4 = new DataPage(xid, 2, alloc4);
				//dp_count++;
			}
			count += 4;
			this_count++;
//			if(((this_count * 1000) > (1024 * 1024 * 16))) {
//				alloc->force_regions(xid);
//				this_count = 0;
//				gettimeofday(&stop, 0);
//				elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
//				printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(1024*1024*elapsed));
//			}
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		alloc2->force_regions(xid);
		alloc3->force_regions(xid);
		alloc4->force_regions(xid);
		gettimeofday(&stop, 0);
		Tcommit(xid);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(elapsed*1024*1024));
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));
		read_alloc = alloc;
		read_alloc2 = alloc2;
		read_alloc3 = alloc3;
		read_alloc4 = alloc4;

	}

	if(!mode) {
		int xid = Tbegin();
		struct timeval start, start_sync, stop; double elapsed;
		printf("Starting four parallel writes of many small datapages\n");
		gettimeofday(&start, 0);
		RegionAllocator * alloc = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc2 = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc3 = new RegionAllocator(xid, num_pages/4);
		RegionAllocator * alloc4 = new RegionAllocator(xid, num_pages/4);
		byte * key = (byte*)calloc(100, 1);
		byte * val = (byte*)calloc(900, 1);
		datatuple * tup = datatuple::create(key, 100, val, 900);
		free(key);
		free(val);
		DataPage * dp = 0;
		DataPage * dp2 = 0;
		DataPage * dp3 = 0;
		DataPage * dp4 = 0;
		uint64_t this_count = 0;
		uint64_t count  = 0;
		uint64_t dp_count = 0;

		pageid_t n1, n2, n3, n4;
		pageid_t l1, l2, l3, l4;
		pageid_t * regions1, * regions2, * regions3, * regions4;
 
		regions1 = read_alloc->list_regions(xid, &l1, &n1);
		regions2 = read_alloc2->list_regions(xid, &l2, &n2);
		regions3 = read_alloc3->list_regions(xid, &l3, &n3);
		regions4 = read_alloc4->list_regions(xid, &l4, &n4);

		pageid_t i1 = regions1[0];
		pageid_t i2 = regions2[0];
		pageid_t i3 = regions3[0];
		pageid_t i4 = regions4[0];

		DataPage * rdp  = new DataPage(xid, 0, i1);
		DataPage * rdp2 = new DataPage(xid, 0, i2);
		DataPage * rdp3 = new DataPage(xid, 0, i3);
		DataPage * rdp4 = new DataPage(xid, 0, i4);

		DataPage::iterator it1 = rdp->begin();
		DataPage::iterator it2 = rdp2->begin();
		DataPage::iterator it3 = rdp3->begin();
		DataPage::iterator it4 = rdp4->begin();

		while((count * 1000) < (mb * 1024*1024)) {
			if((!dp) || !dp->append(tup)) {
				dp = new DataPage(xid, 2, alloc);
				dp_count++;
			}
			if((!dp2) || !dp2->append(tup)) {
				dp2 = new DataPage(xid, 2, alloc2);
				//dp_count++;
			}
			if((!dp3) || !dp3->append(tup)) {
				dp3 = new DataPage(xid, 2, alloc3);
				//dp_count++;
			}
			if((!dp4) || !dp4->append(tup)) {
				dp4 = new DataPage(xid, 2, alloc4);
				//dp_count++;
			}
			datatuple * t;
			if((!rdp) || !(t = it1.getnext())) {
			  i1+= rdp->get_page_count();
			  if(rdp) delete rdp;
			  rdp = new DataPage(xid, 0, i1);
			  //			  i1++;
			  it1 = rdp->begin();
			  t = it1.getnext();
			}
			if(t) datatuple::freetuple(t);
			if((!rdp2) || !(t = it2.getnext())) {
			  i2+= rdp2->get_page_count();
			  if(rdp2) delete rdp2;
			  rdp2 = new DataPage(xid, 0, i2);
			  //			  i2++;
			  it2 = rdp2->begin();
			  t = it2.getnext();
			}
			if(t) datatuple::freetuple(t);
			if((!rdp3) || !(t = it3.getnext())) {
			  i3+= rdp3->get_page_count();
			  if(rdp3) delete rdp3;
			  rdp3 = new DataPage(xid, 0, i3);
			  //			  i3++;
			  it3 = rdp3->begin();
			  t = it3.getnext();
			}
			if(t) datatuple::freetuple(t);
			if((!rdp4) || !(t = it4.getnext())) {
			  i4+= rdp4->get_page_count();
			  if(rdp4) delete rdp4;
			  rdp4 = new DataPage(xid, 0, i4);
			  //			  i4++;
			  it4 = rdp4->begin();
			  t = it4.getnext();
			}
			if(t) datatuple::freetuple(t);

			count += 8;
			this_count++;
//			if(((this_count * 1000) > (1024 * 1024 * 16))) {
//				alloc->force_regions(xid);
//				this_count = 0;
//				gettimeofday(&stop, 0);
//				elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
//				printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(1024*1024*elapsed));
//			}
		}
		gettimeofday(&start_sync,0);
		alloc->force_regions(xid);
		alloc2->force_regions(xid);
		alloc3->force_regions(xid);
		alloc4->force_regions(xid);
		gettimeofday(&stop, 0);
		Tcommit(xid);
		elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
		printf("Write took %f seconds (%f mb/sec)\n", elapsed, ((double)(count*1000))/(elapsed*1024*1024));
		printf("Sync took %f seconds.\n", stasis_timeval_to_double(stasis_subtract_timeval(stop, start_sync)));
		read_alloc = alloc;
		read_alloc2 = alloc2;
		read_alloc3 = alloc3;
		read_alloc4 = alloc4;

	}


	logtable::deinit_stasis();
}
