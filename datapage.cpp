
#include "logstore.h"
#include "datapage.h"

template <class TUPLE>
const int32_t DataPage<TUPLE>::HEADER_SIZE = sizeof(int32_t);

template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, pageid_t pid):
    alloc_region(0),
    alloc_state(0),
    fix_pcount(-1)
{
    assert(pid!=0);
    
    pcount = readPageCount(xid, pid);
    
    pidarr = (pageid_t *) malloc(sizeof(pageid_t) * pcount);

    for(int i=0; i<pcount; i++)
        pidarr[i] = i + pid;

    byte_offset = HEADER_SIZE; //step over the header info
    
}

template <class TUPLE>
DataPage<TUPLE>::DataPage(int xid, int fix_pcount, pageid_t (*alloc_region)(int, void*), void * alloc_state)
{
    assert(fix_pcount >= 1);
    byte_offset = -1;

    this->fix_pcount = fix_pcount;

    if(alloc_region != 0)    
        this->alloc_region = alloc_region;
    if(alloc_state != 0)
        this->alloc_state = alloc_state;
    
    initialize(xid);
}

template<class TUPLE>
DataPage<TUPLE>::~DataPage()
{
    if(pidarr)
        free(pidarr);
}


template<class TUPLE>
void DataPage<TUPLE>::initialize(int xid)
{
    //initializes to an empty datapage
    //alloc a new page
    pageid_t pid = alloc_region(xid, alloc_state);

    //load the first page
    //Page *p = loadPage(xid, pid);
    Page *p = loadPageOfType(xid, pid, SEGMENT_PAGE);
    writelock(p->rwlatch,0);
    
    //initialize header
    
    //set number of pages to 1
    int32_t * numpages_ptr = (int32_t*)stasis_page_byte_ptr_from_start(p, 0);
    *numpages_ptr = 1;
    
    //write 0 to first data size    
    int32_t * size_ptr = (int32_t*)stasis_page_byte_ptr_from_start(p, HEADER_SIZE);
    *size_ptr = 0;

    //set the page dirty
    stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
    
    //release the page
    unlock(p->rwlatch);
    releasePage(p);
    
    //set the class variables
    byte_offset = HEADER_SIZE;
    pcount = 1;
    pidarr = (pageid_t *) malloc(fix_pcount * sizeof(pageid_t));
    pidarr[0] = pid;
    
}

template <class TUPLE>
inline bool DataPage<TUPLE>::append(int xid, TUPLE const & dat)
{
    assert(byte_offset >= HEADER_SIZE);
    assert(fix_pcount >= 1);
    
    //check if there is enough space (for the data length + data)
    int32_t blen = dat.byte_length() + sizeof(int32_t);    
    if(PAGE_SIZE * fix_pcount - byte_offset < blen)
    {
        //check if the record is too large
        // and if so do we wanna accomodate here by going over the fix_pcount
        if(PAGE_SIZE * fix_pcount - HEADER_SIZE < blen && //record is larger than datapage           
           PAGE_SIZE * fix_pcount - HEADER_SIZE > 2 * byte_offset)//accept if i am less than half full
        {
            //nothing
        }
        else
        {
            //printf("page has %d bytes left, we needed %d. (byte_offset %d)\n",
            //PAGE_SIZE * fix_pcount - byte_offset, blen, byte_offset);
            return false;   //not enough mana, return
        }
    }

    //write the length of the data
    int32_t dsize = blen - sizeof(int32_t);
    
    if(!writebytes(xid, sizeof(int32_t), (byte*)(&dsize)))    
        return false;    
    byte_offset += sizeof(int32_t);

    //write the data
    byte * barr = dat.to_bytes();
    if(!writebytes(xid, dsize, barr)) //if write fails, undo the previous write
    {        
        byte_offset -= sizeof(int32_t);        
        free(barr);
        //write 0 for the next tuple size, if there is enough space in this page
        if(PAGE_SIZE - (byte_offset % PAGE_SIZE) >= sizeof(int32_t))
        {
            dsize = 0;
            writebytes(xid, sizeof(int32_t), (byte*)(&dsize));//this will succeed, since there is enough space on the page
        }        
        return false;
    }
    free(barr);
    byte_offset += dsize;

    //write 0 for the next tuple size, if there is enough space in this page
    if(PAGE_SIZE - (byte_offset % PAGE_SIZE) >= sizeof(int32_t))
    {
        dsize = 0;
        writebytes(xid, sizeof(int32_t), (byte*)(&dsize));//this will succeed, since there is enough space on the page
    }
    
    return true;
}

template <class TUPLE>
bool DataPage<TUPLE>::writebytes(int xid, int count, byte *data)
{

    int32_t bytes_copied = 0;
    while(bytes_copied < count)
    {
        //load the page to copy into
        int pindex = (byte_offset + bytes_copied) / PAGE_SIZE;
        if(pindex == pcount) //then this page must be allocated
        {
            pageid_t newid = alloc_region(xid, alloc_state);
            //check continuity
            if(pidarr[pindex-1] != newid - 1)//so we started a new region and that is not right after the prev region in the file
            {                
                return false;//we cant store this
            }

            //check whether we need to extend the pidarr, add fix_pcount many pageid_t slots
            if(pindex >= fix_pcount && (pindex % fix_pcount==0))
            {
                pidarr = (pageid_t*)realloc(pidarr, (pindex + fix_pcount)*sizeof(pageid_t));
            }
            pidarr[pindex] = newid;
            pcount++;
            incrementPageCount(xid, pidarr[0]);
        }
        //Page *p = loadPage(xid, pidarr[pindex]);
        Page *p = loadPageOfType(xid, pidarr[pindex], SEGMENT_PAGE);
        writelock(p->rwlatch,0);
        
        //copy the portion of bytes we can copy in this page
        int32_t page_offset = (byte_offset+bytes_copied) % PAGE_SIZE;
        int32_t copy_len = ( (PAGE_SIZE - page_offset < count - bytes_copied ) ? PAGE_SIZE - page_offset: count - bytes_copied);

        byte * pb_ptr = stasis_page_byte_ptr_from_start(p, page_offset);
        memcpy(pb_ptr, data+bytes_copied  ,copy_len);
    
        //release the page
        stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
        unlock(p->rwlatch);
        releasePage(p);        
        
        //update the copied bytes_count
        bytes_copied += copy_len;


    }

    assert(bytes_copied == count);
    return true;
}

template <class TUPLE>
bool DataPage<TUPLE>::recordRead(int xid, typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf)
{
    RecordIterator itr(this);

    int match = -1;
    while((*buf=itr.getnext(xid)) != 0)
        {
            match = TUPLE::compare((*buf)->get_key(), key);
            
            if(match<0) //keep searching
            {
                free((*buf)->keylen);
                free(*buf);                
                *buf=0;
            }
            else if(match==0) //found
            {
                return true;
            }
            else // match > 0, then does not exist
            {
                free((*buf)->keylen);
                free(*buf);
                *buf = 0;
                break;
            }
        }
    
    return false;
}

template <class TUPLE>
void DataPage<TUPLE>::readbytes(int xid, int32_t offset, int count, byte **data)
{

    if(*data==NULL)
        *data = (byte*)malloc(count);
    
    int32_t bytes_copied = 0;
    while(bytes_copied < count)
    {
        //load the page to copy from
        int pindex = (offset + bytes_copied) / PAGE_SIZE;
        
        //Page *p = loadPage(xid, pidarr[pindex]);
        Page *p = loadPageOfType(xid, pidarr[pindex], SEGMENT_PAGE);
        readlock(p->rwlatch,0);
        
        //copy the portion of bytes we can copy from this page
        int32_t page_offset = (offset+bytes_copied) % PAGE_SIZE;
        int32_t copy_len = ( (PAGE_SIZE - page_offset < count - bytes_copied ) ? PAGE_SIZE - page_offset : count - bytes_copied);

        byte * pb_ptr = stasis_page_byte_ptr_from_start(p, page_offset);
        memcpy((*data)+bytes_copied, pb_ptr, copy_len);
    
        //release the page
        unlock(p->rwlatch);
        releasePage(p);        
        
        //update the copied bytes_count
        bytes_copied += copy_len;
    }

    assert(bytes_copied == count);
}


template <class TUPLE>
inline int DataPage<TUPLE>::readPageCount(int xid, pageid_t pid)
{

    //Page *p = loadPage(xid, pid);
    Page *p = loadPageOfType(xid, pid, SEGMENT_PAGE);
    readlock(p->rwlatch,0);

    int32_t numpages = *((int32_t*)stasis_page_byte_ptr_from_start(p, 0));
    
    unlock(p->rwlatch);
    releasePage(p);

    return numpages;
}

template <class TUPLE>
inline void DataPage<TUPLE>::incrementPageCount(int xid, pageid_t pid, int add)
{
    //Page *p = loadPage(xid, pid);
    Page *p = loadPageOfType(xid, pid, SEGMENT_PAGE);
    writelock(p->rwlatch,0);

    int32_t *numpages_ptr = (int32_t*)stasis_page_byte_ptr_from_start(p, 0);

    *numpages_ptr = *numpages_ptr + add;

    stasis_dirty_page_table_set_dirty((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), p);
    
    unlock(p->rwlatch);
    releasePage(p);


    
}


template <class TUPLE>
inline uint16_t DataPage<TUPLE>::recordCount(int xid)
{

    return 0;
}

template <class TUPLE>
pageid_t DataPage<TUPLE>::dp_alloc_region(int xid, void *conf)
{
    RegionAllocConf_t* a = (RegionAllocConf_t*)conf;

    
    if(a->nextPage == a->endOfRegion) {
        if(a->regionList.size == -1) {
            //DEBUG("nextPage: %lld\n", a->nextPage);
            a->regionList = TarrayListAlloc(xid, 1, 4, sizeof(pageid_t));
        DEBUG("regionList.page: %lld\n", a->regionList.page);
        DEBUG("regionList.slot: %d\n", a->regionList.slot);
        DEBUG("regionList.size: %lld\n", a->regionList.size);
        
        a->regionCount = 0;
    }
        DEBUG("{%lld <- alloc region arraylist}\n", a->regionList.page);
    TarrayListExtend(xid,a->regionList,1);
    a->regionList.slot = a->regionCount;
    DEBUG("region lst slot %d\n",a->regionList.slot);
    a->regionCount++;
    DEBUG("region count %lld\n",a->regionCount);
    a->nextPage = TregionAlloc(xid, a->regionSize,12);
    DEBUG("next page %lld\n",a->nextPage);
    a->endOfRegion = a->nextPage + a->regionSize;
    Tset(xid,a->regionList,&a->nextPage);
    DEBUG("next page %lld\n",a->nextPage);
  }
    
  DEBUG("%lld ?= %lld\n", a->nextPage,a->endOfRegion);
  pageid_t ret = a->nextPage;
  // Ensure the page is in buffer cache without accessing disk (this
  // sets it to clean and all zeros if the page is not in cache).
  // Hopefully, future reads will get a cache hit, and avoid going to
  // disk.

  Page * p = loadUninitializedPage(xid, ret);
  //writelock(p->rwlatch,0);
  p->pageType = SEGMENT_PAGE;
  //unlock(p->rwlatch);  
  releasePage(p);  
  DEBUG("ret %lld\n",ret);
  (a->nextPage)++;
  return ret;

}

template <class TUPLE>
pageid_t DataPage<TUPLE>::dp_alloc_region_rid(int xid, void * ridp) {
  recordid rid = *(recordid*)ridp;
  RegionAllocConf_t conf;
  Tread(xid,rid,&conf);
  pageid_t ret = dp_alloc_region(xid,&conf);
  //DEBUG("{%lld <- alloc region extend}\n", conf.regionList.page);
  // XXX get rid of Tset by storing next page in memory, and losing it
  //     on crash.
  Tset(xid,rid,&conf);
  return ret;
}

template <class TUPLE>
void DataPage<TUPLE>::dealloc_region_rid(int xid, void *conf)
{
    RegionAllocConf_t a = *((RegionAllocConf_t*)conf);
    DEBUG("{%lld <- dealloc region arraylist}\n", a.regionList.page);

    for(int i = 0; i < a.regionCount; i++) {
     a.regionList.slot = i;
     pageid_t pid;
     Tread(xid,a.regionList,&pid);
     TregionDealloc(xid,pid);
    }
}

template <class TUPLE>
void DataPage<TUPLE>::force_region_rid(int xid, void *conf)
{
    recordid rid = *(recordid*)conf;
    RegionAllocConf_t a;
    Tread(xid,rid,&a);
    
    for(int i = 0; i < a.regionCount; i++)
    {
        a.regionList.slot = i;
        pageid_t pid;
        Tread(xid,a.regionList,&pid);
        stasis_dirty_page_table_flush_range((stasis_dirty_page_table_t*)stasis_runtime_dirty_page_table(), pid, pid+a.regionSize);
        stasis_buffer_manager_t *bm = 
	   (stasis_buffer_manager_t*)stasis_runtime_buffer_manager();
        bm->forcePageRange(bm, pid, pid+a.regionSize);
    }
}


///////////////////////////////////////////////////////////////
//RECORD ITERATOR
///////////////////////////////////////////////////////////////


template <class TUPLE>
TUPLE* DataPage<TUPLE>::RecordIterator::getnext(int xid)
{

    
    int pindex = offset / PAGE_SIZE;

    if(pindex == dp->pcount)//past end
        return 0;
    if(pindex == dp->pcount - 1 && (PAGE_SIZE - (offset % PAGE_SIZE) < sizeof(int32_t)))
        return 0;
    
    //Page *p = loadPage(xid, dp->pidarr[pindex]);
    Page *p = loadPageOfType(xid, dp->pidarr[pindex], SEGMENT_PAGE);
    readlock(p->rwlatch,0);    

    int32_t *dsize_ptr;
    if(PAGE_SIZE - (offset % PAGE_SIZE) < sizeof(int32_t)) //int spread in two pages
    {
        dsize_ptr = 0;
        dp->readbytes(xid, offset, sizeof(int32_t), (byte**)(&dsize_ptr));
    }
    else //int in a single page
        dsize_ptr = (int32_t*)stasis_page_byte_ptr_from_start(p, offset % PAGE_SIZE);
    
    offset += sizeof(int32_t);
                
    if(*dsize_ptr == 0) //no more keys
    {            
        unlock(p->rwlatch);
        releasePage(p);
        return 0;
    }
    
    byte* tb=0;
    dp->readbytes(xid, offset, *dsize_ptr, &tb);

    TUPLE *tup = TUPLE::from_bytes(tb);

    offset += *dsize_ptr;

    unlock(p->rwlatch);
    releasePage(p);

    return tup;
}



template <class TUPLE>
void DataPage<TUPLE>::RecordIterator::advance(int xid, int count)
{

    int pindex = -1;
    Page *p = 0;
    
    for(int i=0; i<count; i++)
    {
        if(pindex != offset / PAGE_SIZE) //advance to new page if necessary
        {
            if(p!=NULL)
            {
                unlock(p->rwlatch);
                releasePage(p);
            }
            
            pindex = offset / PAGE_SIZE;

            if(pindex == dp->pcount)//past end
                return;
            
            //p = loadPage(xid, dp->pidarr[pindex]);
            p = loadPageOfType(xid, dp->pidarr[pindex], SEGMENT_PAGE);
            readlock(p->rwlatch,0);            
        }

        if(pindex == dp->pcount - 1 && (PAGE_SIZE - (offset % PAGE_SIZE) < sizeof(int32_t)))
            return;

        int32_t *dsize_ptr=0;
        if(PAGE_SIZE - (offset % PAGE_SIZE) < sizeof(int32_t)) //int spread in two pages        
            dp->readbytes(xid, offset, sizeof(int32_t), (byte**)(&dsize_ptr));        
        else //int in a single page
            dsize_ptr = (int32_t*)stasis_page_byte_ptr_from_start(p, offset % PAGE_SIZE);
        
        offset += sizeof(int32_t);
                
        if(*dsize_ptr == 0) //no more keys
        {            
            unlock(p->rwlatch);
            releasePage(p);
            return;
        }

        offset += *dsize_ptr;

    }

}
