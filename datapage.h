#ifndef _SIMPLE_DATA_PAGE_H_
#define _SIMPLE_DATA_PAGE_H_

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>



template<class TUPLE>
class DataPage
{
public:
    
    class RecordIterator
    {
    public:
        RecordIterator(DataPage *dp)
            {
                offset = HEADER_SIZE;
                this->dp = dp;
            }

        RecordIterator(const RecordIterator &rhs)
            {
                this->offset = rhs.offset;
                this->dp = rhs.dp;            
            }

        void operator=(const RecordIterator &rhs)
            {
                this->offset = rhs.offset;
                this->dp = rhs.dp;
            }
        

        //returns the next tuple and also advances the iterator
        TUPLE *getnext(int xid);

        //advance the iterator by count tuples, i.e. skip over count tuples         
        void advance(int xid, int count=1);
        
        
        int32_t offset ;
        DataPage *dp;
        
        
    };

    
public:

    //to be used when reading an existing data page from disk
    DataPage( int xid, pageid_t pid );

    //to be used to create new data pages
    DataPage( int xid, int fix_pcount, pageid_t (*alloc_region)(int, void*), void * alloc_state);

    ~DataPage();

    inline bool append(int xid, TUPLE const & dat);
    bool recordRead(int xid, typename TUPLE::key_t key, size_t keySize,  TUPLE ** buf);

    inline uint16_t recordCount(int xid);


    RecordIterator begin(){return RecordIterator(this);}

    pageid_t get_start_pid(){return pidarr[0];}
    int get_page_count(){return pcount;}

    static pageid_t dp_alloc_region(int xid, void *conf);
    
    static pageid_t dp_alloc_region_rid(int xid, void * ridp);

    static void dealloc_region_rid(int xid, void* conf);

    static void force_region_rid(int xid, void *conf);

public:
    
private:

    void initialize(int xid);

    //reads the page count information from the first page
    int readPageCount(int xid, pageid_t pid);
    void incrementPageCount(int xid, pageid_t pid, int add=1);

    bool writebytes(int xid, int count, byte *data);
    inline void readbytes(int xid, int32_t offset, int count, byte **data=0);

private:
    int fix_pcount; //number of pages in a standard data page
    int pcount;
    pageid_t *pidarr;
    int32_t byte_offset;//points to the next free byte


    //page alloc function
    pageid_t (*alloc_region)(int, void*);
    void *alloc_state;

    static const int32_t HEADER_SIZE;
    

};

#endif
