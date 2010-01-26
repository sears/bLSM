

#include "logstore.h"

int main(int argc, char **argv)
{
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    //    PAGELAYOUT::initPageLayout();
    
    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    logtable ltable;

    recordid table_root = ltable.allocTable(xid);    

    Tcommit(xid);

    //ltable.startTable();

//    lsmTableHandle<PAGELAYOUT>* h = TlsmTableStart<PAGELAYOUT>(lsmTable, INVALID_COL);

    xid = Tbegin();
    logtreeIterator::open(xid,ltable.get_tree_c2()->get_root_rec() );
    Tcommit(xid);
    

    Tdeinit();



}
