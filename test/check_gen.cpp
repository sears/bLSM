

#include "logstore.h"

int main(int argc, char **argv)
{
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    logtree::init_stasis();

    int xid = Tbegin();

    logtable ltable;

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    //ltable.startTable();

//    lsmTableHandle<PAGELAYOUT>* h = TlsmTableStart<PAGELAYOUT>(lsmTable, INVALID_COL);

    xid = Tbegin();
    lladdIterator_t * it = logtreeIterator::open(xid,ltable.get_tree_c2()->get_root_rec() );
    logtreeIterator::close(xid, it);
    Tcommit(xid);

    logtree::deinit_stasis();



}
