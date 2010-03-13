

#include "logstore.h"

int main(int argc, char **argv)
{
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    diskTreeComponent::internalNodes::init_stasis();

    int xid = Tbegin();

    logtable ltable(1000, 10000, 5);

    recordid table_root = ltable.allocTable(xid);

    Tcommit(xid);

    xid = Tbegin();
    diskTreeComponent::internalNodes::iterator * it = new diskTreeComponent::internalNodes::iterator(xid,ltable.get_tree_c2()->get_root_rid() );
    it->close();
    delete it;
    Tcommit(xid);

    diskTreeComponent::internalNodes::deinit_stasis();



}
