
#include <stasis/transactional.h>
#undef begin
#undef end


#include "logstore.h"
#include "regionAllocator.h"

int main(int argc, char **argv)
{
    unlink("storefile.txt");
    unlink("logfile.txt");
    system("rm -rf stasis_log/");

    sync();

    logtable::init_stasis();

    int xid = Tbegin();

    logtable *ltable = new logtable(1000, 10000, 5);

    recordid table_root = ltable->allocTable(xid);

    Tcommit(xid);

    xid = Tbegin();
    RegionAllocator * ro_alloc = new RegionAllocator();

    diskTreeComponent::internalNodes::iterator * it = new diskTreeComponent::internalNodes::iterator(xid,ro_alloc, ltable->get_tree_c2()->get_root_rid() );
    it->close();
    delete it;
    delete ro_alloc;
    Tcommit(xid);
    delete ltable;
    logtable::deinit_stasis();



}
