/*
 * check_gen.cpp
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
 *      Author: sears
 */
#include <stasis/transactional.h>
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
