# CMakeLists.txt
#
# Copyright 2009-2012 Yahoo! Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
IF( HAVE_STASIS )
  CREATE_CHECK(check_gen)
  CREATE_CHECK(check_bloomFilter)
  CREATE_CHECK(check_testAndSet)
  CREATE_CHECK(check_logtree)
  CREATE_CHECK(check_datapage)
  CREATE_CHECK(check_logtable)
  CREATE_CHECK(check_merge)
  CREATE_CHECK(check_mergelarge)
  CREATE_CHECK(check_mergetuple)
  CREATE_CHECK(check_rbtree)
#  CREATE_CLIENT_EXECUTABLE(check_tcpclient)  # XXX should build this on non-stasis machines
#  CREATE_CLIENT_EXECUTABLE(check_tcpbulkinsert)  # XXX should build this on non-stasis machines
ENDIF( HAVE_STASIS )
