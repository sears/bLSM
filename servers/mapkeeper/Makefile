# Makefile
#
# Copyright 2012 Yahoo! Inc.
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
THRIFT_DIR = /home/sears/local/thrift

CPPFLAGS =-I../../../stasis -I../.. -I. -I $(THRIFT_DIR)/include/thrift -I../../../mapkeeper/thrift/gen-cpp
CXXFLAGS =-g -O3

LDFLAGS=-lpthread -lblsm -lstasis -lmapkeeper -lthrift 	\
	   -L $(THRIFT_DIR)/lib -L ../../../mapkeeper/thrift/gen-cpp	 		\
	   -L ../../build -L ../../../stasis/build/src/stasis 				\
	   -Wl,-rpath,\$$ORIGIN/../../../build						\
           -Wl,-rpath,\$$ORIGIN/../../../../stasis/build/src/stasis			\
           -Wl,-rpath,\$$ORIGIN/../../../../mapkeeper/thrift/gen-cpp			\
           -Wl,-rpath,/usr/local/lib							\
           -Wl,-rpath,$(THRIFT_DIR)/lib

all: main/blsm_client main/blsm_server

main/blsm_client :

main/blsm_server : bLSMRequestHandler.cpp

clean:
	rm -f main/blsm_client main/blsm_server
