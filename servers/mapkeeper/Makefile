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

main/blsm_server : LSMServerHandler.cc

clean:
	rm -f main/blsm_client main/blsm_server
