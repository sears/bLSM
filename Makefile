
include ../mapkeeper/Makefile.config

INC = -I ../stasis 							\
      -I ../mapkeeper/thrift/gen-cpp/ 					\
      -I $(THRIFT_DIR)/include/ 					\
      -I $(THRIFT_DIR)/include/thrift					\
      -I ./servers/mapkeeper

LIBSRC = $(wildcard *.c) $(wildcard *.cpp)
LIBNAME = blsm

ifeq ($(shell uname),Linux)
STATIC_LIBS= ../mapkeeper/thrift/gen-cpp/libmapkeeper.a	\
	 -lrt servers/mapkeeper/bLSMRequestHandler.cpp bin/libblsm.a		\
	../stasis/bin/libstasis.a
else
STATIC_LIBS= ./libstdc++.a ../mapkeeper/thrift/gen-cpp/libmapkeeper.a	\
	 servers/mapkeeper/bLSMRequestHandler.cpp bin/libblsm.a			\
	../stasis/bin/libstasis.a
endif

MAINSRC = $(wildcard servers/native-XXX/*.cpp) $(wildcard servers/mapkeeper/main/*.cpp)

include ../stasis/config/Makefile.stasis
