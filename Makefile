
include ../mapkeeper/Makefile.config

INC = -I ../stasis 							\
      -I ../mapkeeper/thrift/gen-cpp/ 					\
      -I $(THRIFT_DIR)/include/ 					\
      -I $(THRIFT_DIR)/include/thrift					\
      -I ./sherpa

LIBSRC = $(wildcard *.c) $(wildcard *.cpp)
LIBNAME = logstore

ifeq ($(shell uname),Linux)
STATIC_LIBS= ../mapkeeper/thrift/gen-cpp/libmapkeeper.a	\
	 -lrt sherpa/LSMServerHandler.cc bin/liblogstore.a		\
	../stasis/bin/libstasis.a
else
STATIC_LIBS= ./libstdc++.a ../mapkeeper/thrift/gen-cpp/libmapkeeper.a	\
	 sherpa/LSMServerHandler.cc bin/liblogstore.a			\
	../stasis/bin/libstasis.a
endif

MAINSRC = $(wildcard main/*.cpp) $(wildcard sherpa/main/*.cc)

include ../stasis/config/Makefile.stasis
