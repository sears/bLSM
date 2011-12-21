INC = -I ../stasis 							\
      -I ../mapkeeper/thrift/gen-cpp/ 					\
      -I ../../local/thrift/include/ 					\
      -I ../../local/thrift/include/thrift 				\
      -I ./sherpa

LIBSRC = $(wildcard *.c) $(wildcard *.cpp)
LIBNAME = logstore

STATIC_LIBS= ./libstdc++.a ../mapkeeper/thrift/gen-cpp/libmapkeeper.a	\
	 -lrt sherpa/LSMServerHandler.cc bin/liblogstore.a		\
	../stasis/bin/libstasis.a
MAINSRC = $(wildcard main/*.cpp) $(wildcard sherpa/main/*.cc)

include ../stasis/config/Makefile.stasis