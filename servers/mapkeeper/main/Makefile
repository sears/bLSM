include /home/y/share/yahoo_cfg/Make.defs

USE_OSE = no

EXETARGET = lsm_server lsm_client

CXXSRC		= $(addsuffix .cc, $(EXETARGET))

LCLEAN		+= *~ 
WARN            += -Werror -Wall

LINC		+= -I../ -I /usr/local/include/thrift -I /home/y/include/boost/tr1/ -I ../../ -I ../../../thrift/gen-cpp/ 
LDLIBS		+= -L../ -lmapkeeper -lLSMServer -L ../../../thrift/gen-cpp
LDFLAGS += -Wl,-rpath,/home/y/lib64 -Wl,-rpath,../../build

ifdef UNIT_TEST
CXXFLAGS += -O0 -g -DUNIT_TEST
endif

ifdef DEBUG
CXXFLAGS += -O0 -g
endif

include /home/y/share/yahoo_cfg/Make.rules

$(EXETARGET): $(addsuffix .o, $@)
