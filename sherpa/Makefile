ROOT=/home/y
include $(ROOT)/share/yahoo_cfg/Make.defs

USE_OSE = no

LIB_NAME        = LSMServer
SHLIB_VERSION   = 1

CXXSRC		= LSMServerHandler.cc

LCLEAN		+= *~ 
WARN            += -Werror -Wall
LINC		+= -I. -I /home/y/include/thrift -I /home/y/include64/stasis -I ..

LDLIBS +=	-L/usr/local/lib
LDFLAGS += -Wl,-rpath,/home/y/lib64 -Wl,-rpath,../build


# Poor packaging for yicu
LINC +=         -I/home/y/include/yicu

LDLIBS		+= -lthrift -ldht_persistent_store -lstasis -llogstore -L ../build

# Need to remove potential warnings in yapache.
LDEF += -DEAPI
LDEF += -D_FILE_OFFSET_BITS=64

ifdef UNIT_TEST
CXXFLAGS += -O0 -g -DUNIT_TEST
endif

ifdef DEBUG
CXXFLAGS += -O0 -g
endif

include $(ROOT)/share/yahoo_cfg/Make.rules

$(SOTARGET): $(OBJS)
