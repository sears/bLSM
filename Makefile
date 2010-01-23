STASIS_DIR=../stasis

LIB=$(STASIS_DIR)/build/src/stasis \
	-L/home/y/lib
INCLUDE=-I$(STASIS_DIR)/src/ -I$(STASIS_DIR) -I./ \
	-I/home/y/include

LIBLIST=-lpthread \
	-lstasis \
	-lm 
#	-licui18n \
#	-licuuc \
#	-licudata \
#	-licuio \
#	-llog4cpp_y \
#	-lthoth 

FLAGS=-pg -g -O1
#FLAGS=-O3

HFILES=logserver.h logstore.h logiterators.h datapage.h merger.h tuplemerger.h datatuple.h
CFILES=logserver.cpp logstore.cpp logiterators.cpp datapage.cpp merger.cpp tuplemerger.cpp 	


# STASIS_DIR=../stasis
# LD_LIBRARY_PATH=$STASIS_DIR/build/src/stasis 
# LD_LIBRARY_PATH=$STASIS_DIR/build/src/stasis ./hello


logstore: check_gen.cpp	$(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

test:	dp_check lt_check ltable_check merger_check rb_check  \
	lmerger_check tmerger_check server_check tcpclient_check

lt_check: check_logtree.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

dp_check: check_datapage.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

ltable_check: check_logtable.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

merger_check: check_merge.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

lmerger_check: check_mergelarge.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

tmerger_check: check_mergetuple.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

rb_check: check_rbtree.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

server_check:  check_server.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

tcpclient_check:  check_tcpclient.cpp $(HFILES) $(CFILES)
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)


hello : hello.cpp UCharUtils.cc LogUtils.cc
	g++ -o $@ $^ -L$(LIB) $(INCLUDE) $(LIBLIST) $(FLAGS)

clean: 
	rm -f logstore server_check hello lt_check merger_check lmerger_check rb_check \
	dp_check ltable_check tmerger_check rose tcpclient_check
veryclean: clean
	rm -f *~ gmon.out prof.res



