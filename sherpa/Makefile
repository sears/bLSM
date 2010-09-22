# yinst i log4cpp_y_dev-1.4.2 

SHERPA_DIR = /homes/sears/svndev/trunk/dht

CPPFLAGS=-Wall -I${SHERPA_DIR}/common/include \
			   -I${SHERPA_DIR}/storage/handler/src \
			   -I${SHERPA_DIR}/storage/handler/include/ \
			   -I${SHERPA_DIR}/framework/base/include/ \
			   -I${SHERPA_DIR}/externals/fwcore/include \
			   -I /home/y/include -I .. -c -fPIC
CXXFLAGS=-m32 -g
CFLAGS=-m32 -g

libsherpalogstore.so : LSMPersistentStoreImpl.o tcpclient.o LSMPersistentParentImpl.o
	gcc -m32 -g -shared -Wl,-soname,libsherpalogstore.so -o libsherpalogstore.so LSMPersistentStoreImpl.o tcpclient.o  LSMPersistentParentImpl.o
	cp libsherpalogstore.so /homes/sears/sherpa-so/

tcpclient.cpp : ../tcpclient.cpp
	ln -s ../tcpclient.cpp

clean:
	rm -f LSMPersistentStoreImpl.o tcpclient.o libsherpalogstore.so
