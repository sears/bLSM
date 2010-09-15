# yinst i log4cpp_y_dev-1.4.2 

CPPFLAGS=-Wall -I/homes/sears/svndev/trunk/dht/common/include -I/homes/sears/svndev/trunk/dht/storage/handler/src -I/homes/sears/svndev/trunk/dht/storage/handler/include/ -I/homes/sears/svndev/trunk/dht/framework/base/include/ -I /homes/sears/svndev/trunk/dht/externals/fwcore/include -I /home/y/include -I ..  -c -fPIC
CXXFLAGS=-m32
CFLAGS=-m32

libsherpalogstore.so : LSMPersistentStoreImpl.o tcpclient.o LSMPersistentParentImpl.o
	gcc -m32 -shared -Wl,-soname,libsherpalogstore.so -o libsherpalogstore.so LSMPersistentStoreImpl.o tcpclient.o  LSMPersistentParentImpl.o
	cp libsherpalogstore.so /homes/sears/sherpa-so/

#LSMPersistentStoreImpl.o : LSMPersistentStoreImpl.cc

tcpclient.cpp : ../tcpclient.cpp
	ln -s ../tcpclient.cpp

clean:
	rm -f LSMPersistentStoreImpl.o tcpclient.o libsherpalogstore.so
