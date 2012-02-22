#!/bin/bash
mkdir -p ~/stuff/lib/
mkdir -p ~/stuff/include/

cp build/liblogstore_client.so ~/stuff/lib/
cp network.h datatuple.h tcpclient.h ~/stuff/include/
cp sherpa/LSMPersistentStoreImpl.cc  sherpa/LSMPersistentStoreImpl.h ~/stuff/sherpa_logstore/


echo ' in the yroot, do this: '

echo ' pushd /usr/local/include ; sudo ln -s /home/sears/stuff/include/*.h . ; popd '
echo ' pushd /usr/local/lib ; sudo ln -s /home/sears/stuff/lib/*.so . ; popd '
echo ' pushd ~/svndev/trunk/dht/storage/handler/src/ ; ln -s ~/stuff/sherpa_logstore/* . ; popd '

