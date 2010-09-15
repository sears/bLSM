/**
 *  \file LSMPersistentStoreImpl.h
 *  \brief This file is a wrapper over the LSM-Tree network protocol.
 *
 *  Copyright (c) 2008 Yahoo, Inc.
 *  All rights reserved.
 */

#ifndef LSM_PSTORE_IMPL_H
#define LSM_PSTORE_IMPL_H

#include <fstream>
#include <iostream>

#include "PersistentStore.h"
#include "datatuple.h"
//#include "LSMCoreImpl.h"

struct logstore_handle_t;

class LSMIterator;

class LSMPersistentStoreImpl : public PersistentStore
{
friend class LSMIterator;
friend class LSMPersistentParent;
protected:
  //    LSMCoreImpl& mySQLCoreImpl_;
  bool isOrdered_;
  unsigned char * my_strcat(const std::string& table,
			    const std::string& tablet,
			    const std::string& key,
			    size_t * len);
  void my_strtok(const unsigned char* in, size_t len, std::string& table, std::string& tablet, std::string& key);
  unsigned char * buf_key(const TabletMetadata& m, const RecordKey& r,
			  size_t * len);
  unsigned char * buf_key(const TabletMetadata& m, const std::string s,
              size_t * len);
  unsigned char * buf_val(const StorageRecord &val,
			  size_t * len);
  SuCode::ResponseCode tup_buf(StorageRecord &ret, datatuple * tup);
  SuCode::ResponseCode key_buf(StorageRecord &ret,
           const unsigned char * buf, size_t buf_len);
  SuCode::ResponseCode val_buf(StorageRecord &ret,
	       const unsigned char * buf, size_t buf_len);
 public:
  std::fstream filestr;

  LSMPersistentStoreImpl(bool ordered);
    virtual ~LSMPersistentStoreImpl();

    SuCode::ResponseCode initMetadataMetadata(TabletMetadata& m);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode init(const SectionConfig& config);
    
    /**
     * See PersistentStore API
     */
    bool isOrdered();

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode addEmptyTablet(TabletMetadata& tabletMeta);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode dropTablet(TabletMetadata& tabletMeta);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode clearTabletRange(TabletMetadata& tabletMeta,
                                          uint32_t removalLimit);

    /**
       Not part of PersistentStore api.  PersistentParent needs to
       provide this method as well, but it gets a string instead of a
       TabletMetadata...
    */
    SuCode::ResponseCode getApproximateTableSize(std::string tabletMeta,
                              int64_t& tableSize,
                              int64_t & rowCount);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode getApproximateTableSize(TabletMetadata& tabletMeta,
                                                 int64_t& tableSize,
                                                 int64_t & rowCount);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode get(const TabletMetadata& tabletMeta,
                             StorageRecord& recordData);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode update(const TabletMetadata& tabletMeta,
                                const StorageRecord& updateData);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode insert(const TabletMetadata& tabletMeta,
                                const StorageRecord& insertData);
    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode remove(const TabletMetadata& tabletMeta,
                                const RecordKey& recordKey);
    /**
     * Not part of PersistentStore API. However, PersistentParent needs to implement ping
     */
    bool ping();

    /**
     * See PersistentStore API
     */
    StorageRecordIterator
    scan(const TabletMetadata& tabletMeta, const ScanContinuation& continuation,
         ScanSelect::Selector selector, const uint64_t expiryTime, unsigned int scanLimit, size_t byteLimit);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode getSnapshotExporter(const TabletMetadata& tabletMeta,
                                        const std::string& snapshotId,
                                        SnapshotExporterAutoPtr& exporter);

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode getSnapshotImporter(const TabletMetadata& tabletMeta,
                                        const std::string& version,
                                        const std::string& snapshotId,
                                        SnapshotImporterAutoPtr& snapshot) ;

    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode getIncomingCopyProgress(const TabletMetadata& metadata,
                                 const std::string& snapshotId, 
                                 int64_t& current, 
                                 int64_t& estimated) const;
    
    /**
     * See PersistentStore API
     */
    SuCode::ResponseCode getOutgoingCopyProgress(const TabletMetadata& metadata,
                                 const std::string& snapshotId, 
                                 int64_t& current, 
                                 int64_t& estimated) const;
private:
    /**
     * connect to the database. Noop if already connected.
     *
     * @return SuCode::SuOk if successful, error otherwise
     */
    //    SuCode::ResponseCode connect();

private:
    LSMPersistentStoreImpl(LSMPersistentStoreImpl &);
    LSMPersistentStoreImpl operator=(LSMPersistentStoreImpl &);

//    unsigned char* TabletMetadataToString(const TabletMetadata&m, size_t* keylen);
//    unsigned char* TabletMetadataToKey(const TabletMetadata&m, size_t *key_len);

    void metadata_buf(TabletMetadata &m, const unsigned char * buf, size_t len);
    void buf_metadata(unsigned char ** buf, size_t *len, const TabletMetadata &m);
protected:
    logstore_handle_t * l_;

};

#endif /*LSM_PSTORE_IMPL_H*/
