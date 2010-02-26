/**
 *  \file LSMPersistentStoreImpl.h
 *  \brief This file is a wrapper over the LSM-Tree network protocol.
 *
 *  Copyright (c) 2008 Yahoo, Inc.
 *  All rights reserved.
 */

#ifndef LSM_PSTORE_IMPL_H
#define LSM_PSTORE_IMPL_H

#include "PersistentStore.h"
//#include "LSMCoreImpl.h"

struct logstore_handle_t;

class LSMPersistentStoreImpl : public PersistentStore
{

private:
  //    LSMCoreImpl& mySQLCoreImpl_;
  //    bool isOrdered_;
  unsigned char * my_strcat(const std::string& table,
			    const std::string& tablet,
			    const std::string& key,
			    size_t * len);
  unsigned char * buf_key(const TabletMetadata& m, const RecordKey& r,
			  size_t * len);
  unsigned char * buf_val(const StorageRecord &val,
			  size_t * len);
  SuCode::ResponseCode val_buf(StorageRecord &ret,
	       const unsigned char * buf, size_t buf_len);
 public:
    LSMPersistentStoreImpl(bool ordered);
    virtual ~LSMPersistentStoreImpl();

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
     * See PersistentStore API
     */
    StorageRecordIterator 
    scan(const TabletMetadata& tabletMeta, const ScanContinuation& continuation, 
         bool getMetadataOnly, const uint64_t expiryTime, unsigned int scanLimit);

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

    logstore_handle_t * l_;

};

#endif /*LSM_PSTORE_IMPL_H*/
