/**
 *  \file LSMPersistentStoreImpl.cc
 *
 *  Copyright (c) 2008 Yahoo, Inc.
 *  All rights reserved.
 */
#include "LSMPersistentStoreImpl.h"

#include <dht/LogUtils.h>
#include <dht/ActionContext.h>

#include <tcpclient.h>

// Initialize the logger
static log4cpp::Category &log =
       log4cpp::Category::getInstance("dht.su."__FILE__);

unsigned char* LSMPersistentStoreImpl::my_strcat(const std::string& table,
						 const std::string& tablet,
						 const std::string& key,
						 size_t * len) {
  const char * a = table.c_str();
  size_t alen =    table.length();
  const char * b = tablet.c_str();
  size_t blen =    tablet.length();
  const char * c = key.c_str();
  size_t clen =    key.length();
  // The following two bytes cannot occur in valid utf-8
  const unsigned char low_eos  = (unsigned char) 0xFE;
  const unsigned char high_eos = (unsigned char) 0xFF;
  const unsigned char zero     = (unsigned char) 0x00;
  (void) high_eos;
  *len = alen + 1 + blen + 1 + clen + 1;
  unsigned char * ret = (unsigned char*)malloc(*len);
  unsigned char * buf = ret;
  memcpy(buf, a, alen);     buf += alen;
  memcpy(buf, &low_eos, 1); buf ++;
  memcpy(buf, b, blen);     buf += blen;
  memcpy(buf, &low_eos, 1); buf ++;
  memcpy(buf, c, clen);     buf += clen;
  memcpy(buf, &zero, 1);    buf = 0;
  return ret;
}

unsigned char *
LSMPersistentStoreImpl::buf_key(const TabletMetadata&m,
			const RecordKey& k, size_t * len) {
  return my_strcat(m.table(), m.tablet(), k.name(), len);
}

unsigned char *
LSMPersistentStoreImpl::buf_val(const StorageRecord& val, size_t * len) {
  uint64_t expiryTime = val.expiryTime();
  uint32_t dataBlob_len, metadata_len; // Below, we assume these are of the same type.

  const UtilityBuffer& dataBlob = val.dataBlob();
  const UtilityBuffer& metadata = val.metadata();

  dataBlob_len = dataBlob.dataSize();
  metadata_len = metadata.dataSize();

  DHT_DEBUG_STREAM() << "write storage record expiryTime " << expiryTime << " metadata len " << metadata_len << " datalen " << dataBlob_len;


  *len = sizeof(expiryTime) + 2 * sizeof(dataBlob_len) + dataBlob_len + metadata_len;

  unsigned char * ret = (unsigned char *) malloc(*len);
  unsigned char * buf = ret;
  memcpy(buf, &expiryTime,   sizeof(expiryTime));    buf += sizeof(expiryTime);
  memcpy(buf, &metadata_len, sizeof(metadata_len));  buf += sizeof(metadata_len);
  memcpy(buf, &dataBlob_len, sizeof(dataBlob_len));  buf += sizeof(dataBlob_len);
  memcpy(buf, const_cast<UtilityBuffer&>(metadata).buffer(),
	                     metadata_len);      buf += metadata_len;
  memcpy(buf, const_cast<UtilityBuffer&>(dataBlob).buffer(),
	                     dataBlob_len);      buf += dataBlob_len;

  return ret;
}

SuCode::ResponseCode
LSMPersistentStoreImpl::val_buf(StorageRecord& ret,
				const unsigned char * buf, size_t buf_len) {
  uint64_t expiryTime;
  uint32_t dataBlob_len, metadata_len;
  assert(buf_len >= sizeof(expiryTime) + sizeof(dataBlob_len) + sizeof(metadata_len));

  // Copy header onto stack.

  memcpy(&expiryTime,   buf, sizeof(expiryTime));    buf += sizeof(expiryTime);
  memcpy(&metadata_len, buf, sizeof(metadata_len));  buf += sizeof(metadata_len);
  memcpy(&dataBlob_len, buf, sizeof(dataBlob_len));  buf += sizeof(dataBlob_len);

  DHT_DEBUG_STREAM() << "read storage record expiryTime " << expiryTime << " metadata len " << metadata_len << " datalen " << dataBlob_len;

  // Is there room in ret?

  assert(buf_len >= sizeof(expiryTime) + sizeof(dataBlob_len) + sizeof(metadata_len)
	          + metadata_len + dataBlob_len);

  if(ret.metadata().bufSize() < metadata_len ||
     ret.dataBlob().bufSize() < dataBlob_len) {
    return SuCode::PStoreDataTruncated; // RCS: This is what the mysql implementation does.
                                        // it's someewhat misleading, as we don't truncate anything.
  }

  // Copy the data into ret.

  //  ret->setName(recordName); // somebody else's problem....
  ret.setExpiryTime(expiryTime);

  memcpy(ret.metadata().buffer(), buf, metadata_len);      buf += metadata_len;
  memcpy(ret.dataBlob().buffer(), buf, dataBlob_len);      buf += dataBlob_len;
  ret.metadata().setDataSize(metadata_len);
  ret.dataBlob().setDataSize(dataBlob_len);
  return SuCode::SuOk;
}
LSMPersistentStoreImpl::
LSMPersistentStoreImpl(bool isOrdered) : l_(NULL) { }

LSMPersistentStoreImpl::
~LSMPersistentStoreImpl()
{
  logstore_client_close(l_);
}

SuCode::ResponseCode LSMPersistentStoreImpl::
init(const SectionConfig &config)
{
  if(!l_) // workaround bug 2870547
    l_ = logstore_client_open("localhost", 32432, 60);  // XXX hardcode none of these values
  return l_ ? SuCode::SuOk : FwCode::NotFound;
}

bool LSMPersistentStoreImpl::
isOrdered(){
  return true;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
addEmptyTablet(TabletMetadata& tabletMeta)
{
  // This is a no-op; we'll simply prepend the tablet string to each record.
  const std::string& mySQLTableName = tabletMeta.getTabletId();

  if (mySQLTableName!=""){
    return SuCode::PStoreTabletAlreadyExists;
  }
  /*
    std::string newLSMTableName;

    SuCode::ResponseCode rc;
    if ((rc = mySQLCoreImpl_.addEmptyTablet(tabletMeta.table(),
					    tabletMeta.tablet(),
					    newLSMTableName,
					    isOrdered_)) !=
	      SuCode::SuOk) {

	return rc;
    }

    //Save the mysql table name back in TabletMetadata
    tabletMeta.setTabletId(newLSMTableName); */
    return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
dropTablet(TabletMetadata& tabletMeta)
{
  DHT_DEBUG_STREAM() << "dropTablet called.  Falling back on clearTabletRange()";
  return clearTabletRange(tabletMeta, 0);
  /*    SuCode::ResponseCode rc;
    const std::string& mySQLTableName = tabletMeta.getTabletId();

    if ((rc = mySQLCoreImpl_.cleanupTablet(mySQLTableName)) != SuCode::SuOk) {
	return SuCode::PStoreTabletCleanupFailed;
    }

    tabletMeta.setTabletId("");

    DHT_DEBUG_STREAM() << "successfully dropped table from mysql"; */
}

// alternate to dropTablet() for when a tablet has been split and the underlying
// mysql table is being shared...just wipe out the record range for the tablet
// that is being dropped.
SuCode::ResponseCode LSMPersistentStoreImpl::
clearTabletRange(TabletMetadata& tabletMeta, uint32_t removalLimit)
{
  DHT_DEBUG_STREAM() << "clear tablet range is unimplemented.  ignoring request";
  //    const std::string& mySQLTableName = tabletMeta.getTabletId();
  //  return mySQLCoreImpl_.deleteRange(mySQLTableName, tabletMeta.tablet(), removalLimit);
  return SuCode::SuOk;
}


SuCode::ResponseCode LSMPersistentStoreImpl::
getApproximateTableSize(TabletMetadata& tabletMeta,
			int64_t& tableSize,
			int64_t & rowCount)
{
  DHT_DEBUG_STREAM() << "get approximate table size is unimplemented.  returning dummy values";
  tableSize = 1024 * 1024 * 1024;
  rowCount = 1024 * 1024;
  //  const std::string& mySQLTableName = tabletMeta.getTabletId();
  //    return mySQLCoreImpl_.getApproximateTableSize(mySQLTableName, tableSize, rowCount);
  return SuCode::SuOk;
}


SuCode::ResponseCode LSMPersistentStoreImpl::
get(const TabletMetadata& tabletMeta, StorageRecord& recordData)
{
  size_t buflen;
  unsigned char * buf = buf_key(tabletMeta, recordData.recordKey(), &buflen);
  datatuple * key_tup = datatuple::create(buf, buflen);
  datatuple * result = logstore_client_op(l_, OP_FIND, key_tup);
  datatuple::freetuple(key_tup);
  SuCode::ResponseCode ret;
  if((!result) || result->isDelete()) {
    ret = SuCode::PStoreRecordNotFound;
  } else {
    ret = val_buf(recordData, result->data(), result->datalen());
  }
  free(buf);
  datatuple::freetuple(result);
  return ret;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
update(const TabletMetadata& tabletMeta, const StorageRecord& updateData)
{
  SuCode::ResponseCode ret;
  {  /// XXX hack.  Copy of get() implementation, without the memcpy.
    size_t buflen;
    unsigned char * buf = buf_key(tabletMeta, updateData.recordKey(), &buflen);
    datatuple * key_tup = datatuple::create(buf, buflen);
    datatuple * result = logstore_client_op(l_, OP_FIND, key_tup);
    datatuple::freetuple(key_tup);

    if((!result) || result->isDelete()) {
      RESPONSE_ERROR_STREAM(SuCode::PStoreRecordNotFound) << "EC:PSTORE:No matching " <<
	                    " record to update";
      ret = SuCode::PStoreRecordNotFound;
    } else {
      // skip val_buf ...
      ret = SuCode::SuOk;
    }
    free(buf);
    if(result) { datatuple::freetuple(result); }  // XXX differentiate between dead connection and missing tuple
  }
  if(ret == SuCode::PStoreRecordNotFound) { return ret; }
  return insert(tabletMeta, updateData);
}

SuCode::ResponseCode LSMPersistentStoreImpl::  // XXX what to do about update?
insert(const TabletMetadata& tabletMeta,
       const StorageRecord& insertData) {
  size_t keybuflen, valbuflen;
  unsigned char * keybuf = buf_key(tabletMeta, insertData.recordKey(), &keybuflen);
  unsigned char * valbuf = buf_val(insertData, &valbuflen);
  datatuple * key_ins = datatuple::create(keybuf, keybuflen, valbuf, valbuflen);
  datatuple * result = logstore_client_op(l_, OP_INSERT, key_ins);
  datatuple::freetuple(result);
  free(keybuf);
  free(valbuf);
  return result ? SuCode::SuOk : SuCode::PStoreUnexpectedError;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
remove(const TabletMetadata& tabletMeta, const RecordKey& recordName)
{
  size_t buflen;
  unsigned char * buf = buf_key(tabletMeta, recordName, &buflen);
  datatuple * key_ins = datatuple::create(buf, buflen);
  datatuple * result = logstore_client_op(l_, OP_INSERT, key_ins);
  datatuple::freetuple(result);
  free(buf);
  return result ? SuCode::SuOk : SuCode::PStoreUnexpectedError;
}

StorageRecordIterator LSMPersistentStoreImpl::
scan(const TabletMetadata& tabletMeta, const ScanContinuation& continuation,
     bool getMetadataOnly, const uint64_t expiryTime, unsigned int scanLimit)
{

  /*    const std::string& mySQLTableName = tabletMeta.getTabletId();

    ScanContinuationAutoPtr newContinuation;
    TabletRangeAutoPtr tabletRange;

    if (SuCode::SuOk != tabletMeta.keyRange(tabletRange)){
	BAD_CODE_ABORT("Bad tablet name");
	} */

    /* This is necessary once we turn on splits, because multiple tablets
     * might reside in the same mysql table. Shouldnt scan beyond the
     * upper limit of the tablet because that might be stale data left
     * from before this tablet split.
     */
  /*    newContinuation = continuation.getContinuationLimitedToTabletRange(
				       *tabletRange);

    LSMIterator* iter = new LSMIterator(mySQLTableName, newContinuation,
					    getMetadataOnly,
					    expiryTime, scanLimit);
					    return StorageRecordIterator(iter); */
  return StorageRecordIterator(NULL);

}

SuCode::ResponseCode LSMPersistentStoreImpl::
getSnapshotExporter(const TabletMetadata& tabletMeta,
		    const std::string& snapshotId,
	       SnapshotExporterAutoPtr& exporter)
{
  /*    const std::string& mySQLTableName = tabletMeta.getTabletId();

    TabletRangeAutoPtr tabletRange;
    RETURN_IF_NOT_OK(tabletMeta.keyRange(tabletRange));

    ScanContinuationAutoPtr cont = tabletRange->getContinuationForTablet();

    exporter =  SnapshotExporterAutoPtr(
		    new LSMSnapshotExporter(mySQLTableName,cont,snapshotId));
  */
    return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
getSnapshotImporter(const TabletMetadata& tabletMeta,
		    const std::string& version,
		    const std::string& snapshotId,
		    SnapshotImporterAutoPtr& importer)
{
  /*    if (version == LSMSnapshotExporter::VERSION){
	const std::string& mySQLTableName = tabletMeta.getTabletId();
	importer=LSMSnapshotExporter::getImporter(tabletMeta.table(),
						    tabletMeta.tablet(),
						    snapshotId,
						    mySQLTableName);
	return SuCode::SuOk;
    }else{
	RESPONSE_ERROR_STREAM(SuCode::PStoreUnexpectedError) <<
	    "EC:IMPOSSIBLE:Unknown snapshot version " << version <<" while trying to " <<
	    "import to tablet " << tabletMeta.tablet() << "of table " <<
	    tabletMeta.table();
	return SuCode::PStoreUnexpectedError;
    }*/
  return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
getIncomingCopyProgress(const TabletMetadata& metadata,
			const std::string& snapshotId,
			int64_t& current,
			int64_t& estimated) const
{
  fprintf(stderr, "unsupported method getIncomingCopyProgrees called\n");

    //This will be a problem when we have more than 1
    //exporter/importer type. We will have to store the
    //snapshot version somewhere in tablet metadata
  /*    const std::string& mySQLTableName = metadata.getTabletId();

    return LSMSnapshotExporter::
		       getIncomingCopyProgress(mySQLTableName,
						   snapshotId,
						   current,
						   estimated); */
  return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
getOutgoingCopyProgress(const TabletMetadata& metadata,
			const std::string& snapshotId,
			int64_t& current,
			int64_t& estimated) const
{
  fprintf(stderr, "unsupported method getOutgoingCopyProgrees called\n");
  return 1;
  //    return LSMSnapshotExporter::
  //                        getOutgoingCopyProgress(snapshotId,
  //                                                   current,
  //                                                   estimated);
}
