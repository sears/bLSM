/**
 *  \file LSMPersistentStoreImpl.cc
 *
 *  Copyright (c) 2008 Yahoo, Inc.
 *  All rights reserved.
 */
#include "TabletMetadata.h"
#include "TabletIterator.h"
#include "ScanContinuation.h"
#include "OrderedScanContinuation.h"
#include "TabletRange.h"
#include "SuLimits.h"
#include "dht/UtilityBuffer.h"

#include <dht/LogUtils.h>

#include "LSMPersistentStoreImpl.h"

#include <tcpclient.h>


// Initialize the logger
static log4cpp::Category &log =
       log4cpp::Category::getInstance("dht.su."__FILE__);

class LSMIterator : public TabletIterator<StorageRecord> {
  friend class LSMPersistentStoreImpl;

public:
//  StorageRecord * data; //  <- defined in parent class.  next() updates it.

  LSMIterator(LSMPersistentStoreImpl* lsmImpl, const TabletMetadata& tabletMeta, ScanContinuationAutoPtr continuation,
      ScanSelect::Selector ignored, const uint64_t expiryTime,
      unsigned int scanLimit, size_t byteLimit /*ignored*/) : lsmImpl(lsmImpl) {
    const unsigned char low_eos  = (unsigned char) 0xFE;
    const unsigned char high_eos = (unsigned char) 0xFF;
    const unsigned char zero     = (unsigned char) 0x00;
    (void)zero;
    // open iterator
    datatuple * starttup = NULL;
    datatuple * endtup = NULL;

    size_t start_key_len, end_key_len;
    unsigned char *start_key, *end_key;

    if(continuation->isOrdered()) {
      const OrderedScanContinuation& os = static_cast<const OrderedScanContinuation&>(*continuation);

      if(!os.getStartKey().isMinKey()) {
        start_key = lsmImpl->buf_key(tabletMeta, os.getStartKey().getKey(), &start_key_len);
      } else {
        start_key = lsmImpl->buf_key(tabletMeta, "", &start_key_len);
      }

      if(!os.getEndKey().isMaxKey()) {
        end_key = lsmImpl->buf_key(tabletMeta, os.getEndKey().getKey(), &end_key_len);
      } else {
        end_key = lsmImpl->buf_key(tabletMeta, "", &end_key_len);
        if(end_key[end_key_len-2] != low_eos) {
          DHT_ERROR_STREAM() << "CORRUPT lsm tablet key = " << (char*)end_key;
        } else {
          end_key[end_key_len-2] = high_eos;
        }
      }
    } else {
      DHT_WARN_STREAM() << "Scanning hash table, but ignoring contiunation range!";
      start_key = lsmImpl->buf_key(tabletMeta, "", &start_key_len);
      end_key = lsmImpl->buf_key(tabletMeta, "", &end_key_len);
      if(end_key[end_key_len-2] != low_eos) {
        DHT_ERROR_STREAM() << "CORRUPT lsm tablet key = " << (char*)end_key;
      } else {
        end_key[end_key_len-2] = high_eos;
      }
    }

    starttup = datatuple::create(start_key, start_key_len);
    std::string dbg((char*)start_key, start_key_len - 1);
    DHT_DEBUG_STREAM() << "start lsm key = " << dbg;

    endtup = datatuple::create(end_key, end_key_len);
    std::string dbg2((char*)end_key, end_key_len - 1);
    DHT_DEBUG_STREAM() << "end lsm key = " << dbg2;

    uint8_t rc = logstore_client_op_returns_many(lsmImpl->scan_l_, OP_SCAN, starttup, endtup, scanLimit);

    datatuple::freetuple(starttup);
    datatuple::freetuple(endtup);

    if(rc != LOGSTORE_RESPONSE_SENDING_TUPLES) {
      this->error = rc;
    } else {
      this->error = 0;
    }

    this->data = new StorageRecord();
  }
  ~LSMIterator() {
    DHT_DEBUG_STREAM() << "close iterator called";
    // close iterator by running to the end of it...  TODO devise a better way to close iterators early?
    while(this->data) {
      next();
    }
    DHT_DEBUG_STREAM() << "close iterator done";
  }
  SuCode::ResponseCode next() {
    datatuple * tup;
    DHT_DEBUG_STREAM() <<  "next called";
    if(error) { // only catches errors during scan setup.
      return SuCode::PStoreUnexpectedError;
    } else if((tup = logstore_client_next_tuple(lsmImpl->scan_l_))) {
      DHT_DEBUG_STREAM() << "found tuple, key = " << tup->key() << " datalen = " << tup->datalen();
      SuCode::ResponseCode rc = lsmImpl->tup_buf(*(this->data), tup);
      datatuple::freetuple(tup);
      return rc;
    } else {
      DHT_DEBUG_STREAM() << "no tuple";
      delete this->data;
      this->data = NULL;
      return SuCode::PStoreScanEnd; // XXX need to differentiate between end of scan and failure
    }
  }
private:
  LSMPersistentStoreImpl * lsmImpl;
  uint8_t error;
};

void LSMPersistentStoreImpl::buf_metadata(unsigned char ** buf, size_t *len, const TabletMetadata &m) {
  std::string ydht_metadata_table = std::string("ydht_metadata_table");
  std::string zero = std::string("0");
  std::string tmp; unsigned char * tmp_p; size_t tmp_len;
  tmp_p = my_strcat(m.table(), m.tablet(), "", &tmp_len);
  tmp.assign((const char*)tmp_p, tmp_len);
  free(tmp_p);
  *buf = my_strcat(ydht_metadata_table, zero, tmp, len);
}

void LSMPersistentStoreImpl::metadata_buf(TabletMetadata &m, const unsigned char * buf, size_t len) {
  // Metadata table key format:
  // ydht_metadata_table[low_eos]0[low_eos]table[low_eos]tablet[low_eos][low_eos]

  assert(buf);
  std::string ydht_metadata_table, zero, tmp, table, tablet, empty;
  my_strtok(buf, len, ydht_metadata_table, zero, tmp);
  assert(tmp.c_str());
  my_strtok((const unsigned char*)tmp.c_str(), tmp.length(), table, tablet, empty);

  DHT_DEBUG_STREAM() << "Parsed metadata: [" << table << "] [" << tablet << "] [" << empty << "](empty)";
  m.setTable(table);
  m.setTablet(tablet);
  m.setTabletId(tmp.substr(0, tmp.length() - 1));
}

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
void LSMPersistentStoreImpl::my_strtok(const unsigned char* in, size_t len, std::string& table, std::string& tablet, std::string& key) {
  const char * tablep = (const char*) in;

  const unsigned char low_eos  = (unsigned char) 0xFE;
  const unsigned char high_eos = (unsigned char) 0xFF;
  const unsigned char zero     = (unsigned char) 0x00;
  (void)high_eos; (void)zero;

  const char * tabletp = ((const char*)memchr(tablep, low_eos, len)) + 1;
  int tablep_len = (tabletp - tablep)-1; // -1 is due to low_eos terminator
  const char * keyp = ((const char*)memchr(tabletp, low_eos, len-tablep_len)) + 1;
  int tabletp_len = (keyp - tabletp) - 1; // -1 is due to low_eos terminator
  int keyp_len = (len - (tablep_len + 1 + tabletp_len + 1)) - 1;  // -1 is due to null terminator.

  table.assign(tablep, tablep_len);
  tablet.assign(tabletp, tabletp_len);
  key.assign(keyp, keyp_len);
}
unsigned char *
LSMPersistentStoreImpl::buf_key(const TabletMetadata&m,
			const RecordKey& k, size_t * len) {
  return buf_key(m,k.name(),len);
}
  unsigned char *
  LSMPersistentStoreImpl::buf_key(const TabletMetadata&m,
              const std::string s, size_t * len) {
  const unsigned char low_eos  = (unsigned char) 0xFE;
  const unsigned char high_eos = (unsigned char) 0xFF;
  const unsigned char zero     = (unsigned char) 0x00;
  (void)high_eos; (void)low_eos;
  std::string md_name = m.getTabletId();
  *len = md_name.length() /*+ 1*/ + s.length() + 1; // md_name ends in a low_eos...
  unsigned char * ret = (unsigned char*)malloc(*len);
  unsigned char * buf = ret;
  memcpy(buf, md_name.c_str(), md_name.length()); buf += md_name.length();
  //memcpy(buf, &low_eos, 1); buf++;
  memcpy(buf, s.c_str(), s.length()); buf += s.length();
  memcpy(buf, &zero, 1);
  return ret;
}

unsigned char *
LSMPersistentStoreImpl::buf_val(const StorageRecord& val, size_t * len) {
  uint64_t expiryTime = val.expiryTime();
  uint32_t dataBlob_len, metadata_len; // Below, we assume these are of the same type.

  const UtilityBuffer& dataBlob = val.dataBlob();
  const UtilityBuffer& metadata = val.metadata();

  dataBlob_len = dataBlob.dataSize();
  metadata_len = metadata.dataSize();

//  DHT_DEBUG_STREAM() << "write storage record expiryTime " << expiryTime << " metadata len " << metadata_len << " datalen " << dataBlob_len;

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
LSMPersistentStoreImpl::tup_buf(StorageRecord& ret, datatuple * tup) {
  SuCode::ResponseCode rc = SuCode::SuOk;
  if(tup->key()) {
    rc = key_buf(ret, tup->key(), tup->keylen());
  }
  if(rc == SuCode::SuOk && !tup->isDelete() && tup->datalen() > 1) {
    return val_buf(ret, tup->data(), tup->datalen());
  } else {
    return rc;
  }
}
SuCode::ResponseCode
LSMPersistentStoreImpl::key_buf(StorageRecord& ret,
                const unsigned char * buf, size_t buf_len) {
  std::string table, tablet, key;
  my_strtok(buf, buf_len, table, tablet, key);
  DHT_DEBUG_STREAM() << "key_buf parsed datatuple key: table = [" << table <<  "] tablet = [" << tablet <<  "] key = [" << key << "]";
  ret.recordKey().setName(key);
  return SuCode::SuOk;
}

SuCode::ResponseCode
LSMPersistentStoreImpl::val_buf(StorageRecord& ret,
				const unsigned char * buf, size_t buf_len) {
  uint64_t expiryTime;
  uint32_t dataBlob_len, metadata_len;
//  DHT_DEBUG_STREAM() << "read storage record buf_len " << buf_len << std::endl;
  assert(buf_len >= sizeof(expiryTime) + sizeof(dataBlob_len) + sizeof(metadata_len));

  // Copy header onto stack.

  memcpy(&expiryTime,   buf, sizeof(expiryTime));    buf += sizeof(expiryTime);
  memcpy(&metadata_len, buf, sizeof(metadata_len));  buf += sizeof(metadata_len);
  memcpy(&dataBlob_len, buf, sizeof(dataBlob_len));  buf += sizeof(dataBlob_len);

  // Is there room in ret?

  assert(buf_len >= sizeof(expiryTime) + sizeof(dataBlob_len) + sizeof(metadata_len) + metadata_len + dataBlob_len);

  if(ret.metadata().bufSize() < metadata_len ||
     ret.dataBlob().bufSize() < dataBlob_len) {
    return SuCode::PStoreDataTruncated; // RCS: This is what the mysql implementation does.
                                        // it's somewhat misleading, as we don't truncate anything.
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
LSMPersistentStoreImpl(bool isOrdered) : isOrdered_(isOrdered), l_(NULL), scan_l_(NULL) {
  //  filestr.open(isOrdered? "/tmp/lsm-log" : "/tmp/lsm-log-hashed", std::fstream::out | std::fstream::app);
  // It would be unsafe to call the following, since we're statically initialized:  DHT_DEBUG_STREAM() << "LSMP constructor called";
}

LSMPersistentStoreImpl::
~LSMPersistentStoreImpl()
{
  DHT_DEBUG_STREAM() << "LSMP destructor called";
  if(l_)      logstore_client_close(l_);
  if(scan_l_) logstore_client_close(scan_l_);
  DHT_DEBUG_STREAM() << "LSMP destructor cleanly closed connections";
}

SuCode::ResponseCode LSMPersistentStoreImpl::initMetadataMetadata(TabletMetadata& m) {
  DHT_DEBUG_STREAM() << "LSMP initMetadataMetadata called";

  std::string metadata_table = std::string("ydht_metadata_table");
  std::string metadata_tablet= std::string("0");
  size_t keylen;
  char * key =(char*)my_strcat(metadata_table, metadata_tablet, "", &keylen);
  std::string s(key, keylen-1);
  m.setTabletId(s);
  free(key);
  return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
init(const SectionConfig &config)
{
  DHT_DEBUG_STREAM() << "LSMP init called";
  if(!l_) { // workaround bug 2870547
    l_ = logstore_client_open("localhost", 32432, 60);  // XXX hardcode none of these values
    scan_l_ = logstore_client_open("localhost", 32432, 60);  // XXX hardcode none of these values
  }
  return l_ ? SuCode::SuOk : FwCode::NotFound;
}

bool LSMPersistentStoreImpl::
isOrdered(){
  DHT_DEBUG_STREAM() << "LSMP isOrdered called";
  return isOrdered_;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
addEmptyTablet(TabletMetadata& tabletMeta)
{
  DHT_DEBUG_STREAM() << "LSMP addEmptyTablet called";
  // This is a no-op; we'll simply prepend the tablet string to each record.
  {
    // Is table name too long?
    if(tabletMeta.table().length() > SuLimits::MAX_TABLE_NAME_LENGTH_DB) {
      return SuCode::PStoreIOFailed;
    }

    const std::string& mySQLTableName = tabletMeta.getTabletId();


    if (mySQLTableName!=""){
      DHT_INFO_STREAM() << "Tablet " << mySQLTableName << " already exists!";
      return SuCode::PStoreTabletAlreadyExists;
    } else {
      size_t keylen; unsigned char * key;
      buf_metadata(&key, &keylen, tabletMeta);
      metadata_buf(tabletMeta, key, keylen);
      free(key);
      return SuCode::SuOk;
    }
  }
}

SuCode::ResponseCode LSMPersistentStoreImpl::
dropTablet(TabletMetadata& tabletMeta)
{
  DHT_INFO_STREAM() << "dropTablet called.  Falling back on clearTabletRange()";
  SuCode::ResponseCode ret =  clearTabletRange(tabletMeta, 0);

  size_t keylen;
  unsigned char * key;
  buf_metadata(&key, &keylen, tabletMeta);

  datatuple * tup = datatuple::create(key, keylen); // two-argument form of datatuple::create creates a tombstone, which we will now insert.
  free(key);
  void * result = (void*)logstore_client_op(l_, OP_INSERT, tup);
  datatuple::freetuple(tup);

  tabletMeta.setTabletId("");

  if(!result) {
    DHT_WARN_STREAM() << "LSMP dropTablet fails";
    ret = SuCode::PStoreTabletCleanupFailed;
  } else {
    DHT_INFO_STREAM() << "LSMP dropTablet succeeds";
    ret = SuCode::SuOk;
  }
  return ret;
}

// alternate to dropTablet() for when a tablet has been split and the underlying
// mysql table is being shared...just wipe out the record range for the tablet
// that is being dropped.
SuCode::ResponseCode LSMPersistentStoreImpl::
clearTabletRange(TabletMetadata& tabletMeta, uint32_t removalLimit)
{
  DHT_WARN_STREAM() << "clear tablet range is unimplemented.  ignoring request";
  return SuCode::SuOk;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
getApproximateTableSize(std::string tabletMeta,
			int64_t& tableSize,
			int64_t & rowCount) {
  DHT_WARN_STREAM() << "get approximate table size is unimplemented.  returning dummy values";
  tableSize = 1024 * 1024 * 1024;
  rowCount = 1024 * 1024;
  return SuCode::SuOk;
}
SuCode::ResponseCode LSMPersistentStoreImpl::
getApproximateTableSize(TabletMetadata& tabletMeta,
			int64_t& tableSize,
			int64_t & rowCount)
{
  DHT_DEBUG_STREAM() << "LSMP getApproximateTableSize (2) called";
  return getApproximateTableSize(tabletMeta.getTabletId(), tableSize, rowCount);
}


SuCode::ResponseCode LSMPersistentStoreImpl::
get(const TabletMetadata& tabletMeta, StorageRecord& recordData)
{
  DHT_DEBUG_STREAM() << "LSMP get called" << tabletMeta.getTabletId()  << ":" << recordData.recordKey();
  if(recordData.recordKey().name().length() > (isOrdered_ ? SuLimits::MAX_ORDERED_RECORD_NAME_LENGTH : SuLimits::MAX_RECORD_NAME_LENGTH)) {
    return SuCode::PStoreIOFailed;
  }
  size_t buflen;
  unsigned char * buf = buf_key(tabletMeta, recordData.recordKey(), &buflen);
  datatuple * key_tup = datatuple::create(buf, buflen);
  free(buf);
  datatuple * result = logstore_client_op(l_, OP_FIND, key_tup);
  datatuple::freetuple(key_tup);
  SuCode::ResponseCode ret;
  if((!result) || result->isDelete()) {
    ret = SuCode::PStoreRecordNotFound;
  } else {
    //DHT_DEBUG_STREAM() << "call val buf from get, data len = " << result->datalen() << std::endl;
    ret = val_buf(recordData, result->data(), result->datalen());
  }
  if(result) {
    datatuple::freetuple(result);
  }
  DHT_DEBUG_STREAM() << "LSMP get returns succ = " << (ret == SuCode::SuOk);
  return ret;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
update(const TabletMetadata& tabletMeta, const StorageRecord& updateData)
{
  DHT_DEBUG_STREAM() << "LSMP update called";
  if(updateData.recordKey().name().length() > (isOrdered_ ? SuLimits::MAX_ORDERED_RECORD_NAME_LENGTH : SuLimits::MAX_RECORD_NAME_LENGTH)) {
    return SuCode::PStoreIOFailed;
  }

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
  DHT_DEBUG_STREAM()<< "LSMP insert called" << tabletMeta.getTabletId()  << ":" << insertData.recordKey();
  size_t keybuflen, valbuflen;
  if(insertData.recordKey().name().length() > (isOrdered_ ? SuLimits::MAX_ORDERED_RECORD_NAME_LENGTH : SuLimits::MAX_RECORD_NAME_LENGTH)) {
    return SuCode::PStoreIOFailed;
  }
  unsigned char * keybuf = buf_key(tabletMeta, insertData.recordKey(), &keybuflen);
  DHT_DEBUG_STREAM() << "keybuf = " << keybuf << " (and perhaps a null)";
  unsigned char * valbuf = buf_val(insertData, &valbuflen);
  DHT_DEBUG_STREAM() << "valbuf = " << valbuf << " (and perhaps a null)";
  datatuple * key_ins = datatuple::create(keybuf, keybuflen, valbuf, valbuflen);
  DHT_DEBUG_STREAM() << "insert create()";
  void * result = (void*)logstore_client_op(l_, OP_INSERT, key_ins);
  DHT_DEBUG_STREAM() << "insert insert()";
  if(result) {
    DHT_DEBUG_STREAM() << "LSMP insert will return result = " << result;
  } else {
    DHT_DEBUG_STREAM() << "LSMP insert will return null ";
  }
  datatuple::freetuple(key_ins);
  free(keybuf);
  free(valbuf);
  DHT_DEBUG_STREAM() << "LSMP insert returns ";
  return result ? SuCode::SuOk : SuCode::PStoreUnexpectedError;
}

SuCode::ResponseCode LSMPersistentStoreImpl::
remove(const TabletMetadata& tabletMeta, const RecordKey& recordName)
{
  DHT_DEBUG_STREAM() << "LSMP remove called";
  if(recordName.name().length() > (isOrdered_ ? SuLimits::MAX_ORDERED_RECORD_NAME_LENGTH : SuLimits::MAX_RECORD_NAME_LENGTH)) {
    return SuCode::PStoreIOFailed;
  }

  StorageRecord tmp(recordName.name());
  SuCode::ResponseCode rc = get(tabletMeta, tmp);
  if(SuCode::SuOk == rc) {
    size_t buflen;
    unsigned char * buf = buf_key(tabletMeta, recordName, &buflen);
    datatuple * key_ins = datatuple::create(buf, buflen);
    datatuple * result = logstore_client_op(l_, OP_INSERT, key_ins);
    datatuple::freetuple(key_ins);
    free(buf);
    return result ? SuCode::SuOk : SuCode::PStoreUnexpectedError;
  } else {
    DHT_DEBUG_STREAM() << "LSMP remove: record not found, or error";
    return rc;
  }
}

bool LSMPersistentStoreImpl::ping() {
  DHT_DEBUG_STREAM() << "LSMP ping called";
  datatuple * ret = logstore_client_op(l_, OP_DBG_NOOP);
  if(ret == NULL) {
    DHT_WARN_STREAM() << "LSMP ping failed";
    return false;
  } else {
    datatuple::freetuple(ret);
    return true;
  }
}

StorageRecordIterator LSMPersistentStoreImpl::
scan(const TabletMetadata& tabletMeta, const ScanContinuation& continuation,
     ScanSelect::Selector selector, const uint64_t expiryTime, unsigned int scanLimit, size_t byteLimit)
{
  DHT_DEBUG_STREAM() << "LSMP scan called.  Tablet: " << tabletMeta.getTabletId();
  ScanContinuationAutoPtr newContinuation;
  TabletRangeAutoPtr tabletRange;

  if (SuCode::SuOk != tabletMeta.keyRange(tabletRange)){
    BAD_CODE_ABORT("Bad tablet name");
  }

    /* This is necessary once we turn on splits, because multiple tablets
     * might reside in the same mysql table. Shouldnt scan beyond the
     * upper limit of the tablet because that might be stale data left
     * from before this tablet split.
     */
  newContinuation = continuation.getContinuationLimitedToTabletRange(
				       *tabletRange);

  LSMIterator* iter = new LSMIterator(this, tabletMeta, newContinuation,
					    selector, /*getMetadataOnly,*/
					    expiryTime, scanLimit, byteLimit);

  DHT_DEBUG_STREAM() << "LSMP scan returns.  Error = " << iter->error;
  return StorageRecordIterator(iter);

}

SuCode::ResponseCode LSMPersistentStoreImpl::
getSnapshotExporter(const TabletMetadata& tabletMeta,
		    const std::string& snapshotId,
	       SnapshotExporterAutoPtr& exporter)
{
  DHT_WARN_STREAM() << "Unimplemented: LSMP getSnapshotExported called";
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
  DHT_WARN_STREAM() << "Unimplemented: getSnapshotImporter called";
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
  DHT_DEBUG_STREAM() << "Unimplemented: LSMP getIncomingCopyProgress called";

  //This will be a problem when we have more than 1
    //exporter/importer type. We will have to store the
    //snapshot version somewhere in tablet metadata
  current = 1024*1024*1024;
  estimated = 1024*1024*1024;

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
  DHT_DEBUG_STREAM() << "Unimplemented: LSMP getOutgoingCopyProgress called";
  current = 1024*1024*1024;
  estimated = 1024*1024*1024;
  return SuCode::SuOk;
  //    return LSMSnapshotExporter::
  //                        getOutgoingCopyProgress(snapshotId,
  //                                                   current,
  //                                                   estimated);
}
