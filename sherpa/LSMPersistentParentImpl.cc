#include "PersistentParent.h"
#include "LSMPersistentStoreImpl.h"
#include "TabletMetadata.h"
#include "tcpclient.h"

// XXX for getpid...
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

class LSMPersistentParent : PersistentParent {
public:

  LSMPersistentParent() : ordered(true), hashed(false) {
  } // we ignore the ordered flag...
  SuCode::ResponseCode install(const SectionConfig& config) {
    ordered.filestr << "LSM install called" << std::endl;
    return SuCode::SuOk;
  }
  SuCode::ResponseCode init(const SectionConfig& config) {
    ordered.filestr << "LSM init called" << std::endl;
    ordered.init(config);
    hashed.init(config);
    return SuCode::SuOk;
  }
  PersistentStore *getHashedStore() {
    ordered.filestr << "LSM getHashedStore called" << std::endl;
    return &hashed;
  }
  PersistentStore *getOrderedStore() {
    ordered.filestr << "LSM getOrderedStore called" << std::endl;
    return &ordered;
  }
  bool ping() {
    ordered.filestr << "LSM ping called" << std::endl;
    return true;
  } // XXX call OP_DBG_NOOP
  std::string getName() {
    ordered.filestr << "LSM getName called" << std::endl;
    return "logstore";
  }
  SuCode::ResponseCode getFreeSpaceBytes(double & freeSpaceBytes) {
    ordered.filestr << "LSM getFreeSpaceBytes called" << std::endl;
    freeSpaceBytes = 1024.0 *1024.0 * 1024.0; // XXX stub
    return SuCode::SuOk;
  }
  SuCode::ResponseCode getDiskMaxBytes(double & diskMaxBytes) {
    ordered.filestr << "LSM getDiskMaxBytes called" << std::endl;
    diskMaxBytes = 10.0 * 1024.0 *1024.0 * 1024.0; // XXX stub
    return SuCode::SuOk;
  }
  SuCode::ResponseCode cleanupTablet(uint64_t uniqId,
                                     const std::string & tableName,
                                     const std::string & tabletName) {
    ordered.filestr << "LSM cleanupTablet called" << std::endl;
    return SuCode::SuOk; // XXX stub
  }
  SuCode::ResponseCode getTabletMappingList(TabletList & tabletList) {
    ordered.filestr << "LSM getTabletMappingList called" << std::endl;

    std::string metadata_table     = std::string("ydht_metadata_table");
    std::string metadata_tablet    = std::string("0");
    std::string metadata_tabletEnd = std::string("1");

    size_t startlen;
    size_t endlen;

    ordered.filestr << "getTabletMappingList C" << std::endl;

    unsigned char * start_tup = ordered.my_strcat(metadata_table, metadata_tablet, "", &startlen);
    unsigned char * end_tup =   ordered.my_strcat(metadata_table, metadata_tabletEnd, "", &endlen);

    ordered.filestr << "start tup = " << start_tup << std::endl;
    ordered.filestr << "end tup = " << end_tup << std::endl;

    datatuple * starttup = datatuple::create(start_tup, startlen);
    datatuple * endtup = datatuple::create(end_tup, endlen);

    free(start_tup);
    free(end_tup);
    ordered.filestr << "getTabletMappingList B conn = " << ordered.l_ << std::endl;
    pid_t pid = getpid();

    uint8_t rcode = logstore_client_op_returns_many(ordered.l_, OP_SCAN, starttup, endtup, 0); // 0 = no limit.
    ordered.filestr << "getTabletMappingList A'" << std::endl;

    datatuple::freetuple(starttup);
    datatuple::freetuple(endtup);

    datatuple * next;
    ordered.filestr << "getTabletMappingList A" << std::endl;
    ordered.filestr.flush();

    TabletMetadata m;
    if(rcode == LOGSTORE_RESPONSE_SENDING_TUPLES) {
      while((next = logstore_client_next_tuple(ordered.l_))) {
        ordered.metadata_buf(m, next->key(), next->keylen());

        struct ydht_maptable_schema md;
        md.uniq_id = 0;
        md.tableName = m.table();
        md.tabletName = m.tablet();
        ordered.filestr << md.tableName << " : " << md.tabletName << std::endl;
        tabletList.push_back(md);
        datatuple::freetuple(next);
      }
      ordered.filestr << "getTabletMappingListreturns" << std::endl;
    } else {
      ordered.filestr << "error " << (int)rcode << " in getTabletMappingList." << std::endl;
      return SuCode::PStoreUnexpectedError; // XXX should be "connection closed error" or something...
    }

    return SuCode::SuOk;
  }
  SuCode::ResponseCode getApproximateTableSize
        (const std::string& tableId,
         int64_t& tableSize, int64_t & rowCount) {
    ordered.filestr << "LSM getApproximateTableSize called" << std::endl;
    return ordered.getApproximateTableSize(tableId, tableSize, rowCount);
  }
private:
  LSMPersistentStoreImpl ordered;
  LSMPersistentStoreImpl hashed;
};

extern "C" {
  void * lsmsherpa_init();
}

static LSMPersistentParent pp;
void * lsmsherpa_init() {
  return &pp;
}
