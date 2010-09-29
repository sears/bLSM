#include "PersistentParent.h"
#include "LSMPersistentStoreImpl.h"
#include "TabletMetadata.h"
#include "tcpclient.h"

#include <dht/LogUtils.h>

// Initialize the logger
static log4cpp::Category &log =
       log4cpp::Category::getInstance("dht.su."__FILE__);

class LSMPersistentParent : PersistentParent {
public:

  LSMPersistentParent() : ordered(true), hashed(false) {
  } // we ignore the ordered flag...
  ~LSMPersistentParent() {
    DHT_DEBUG_STREAM() << "~LSMPersistentParent called";
  }
  SuCode::ResponseCode install(const SectionConfig& config) {
    DHT_DEBUG_STREAM() << "LSM install called";
    return SuCode::SuOk;
  }
  SuCode::ResponseCode init(const SectionConfig& config) {
    DHT_DEBUG_STREAM() << "LSM init called";
    ordered.init(config);
    hashed.init(config);
    return SuCode::SuOk;
  }
  PersistentStore *getHashedStore() {
    DHT_DEBUG_STREAM()  << "LSM getHashedStore called";
    return &hashed;
  }
  PersistentStore *getOrderedStore() {
    DHT_DEBUG_STREAM() << "LSM getOrderedStore called";
    return &ordered;
  }
  bool ping() {
    DHT_DEBUG_STREAM() << "LSM ping called";
    return ordered.ping();
  }
  std::string getName() {
    DHT_DEBUG_STREAM() << "LSM getName called";
    return "logstore";
  }
  SuCode::ResponseCode getFreeSpaceBytes(double & freeSpaceBytes) {
    DHT_DEBUG_STREAM() << "LSM getFreeSpaceBytes called";
    freeSpaceBytes = 1024.0 *1024.0 * 1024.0; // XXX stub
    return SuCode::SuOk;
  }
  SuCode::ResponseCode getDiskMaxBytes(double & diskMaxBytes) {
    DHT_DEBUG_STREAM() << "LSM getDiskMaxBytes called";
    diskMaxBytes = 10.0 * 1024.0 *1024.0 * 1024.0; // XXX stub
    return SuCode::SuOk;
  }
  SuCode::ResponseCode cleanupTablet(uint64_t uniqId,
                                     const std::string & tableName,
                                     const std::string & tabletName) {
    DHT_DEBUG_STREAM() << "LSM cleanupTablet called";
    return SuCode::SuOk; // XXX stub
  }
  SuCode::ResponseCode getTabletMappingList(TabletList & tabletList) {
    DHT_DEBUG_STREAM() << "LSM getTabletMappingList called";

    std::string metadata_table     = std::string("ydht_metadata_table");
    std::string metadata_tablet    = std::string("0");
    std::string metadata_tabletEnd = std::string("1");

    size_t startlen;
    size_t endlen;

    unsigned char * start_tup = ordered.my_strcat(metadata_table, metadata_tablet, "", &startlen);
    unsigned char * end_tup =   ordered.my_strcat(metadata_table, metadata_tabletEnd, "", &endlen);

    DHT_DEBUG_STREAM() << "start tup = " << start_tup;
    DHT_DEBUG_STREAM() << "end tup = " << end_tup;

    datatuple * starttup = datatuple::create(start_tup, startlen);
    datatuple * endtup = datatuple::create(end_tup, endlen);

    free(start_tup);
    free(end_tup);

    uint8_t rcode = logstore_client_op_returns_many(ordered.l_, OP_SCAN, starttup, endtup, 0); // 0 = no limit.

    datatuple::freetuple(starttup);
    datatuple::freetuple(endtup);

    datatuple * next;

    TabletMetadata m;
    if(rcode == LOGSTORE_RESPONSE_SENDING_TUPLES) {
      while((next = logstore_client_next_tuple(ordered.l_))) {
        ordered.metadata_buf(m, next->key(), next->keylen());

        struct ydht_maptable_schema md;
        std::string cat = m.table() + m.tablet();
        md.uniq_id = 0;
        for(int i = 0; i < cat.length(); i++) {
          md.uniq_id += ((unsigned char)cat[i]); // XXX obviously, this is a terrible hack (and a poor hash function)
        }
        md.tableName = m.table();
        md.tabletName = m.tablet();
        DHT_DEBUG_STREAM() << md.uniq_id << " : " << md.tableName << " : " << md.tabletName;
        tabletList.push_back(md);
        datatuple::freetuple(next);
      }
      DHT_DEBUG_STREAM() << "getTabletMappingListreturns";
    } else {
      DHT_ERROR_STREAM() << "error " << (int)rcode << " in getTabletMappingList.";
      return SuCode::PStoreUnexpectedError; // XXX should be "connection closed error" or something...
    }

    return SuCode::SuOk;
  }
  SuCode::ResponseCode getApproximateTableSize
        (const std::string& tableId,
         int64_t& tableSize, int64_t & rowCount) {
    DHT_DEBUG_STREAM() << "LSM getApproximateTableSize called";
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
