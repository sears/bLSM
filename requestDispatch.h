/*
 * requestDispatch.h
 *
 *  Created on: Aug 11, 2010
 *      Author: sears
 */

#ifndef REQUESTDISPATCH_H_
#define REQUESTDISPATCH_H_
#include "network.h"
#include "datatuple.h"
#include "logstore.h"
template<class HANDLE>
class requestDispatch {
private:
  static inline int op_insert(logtable<datatuple> * ltable, HANDLE fd, datatuple * tuple);
  static inline int op_find(logtable<datatuple> * ltable, HANDLE fd, datatuple * tuple);
  static inline int op_scan(logtable<datatuple> * ltable, HANDLE fd, datatuple * tuple, datatuple * tuple2, size_t limit);
  static inline int op_bulk_insert(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_flush(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_shutdown(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_stat_space_usage(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_stat_perf_report(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_stat_histogram(logtable<datatuple> * ltable, HANDLE fd, size_t limit);
  static inline int op_dbg_blockmap(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_dbg_drop_database(logtable<datatuple> * ltable, HANDLE fd);
  static inline int op_dbg_noop(logtable<datatuple> * ltable, HANDLE fd);

public:
  static int dispatch_request(HANDLE f, logtable<datatuple> * ltable);
  static int dispatch_request(network_op_t opcode, datatuple * tuple, datatuple * tuple2, logtable<datatuple> * ltable, HANDLE fd);
};
#endif /* REQUESTDISPATCH_H_ */
