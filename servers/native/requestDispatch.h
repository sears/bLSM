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
#include "blsm.h"
template<class HANDLE>
class requestDispatch {
private:
  static inline int op_insert(bLSM * ltable, HANDLE fd, dataTuple * tuple);
  static inline int op_test_and_set(bLSM * ltable, HANDLE fd, dataTuple * tuple, dataTuple * tuple2);
  static inline int op_find(bLSM * ltable, HANDLE fd, dataTuple * tuple);
  static inline int op_scan(bLSM * ltable, HANDLE fd, dataTuple * tuple, dataTuple * tuple2, size_t limit);
  static inline int op_bulk_insert(bLSM * ltable, HANDLE fd);
  static inline int op_flush(bLSM * ltable, HANDLE fd);
  static inline int op_shutdown(bLSM * ltable, HANDLE fd);
  static inline int op_stat_space_usage(bLSM * ltable, HANDLE fd);
  static inline int op_stat_perf_report(bLSM * ltable, HANDLE fd);
  static inline int op_stat_histogram(bLSM * ltable, HANDLE fd, size_t limit);
  static inline int op_dbg_blockmap(bLSM * ltable, HANDLE fd);
  static inline int op_dbg_drop_database(bLSM * ltable, HANDLE fd);
  static inline int op_dbg_noop(bLSM * ltable, HANDLE fd);
  static inline int op_dbg_set_log_mode(bLSM * ltable, HANDLE fd, dataTuple * tuple);

public:
  static int dispatch_request(HANDLE f, bLSM * ltable);
  static int dispatch_request(network_op_t opcode, dataTuple * tuple, dataTuple * tuple2, bLSM * ltable, HANDLE fd);
};
#endif /* REQUESTDISPATCH_H_ */
