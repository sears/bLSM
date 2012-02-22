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
  static inline int op_insert(blsm * ltable, HANDLE fd, datatuple * tuple);
  static inline int op_test_and_set(blsm * ltable, HANDLE fd, datatuple * tuple, datatuple * tuple2);
  static inline int op_find(blsm * ltable, HANDLE fd, datatuple * tuple);
  static inline int op_scan(blsm * ltable, HANDLE fd, datatuple * tuple, datatuple * tuple2, size_t limit);
  static inline int op_bulk_insert(blsm * ltable, HANDLE fd);
  static inline int op_flush(blsm * ltable, HANDLE fd);
  static inline int op_shutdown(blsm * ltable, HANDLE fd);
  static inline int op_stat_space_usage(blsm * ltable, HANDLE fd);
  static inline int op_stat_perf_report(blsm * ltable, HANDLE fd);
  static inline int op_stat_histogram(blsm * ltable, HANDLE fd, size_t limit);
  static inline int op_dbg_blockmap(blsm * ltable, HANDLE fd);
  static inline int op_dbg_drop_database(blsm * ltable, HANDLE fd);
  static inline int op_dbg_noop(blsm * ltable, HANDLE fd);
  static inline int op_dbg_set_log_mode(blsm * ltable, HANDLE fd, datatuple * tuple);

public:
  static int dispatch_request(HANDLE f, blsm * ltable);
  static int dispatch_request(network_op_t opcode, datatuple * tuple, datatuple * tuple2, blsm * ltable, HANDLE fd);
};
#endif /* REQUESTDISPATCH_H_ */
