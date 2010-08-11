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

int dispatch_request(network_op_t opcode, datatuple * tuple, datatuple * tuple2, logtable<datatuple> * logstore, int fd);

#endif /* REQUESTDISPATCH_H_ */
