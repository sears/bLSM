/*
 * copy_database.cpp
 *
 *  Created on: Aug 30, 2010
 *      Author: sears
 */

#include "../test/check_util.h"
#include <sys/time.h>

#include "../tcpclient.h"
#include "../network.h"
#include "../datatuple.h"
void usage(char * argv[]) {
    fprintf(stderr, "usage %s from_host to_host\n", argv[0]);
}
#include "util_main.h"
int main(int argc, char * argv[]) {
  if(argc != 3) { usage(argv); return -1; }

  char ** from_arg = (char**)malloc(2*sizeof(char*));
  from_arg[0] = argv[0];
  from_arg[1] = argv[1];

  char ** to_arg = (char**)malloc(2*sizeof(char*));
  to_arg[0] = argv[0];
  to_arg[1] = argv[2];

  logstore_handle_t * from = util_open_conn(2, from_arg);
  logstore_handle_t * to   = util_open_conn(2, to_arg);

  uint8_t ret = logstore_client_op_returns_many(from, OP_SCAN, 0, 0, -2);
  if(ret != LOGSTORE_RESPONSE_SENDING_TUPLES) {
    perror("Open database scan failed"); return 3;
  }
  ret = logstore_client_op_returns_many(to, OP_BULK_INSERT);
  if(ret != LOGSTORE_RESPONSE_RECEIVING_TUPLES) {
    perror("Open bulk insert failed"); return 3;
  }
  long long num_tuples = 0;
  long long size_copied = 0;
  datatuple *tup;

  int bytes_per_dot = 10 * 1024 * 1024;
  int dots_per_line = 50;
  long long last_dot = 0;

  struct timeval load_start_time;
  gettimeofday(&load_start_time, 0);

  while((tup = logstore_client_next_tuple(from))) {
    ret = logstore_client_send_tuple(to, tup);
    num_tuples ++;
    size_copied += tup->byte_length();
    datatuple::freetuple(tup);
    if(ret != LOGSTORE_RESPONSE_SUCCESS) {
      perror("Send tuple failed"); return 3;
    }
    if(last_dot != size_copied / bytes_per_dot) {
      printf("."); fflush(stdout);
      last_dot = size_copied / bytes_per_dot;
      if(last_dot % dots_per_line == 0) {
        struct timeval line_stop_time;
        gettimeofday(&line_stop_time,0);
        double seconds = tv_to_double(line_stop_time) - tv_to_double(load_start_time);
        printf(" %6.1f s %6.2f mb/s %6.2f tuples/s\n", seconds, (double)size_copied/(1024.0 * 1024.0 * seconds), (double)num_tuples/seconds);
      }
    }
  }
  logstore_client_send_tuple(to,NULL);
  if(ret != LOGSTORE_RESPONSE_SUCCESS) {
    perror("Close bulk load failed"); return 3;
  }
  logstore_client_close(from);
  logstore_client_close(to);
  printf("Copy database done.  %lld tuples, %lld bytes\n", (long long)num_tuples, (long long)size_copied);
  return 0;

}

