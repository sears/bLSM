/*
 * merger.h
 *
 * Copyright 2010-2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#ifndef _MERGER_H_
#define _MERGER_H_

#include "logstore.h"

#include <stasis/common.h>

class merge_scheduler {
public:
  merge_scheduler(logtable * ltable);
  ~merge_scheduler();

  void start();
  void shutdown();

  void * memMergeThread();
  void * diskMergeThread();

private:
  pthread_t mem_merge_thread_;
  pthread_t disk_merge_thread_;
  logtable * ltable_;
  const double MIN_R;
};

#endif
