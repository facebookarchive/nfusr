/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <condition_variable>
#include <thread>

#include "fuse_optype.h"
#include "logger.h"

class ClientStats {
 public:
  ClientStats() : enabled_(false) {}
  ~ClientStats();
  int start(
      const char* fileName,
      const char* prefix,
      unsigned interval_seconds = 60);
  void recordOperation(
      enum fuse_optype optype,
      std::chrono::microseconds elapsed);

 private:
  void statsDumpThread();

  bool enabled_;
  struct {
    uint64_t total_ops;
    uint64_t total_time_usec;
    uint64_t interval_ops;
    uint64_t interval_time_usec;
  } counters_[num_fuse_optypes];
  unsigned interval_;
  std::unique_ptr<nfusr::Logger> logger_;
  std::mutex lk_;
  std::condition_variable cv_;
  std::thread threadHandle_;
  bool terminateThread_;
  std::string prefix_;
};
