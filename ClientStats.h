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
#include <vector>
#include <functional>

#include "fuse_optype.h"
#include "logger.h"

/// @brief stats about the RPC subsystem as a whole.
struct rpcStats {
  uint64_t issued;          ///< total RPC calls issued.
  uint64_t completed;       ///< total RPC calls completed.
  uint64_t failures;        ///< failed RPC calls.
  uint64_t timeouts;        ///< timed out RPC calls (== retries).
  uint64_t connect_success; ///< transport layer connections.
  uint64_t connect_failure; ///< failures of transport layer.
};

/// @brief stats about a particular operation type (e.g. LOOKUP)
struct opStats {
  uint64_t count;         ///< total completed.
  uint64_t time_usec;     ///< total time for all 'count' completions.
  uint64_t max_time_usec; ///< max time for any completion.
};

using statsCb =
    std::function<void(std::shared_ptr<nfusr::Logger>, const char*)>;

class ClientStats {
 public:
  ClientStats() : enabled_(false) {}
  ~ClientStats();
  int start(
      const char* fileName,
      const char* prefix,
      unsigned interval_seconds = 60);

  void addCallback(statsCb cb) {
    callbacks_.push_back(cb);
  }

  void recordIssue();

  void recordOperation(
      enum fuse_optype optype,
      std::chrono::microseconds elapsed);

  void recordRpcFailure(bool isTimeout) {
    std::unique_lock<std::mutex> lock(lk_);
    ++rpcIntervalStats_.failures;
    ++rpcTotalStats_.failures;
    if (isTimeout) {
      ++rpcIntervalStats_.timeouts;
      ++rpcTotalStats_.timeouts;
    }
  }

  void recordConnectSuccess() {
    std::unique_lock<std::mutex> lock(lk_);
    ++rpcIntervalStats_.connect_success;
    ++rpcTotalStats_.connect_success;
  }

  void recordConnectFailure() {
    std::unique_lock<std::mutex> lock(lk_);
    ++rpcIntervalStats_.connect_failure;
    ++rpcTotalStats_.connect_failure;
  }

 private:
  void statsDumpThread();

  bool enabled_;

  // lk_ protects accesses to the shared counters below.
  std::mutex lk_;
  opStats opIntervalStats_[num_fuse_optypes];
  opStats opTotalStats_[num_fuse_optypes];
  rpcStats rpcIntervalStats_;
  rpcStats rpcTotalStats_;
  // end of members protected by lk_.

  unsigned interval_;
  std::shared_ptr<nfusr::Logger> logger_;
  std::condition_variable cv_;
  std::thread threadHandle_;
  bool terminateThread_;
  std::string prefix_;

  std::vector<statsCb> callbacks_;
};
