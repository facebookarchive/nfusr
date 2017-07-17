/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "ClientStats.h"
#include <cassert>
#include <cstring>

ClientStats::~ClientStats() {
  if (enabled_) {
    {
      std::unique_lock<std::mutex> lock(lk_);
      terminateThread_ = true;
      cv_.notify_one();
    }
    threadHandle_.join();
  }
}

int ClientStats::start(
    const char* fileName,
    const char* prefix,
    unsigned interval) {
  int rc;

  if (prefix) {
    prefix_ = prefix;
    prefix_ += ".";
  }
  interval_ = interval;
  logger_ = std::make_shared<nfusr::Logger>();
  if ((rc = logger_->openFile(
           fileName, /* fifoMode = */ true, /* autoFlush = */ false)) != 0) {
    return rc;
  }

  ::memset(&opIntervalStats_, 0, sizeof(opIntervalStats_));
  ::memset(&rpcIntervalStats_, 0, sizeof(rpcIntervalStats_));
  ::memset(&opTotalStats_, 0, sizeof(opTotalStats_));
  ::memset(&rpcTotalStats_, 0, sizeof(rpcTotalStats_));

  terminateThread_ = false;
  threadHandle_ = std::thread(&ClientStats::statsDumpThread, this);
  enabled_ = true;

  return 0;
}

static inline double avg(uint64_t total_usec, uint64_t count) {
  if (count) {
    return (double)total_usec / (double)count;
  }
  return 0.;
}

void ClientStats::statsDumpThread() {
  std::unique_lock<std::mutex> lock(lk_);
  while (!terminateThread_) {
    if (cv_.wait_until(
            lock,
            std::chrono::steady_clock::now() +
                std::chrono::seconds(interval_)) == std::cv_status::timeout) {
      // Copy all the counters into local buffers, then drop the lock.
      opStats opIntervalStats[num_fuse_optypes];
      opStats opTotalStats[num_fuse_optypes];
      rpcStats rpcIntervalStats;
      rpcStats rpcTotalStats;

      ::memcpy(&opIntervalStats, &opIntervalStats_, sizeof(opIntervalStats));
      ::memcpy(&opTotalStats, &opTotalStats_, sizeof(opTotalStats));
      ::memcpy(&rpcIntervalStats, &rpcIntervalStats_, sizeof(rpcIntervalStats));
      ::memcpy(&rpcTotalStats, &rpcTotalStats_, sizeof(rpcTotalStats));

      ::memset(&opIntervalStats_, 0, sizeof(opIntervalStats_));
      ::memset(&rpcIntervalStats_, 0, sizeof(rpcIntervalStats_));

      lock.unlock();

      for (unsigned ix = 1; ix < num_fuse_optypes; ++ix) {
        auto name = fuse_optype_name((enum fuse_optype)ix);
        logger_->printf(
            "\"%saggr.%s.count\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            opTotalStats[ix].count);
        logger_->printf(
            "\"%saggr.%s.avg\": \"%lf\"\n",
            prefix_.c_str(),
            name,
            avg(opTotalStats[ix].time_usec, opTotalStats[ix].count));
        logger_->printf(
            "\"%saggr.%s.max\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            opTotalStats[ix].max_time_usec);
        logger_->printf(
            "\"%sinter.%s.count\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            opIntervalStats[ix].count);
        logger_->printf(
            "\"%sinter.%s.avg\": \"%lf\"\n",
            prefix_.c_str(),
            name,
            avg(opIntervalStats[ix].time_usec, opIntervalStats[ix].count));
        logger_->printf(
            "\"%sinter.%s.max\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            opIntervalStats[ix].max_time_usec);
      }

      logger_->printf(
          "\"%saggr.rpc-failures.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.failures);
      logger_->printf(
          "\"%sinter.rpc-failures.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.failures);

      logger_->printf(
          "\"%saggr.rpc-timeouts.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.timeouts);
      logger_->printf(
          "\"%sinter.rpc-timeouts.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.timeouts);

      logger_->printf(
          "\"%saggr.rpc-connect.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.connect_success);
      logger_->printf(
          "\"%sinter.rpc-connect.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.connect_success);

      logger_->printf(
          "\"%saggr.rpc-connect-fail.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.connect_failure);
      logger_->printf(
          "\"%sinter.rpc-connect-fail.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.connect_failure);

      logger_->printf(
          "\"%saggr.rpc-issued.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.issued);
      logger_->printf(
          "\"%sinter.rpc-issued.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.issued);

      logger_->printf(
          "\"%saggr.rpc-completed.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcTotalStats.completed);
      logger_->printf(
          "\"%sinter.rpc-completed.count\": \"%lu\"\n",
          prefix_.c_str(),
          rpcIntervalStats.completed);

      for (auto cb : callbacks_) {
        cb(logger_, prefix_.c_str());
      }

      logger_->printf("<<EOF>>\n");

      logger_->flush();

      lock.lock();
    }
  }
}

/// @brief record the issue of an operation.
void ClientStats::recordIssue() {
  std::unique_lock<std::mutex> lock(lk_);
  rpcIntervalStats_.issued++;
  rpcTotalStats_.issued++;
}

/// @brief record the completion of an operation.
void ClientStats::recordOperation(
    enum fuse_optype optype,
    std::chrono::microseconds elapsed) {
  assert(optype < num_fuse_optypes);

  uint64_t elapsed_usec = elapsed.count();

  std::unique_lock<std::mutex> lock(lk_);
  opTotalStats_[optype].count++;
  opTotalStats_[optype].time_usec += elapsed_usec;
  opTotalStats_[optype].max_time_usec =
      std::max(opTotalStats_[optype].max_time_usec, elapsed_usec);
  opIntervalStats_[optype].count++;
  opIntervalStats_[optype].time_usec += elapsed_usec;
  opIntervalStats_[optype].max_time_usec =
      std::max(opIntervalStats_[optype].max_time_usec, elapsed_usec);
  rpcIntervalStats_.completed++;
  rpcTotalStats_.completed++;
}
