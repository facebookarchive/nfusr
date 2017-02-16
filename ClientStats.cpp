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
  logger_ = std::make_unique<nfusr::Logger>();
  if ((rc = logger_->openFile(fileName, true)) != 0) {
    return rc;
  }

  ::memset(counters_, 0, sizeof(counters_));
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
      for (unsigned ix = 1; ix < num_fuse_optypes; ++ix) {
        auto name = fuse_optype_name((enum fuse_optype)ix);
        logger_->printf(
            "\"%saggr.%s.count\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            counters_[ix].total_ops);
        logger_->printf(
           "\"%saggr.%s.avg\": \"%lf\"\n",
            prefix_.c_str(),
            name,
            avg(counters_[ix].total_time_usec, counters_[ix].total_ops));
        logger_->printf(
           "\"%sinter.%s.count\": \"%lu\"\n",
            prefix_.c_str(),
            name,
            counters_[ix].interval_ops);
        logger_->printf(
            "\"%sinter.%s.avg\": \"%lf\"\n",
            prefix_.c_str(),
            name,
            avg(counters_[ix].interval_time_usec, counters_[ix].interval_ops));
        counters_[ix].interval_time_usec = 0;
        counters_[ix].interval_ops = 0;
      }
      logger_->printf("<<EOF>>\n");

      logger_->flush();
    }
  }
}

void ClientStats::recordOperation(
    enum fuse_optype optype,
    std::chrono::microseconds elapsed) {

  assert(optype < num_fuse_optypes);

  std::unique_lock<std::mutex> lock(lk_);
  counters_[optype].total_ops++;
  counters_[optype].total_time_usec += elapsed.count();
  counters_[optype].interval_ops++;
  counters_[optype].interval_time_usec += elapsed.count();
}
