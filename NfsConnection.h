/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <sys/time.h>
#include <nfsc/libnfs.h>
#include <memory>
#include <mutex>
#include <thread>

#include "logger.h"

/// @brief NfsConnection represents an active connection to a NFS server.
///
/// This object includes the thread (ioLoop_) which handles I/O with the
/// server.
class NfsConnection {
 public:
  explicit NfsConnection(std::shared_ptr<nfusr::Logger> logger);
  ~NfsConnection();

  int open(std::shared_ptr<std::string> url);
  int close();

  void get() {
    lock_.lock();
  }

  void put();

  struct nfs_context* getNfsCtx() {
      return ctx_;
  }

  std::shared_ptr<std::string> getUrl() const {
    return url_;
  }

  /// @brief a user-friendy description of the connection, for debugging.
  std::string const &describe() const {
    return description_;
  }

 private:
  void ioLoop();
  int serviceConnection(int fd);
  int makeWakeable();

  std::shared_ptr<nfusr::Logger> logger_;
  std::shared_ptr<std::string> url_;
  std::mutex lock_;
  struct nfs_context* ctx_;
  int wake_fd_;
  std::thread ioLoop_;
  bool opened_;
  bool closed_;
  bool terminate_;
  std::string description_;
};
