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
#include <memory>
#include <mutex>
#include <vector>

#include "NfsConnection.h"
#include "logger.h"

/// @brief NfsConnectionPool maintains a set of NfsConnections with servers.
///
/// This class attempts to keep some number of simultaneous connections open
/// out of a (possibly larger) set of server URLs. When a connection fails,
/// it attempts to set up a new one from the pool of unattached URLs.
///
/// The handy get() method returns a connection to use. Currently it simply
/// round-robins over live connections. This might benefit from more clever
/// scheduling in future.
class NfsConnectionPool {
 public:
  NfsConnectionPool(
      std::vector<std::string>& urls,
      std::shared_ptr<nfusr::Logger> logger,
      unsigned simultaneousConnections = 1);
  ~NfsConnectionPool();

  std::shared_ptr<NfsConnection> get();
  void failed(std::shared_ptr<NfsConnection> conn);

 private:
  std::shared_ptr<nfusr::Logger> logger_;
  // lock_ protects following members:
  std::mutex lock_;
  std::vector<std::string> urls_;
  std::vector<std::shared_ptr<NfsConnection>> liveConnections_;
  std::vector<std::shared_ptr<NfsConnection>> zombieConnections_;
  std::vector<std::shared_ptr<std::string>> unconnectedUrls_;
  unsigned next_;
  // end of members protected by _lock.
  unsigned liveTarget_; // number of live connections we want to maintain.

  void reaper();

  std::thread reaperThread_;
  bool terminateReaper_;
  std::mutex reaperMutex_;
  std::condition_variable reaperCondvar_;
};
