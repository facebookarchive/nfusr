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
#include "ClientStats.h"

/// @brief Hold a target connection URL and current connection state.
class ConnectionTarget {
 public:
  explicit ConnectionTarget(
      std::shared_ptr<std::string> url,
      bool connected = false)
      : url_(url), connected_(connected) {}

  std::shared_ptr<std::string> getUrl() const {
    return url_;
  }
  bool getConnected() const {
    return connected_;
  }
  void setConnected(bool connected) {
    connected_ = connected;
  }
  void setBlacklisted(std::chrono::duration<int> duration) {
    blacklistedUntil_ = std::chrono::steady_clock::now() + duration;
  }
  bool isBlacklisted() {
    return std::chrono::steady_clock::now() < blacklistedUntil_;
  }

 private:
  std::shared_ptr<std::string> url_;
  bool connected_;
  std::chrono::time_point<std::chrono::steady_clock> blacklistedUntil_;
};

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
      const char* hostscript,
      const int scriptRefreshSeconds,
      std::shared_ptr<nfusr::Logger> logger,
      std::shared_ptr<ClientStats> stats,
      unsigned simultaneousConnections,
      int nfsTimeoutMs);
  ~NfsConnectionPool();

  std::shared_ptr<NfsConnection> get();
  void failed(std::shared_ptr<NfsConnection> conn);

 private:
  std::vector<std::string> runScript(const char * scriptPath);
  std::shared_ptr<nfusr::Logger> logger_;
  std::shared_ptr<ClientStats> stats_;
  // lock_ protects following members:
  std::mutex lock_;
  std::vector<std::shared_ptr<NfsConnection>> liveConnections_;
  std::vector<std::shared_ptr<NfsConnection>> zombieConnections_;
  std::vector<ConnectionTarget> targets_;
  unsigned next_;
  // end of members protected by _lock.
  unsigned liveTarget_; // number of live connections we want to maintain.
  int nfsTimeoutMs_;

  void reaper();

  std::thread reaperThread_;
  bool terminateReaper_;
  std::mutex reaperMutex_;
  std::condition_variable reaperCondvar_;

  void dumpStats(std::shared_ptr<nfusr::Logger> logger, const char *prefix);

  // scriptMutex_ protects terminalScript_
  std::mutex scriptMutex_;
  std::condition_variable scriptCondvar_;
  std::thread scriptThread_; // a thread to run script and update targets_
  bool terminateScript_;
  const size_t nUrls_; // number of urls initially passed in from command line
  void updateTargets(const char* hostscript, const int scriptRefreshSeconds);
};
