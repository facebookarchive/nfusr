/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NfsConnectionPool.h"
#include <thread>

NfsConnectionPool::NfsConnectionPool(
    std::vector<std::string>& urls,
    const char* hostscript,
    const int scriptRefreshSeconds,
    std::shared_ptr<nfusr::Logger> logger,
    std::shared_ptr<ClientStats> stats,
    unsigned simultaneousConnections,
    int nfsTimeoutMs)
    : nUrls_(urls.size()) {
  logger_ = logger;
  stats_ = stats;
  if (stats_) {
    stats_->addCallback(std::bind(
        &NfsConnectionPool::dumpStats,
        this,
        std::placeholders::_1,
        std::placeholders::_2));
  }
  for (auto& url : urls) {
    targets_.emplace_back(std::make_shared<std::string>(url));
  }
  next_ = 0;
  liveTarget_ = simultaneousConnections;
  nfsTimeoutMs_ = nfsTimeoutMs;
  terminateReaper_ = false;
  reaperThread_ = std::thread(&NfsConnectionPool::reaper, this);

  terminateScript_ = false;
  if (hostscript) {
    // insert these generated connections after `urls` for nUrls_ to work
    auto hostFromScript = runScript(hostscript);
    if (hostFromScript.size() == 0) {
      logger_->LOG_MSG(LOG_NOTICE, "%s generates no hosts.\n", hostscript);
    }
    for (auto& url : hostFromScript) {
      targets_.emplace_back(std::make_shared<std::string>(url));
    }
    scriptThread_ = std::thread(
        &NfsConnectionPool::updateTargets,
        this,
        hostscript,
        scriptRefreshSeconds);
  }
}

NfsConnectionPool::~NfsConnectionPool() {
  lock_.lock();
  while (liveConnections_.size()) {
    auto conn = liveConnections_.back();
    liveConnections_.pop_back();
    lock_.unlock();
    conn->close();
    lock_.lock();
  }
  lock_.unlock();

  reaperMutex_.lock();
  terminateReaper_ = true;
  reaperMutex_.unlock();
  reaperCondvar_.notify_one();

  reaperThread_.join();

  if (scriptThread_.joinable()) {
    scriptMutex_.lock();
    terminateScript_ = true;
    scriptMutex_.unlock();
    scriptCondvar_.notify_one();
    scriptThread_.join();
  }
}

void NfsConnectionPool::dumpStats(
    std::shared_ptr<nfusr::Logger> logger,
    const char* prefix) {
  int queue_depth = 0;

  {
    std::lock_guard<std::mutex> guard(lock_);

    for (auto& conn : liveConnections_) {
      queue_depth += conn->getQueuedRequests();
    }
  }

  logger->printf("\"%snfs-queue.count\": \"%d\"\n", prefix, queue_depth);
}

void NfsConnectionPool::reaper() {
  std::unique_lock<std::mutex> lock(reaperMutex_);

  do {
    reaperCondvar_.wait(lock);

    while (zombieConnections_.size()) {
      auto conn = zombieConnections_.back();
      zombieConnections_.pop_back();
      lock.unlock();
      conn->close();
      lock.lock();
    }
  } while (!terminateReaper_);
}

std::shared_ptr<NfsConnection> NfsConnectionPool::get() {
  std::lock_guard<std::mutex> lock(lock_);

  if (liveConnections_.size() < liveTarget_) {
    for (int pass = 0; pass < 2; pass++) {
      // Try to open a new connection using the set of target URLS.
      for (auto& target : targets_) {
        // Only use this target if it has not been blacklisted,
        // or we are on the second pass, which means we found no usable targets
        // on the first pass.
        if (!target.isBlacklisted() || pass > 0) {
          if (!target.getConnected()) {
            auto url = target.getUrl();
            auto conn =
                std::make_shared<NfsConnection>(logger_, stats_, nfsTimeoutMs_);
            logger_->LOG_MSG(LOG_DEBUG, "Trying to open %s.\n", url->c_str());
            if (!conn->open(url)) {
              logger_->LOG_MSG(
                  LOG_NOTICE,
                  "Opened connection %s.\n",
                  conn->describe().c_str());
              if (stats_) {
                stats_->recordConnectSuccess();
              }
              liveConnections_.push_back(conn);
              target.setConnected(true);

              // Log if this target is actually blacklisted and we had no choice
              if (target.isBlacklisted()) {
                logger_->LOG_MSG(
                    LOG_WARNING,
                    "Using target %s even though it is blacklisted!\n",
                    url->c_str());
              }
              goto connected;
            } else {
              logger_->LOG_MSG(
                  LOG_NOTICE,
                  "Failed to open connection to %s.\n",
                  url->c_str());
              if (stats_) {
                stats_->recordConnectFailure();
              }
            }
          }
        }
      }
    }
  }
connected:
  if (liveConnections_.size()) {
    next_ = (next_ + 1) % liveConnections_.size();
    auto conn = liveConnections_[next_];
    logger_->LOG_MSG(
        LOG_DEBUG,
        "%s selected %s (#%u of %lu).\n",
        __func__,
        conn->describe().c_str(),
        next_ + 1,
        liveConnections_.size());
    return conn;
  }

  logger_->LOG_MSG(LOG_WARNING, "%s: no live connections.\n", __func__);

  return nullptr;
}

void NfsConnectionPool::failed(std::shared_ptr<NfsConnection> conn) {
  std::lock_guard<std::mutex> lock(lock_);
  // Remove from list of live connections.
  for (auto i = liveConnections_.begin(); i != liveConnections_.end(); ++i) {
    if (*i == conn) {
      for (auto& target : targets_) {
        if (target.getUrl() == conn->getUrl()) {
          target.setConnected(false);
          target.setBlacklisted(std::chrono::seconds(90));
          break;
        }
      }
      liveConnections_.erase(i);

      // We cannot directly close the connection here, because we are
      // inside the connection's ioLoop() with the lock held. Push it on
      // to a queue for the reaper,
      reaperMutex_.lock();
      zombieConnections_.push_back(conn);
      reaperCondvar_.notify_one();
      reaperMutex_.unlock();
      logger_->LOG_MSG(
          LOG_NOTICE, "%s: %s.\n", __func__, conn->describe().c_str());
      break;
    }
  }
}

static std::vector<std::string> parseHosts(const std::string& s) {
  std::vector<std::string> result;
  static const char* nfs_prefix = "nfs://";
  size_t beg = s.find(nfs_prefix);

  while (beg != std::string::npos) {
    size_t space_pos = s.find_first_of(" \t\r\n", beg);
    if (space_pos == std::string::npos) {
      result.push_back(s.substr(beg));
      break;
    } else {
      result.push_back(s.substr(beg, space_pos - beg));
      beg = s.find(nfs_prefix, space_pos + 1);
    }
  }

  return result;
}

std::vector<std::string> NfsConnectionPool::runScript(const char* scriptPath) {
  std::array<char, 128> buffer;
  std::string output;
  FILE* pFile = popen(scriptPath, "r");

  if (pFile) {
    size_t readN = 0;
    while (!feof(pFile)) {
      if ((readN = fread(buffer.data(), 1, buffer.size() - 1, pFile))) {
        buffer[readN] = '\0';
        output += buffer.data();
      }
    }

    pclose(pFile);
    return parseHosts(output);
  } else {
    logger_->LOG_MSG(LOG_DEBUG, "popen returns null. %s failed!\n", scriptPath);
  }
  return std::vector<std::string>();
}

void NfsConnectionPool::updateTargets(
    const char* hostscript,
    const int scriptRefreshSeconds) {
  std::unique_lock<std::mutex> scriptLock(scriptMutex_);

  do {
    logger_->LOG_MSG(
        LOG_DEBUG,
        "script %s is running (every %d seconds)\n",
        hostscript,
        scriptRefreshSeconds);
    auto hosts = runScript(hostscript);

    // update the target list
    if (!hosts.empty()) {
      std::lock_guard<std::mutex> lock(lock_);
      targets_.erase(targets_.begin() + nUrls_, targets_.end());
      for (auto& host : hosts) {
        targets_.emplace_back(std::make_shared<std::string>(host));
      }
    } else {
      logger_->LOG_MSG(LOG_NOTICE, "%s generates no hosts.\n", hostscript);
    }

    scriptCondvar_.wait_for(
        scriptLock, std::chrono::seconds(scriptRefreshSeconds));

  } while (!terminateScript_);
}
