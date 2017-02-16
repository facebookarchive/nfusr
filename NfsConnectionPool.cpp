/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NfsConnectionPool.h"

NfsConnectionPool::NfsConnectionPool(
    std::vector<std::string>& urls,
    std::shared_ptr<nfusr::Logger> logger,
    unsigned simultaneousConnections) {
  logger_ = logger;
  for (auto& url : urls) {
    unconnectedUrls_.push_back(std::make_shared<std::string>(url));
  }
  next_ = 0;
  liveTarget_ = simultaneousConnections;
  terminateReaper_ = false;
  reaperThread_ = std::thread(&NfsConnectionPool::reaper, this);
}

NfsConnectionPool::~NfsConnectionPool() {
  lock_.lock();
  for (auto& conn : liveConnections_) {
    conn->close();
  }
  lock_.unlock();

  reaperMutex_.lock();
  terminateReaper_ = true;
  reaperMutex_.unlock();
  reaperCondvar_.notify_one();

  reaperThread_.join();
}

void NfsConnectionPool::reaper() {
  std::unique_lock<std::mutex> lock(reaperMutex_);

  do {
    reaperCondvar_.wait(lock);

    while (zombieConnections_.size()) {
      auto conn = zombieConnections_.begin();
      (*conn)->close();
      zombieConnections_.erase(conn);
    }
  } while (!terminateReaper_);
}

std::shared_ptr<NfsConnection> NfsConnectionPool::get() {
  std::lock_guard<std::mutex> lock(lock_);

  if (liveConnections_.size() < liveTarget_) {
    // Try to open a new connection using the set of unconnected URLS.
    std::vector<std::shared_ptr<std::string>> sadUrls;

    while (unconnectedUrls_.size()) {
      auto url = *(--unconnectedUrls_.end());
      unconnectedUrls_.pop_back();

      auto conn = std::make_shared<NfsConnection>(logger_);
      logger_->LOG_MSG(LOG_DEBUG, "Trying to open %s.\n", url->c_str());
      if (!conn->open(url)) {
        logger_->LOG_MSG(
            LOG_INFO, "Opened connection %s.\n", conn->describe().c_str());
        liveConnections_.push_back(conn);
        break;
      } else {
        logger_->LOG_MSG(
            LOG_INFO, "Failed to open connection to %s.\n", url->c_str());
        sadUrls.push_back(url);
      }
    }

    // Put back all the sad urls for next time.
    for (auto& url : sadUrls) {
      unconnectedUrls_.push_back(url);
    }
  }

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
      liveConnections_.erase(i);
      unconnectedUrls_.push_back(conn->getUrl());

      // We cannot directly close the connection here, because we are
      // inside the connection's ioLoop() with the lock held. Push it on
      // to a queue for the reaper,
      reaperMutex_.lock();
      zombieConnections_.push_back(conn);
      reaperCondvar_.notify_one();
      reaperMutex_.unlock();
      logger_->LOG_MSG(
          LOG_INFO, "%s: %s.\n", __func__, conn->describe().c_str());
      break;
    }
  }
}
