/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NfsConnection.h"

#include <cstring>
#include <nfsc/libnfs-raw.h>
#include <poll.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <sstream>
#include <unistd.h>

NfsConnection::NfsConnection(std::shared_ptr<nfusr::Logger> logger) {
  logger_ = logger;
  if ((ctx_ = nfs_init_context()) == nullptr) {
    logger_->LOG_MSG(LOG_ERR, "Cannot initialize NFS context.\n");
    throw std::runtime_error("Cannot initialize NFS context.");
  }
  wake_fd_ = -1;
  opened_ = false;
  closed_ = false;

  std::stringstream description;
  description << (void*)this << "(closed)";
  description_ = description.str();
}

NfsConnection::~NfsConnection() {
  logger_->LOG_MSG(LOG_DEBUG, "%s(%s).\n", __func__, description_.c_str());
  if (opened_) {
    assert(closed_);
  }
  if (ctx_ != nullptr) {
    nfs_destroy_context(ctx_);
  }
}

int NfsConnection::makeWakeable() {
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGUSR1);

  if (sigprocmask(SIG_BLOCK, &sigset, nullptr)) {
      logger_->LOG_MSG(LOG_ERR, "sigprocmask() failed: %s.\n",
              strerror(errno));
      return -1;
  }

  wake_fd_ = signalfd(-1, &sigset, 0);
  if (wake_fd_ == -1) {
      logger_->LOG_MSG(LOG_ERR, "signalfd() failed: %s.\n",
              strerror(errno));
      return -1;
  }

  return 0;
}

static void wakeThread(std::thread& t) {
  pthread_kill(t.native_handle(), SIGUSR1);
}

void NfsConnection::put() {
    if (nfs_which_events(ctx_) & POLLOUT) {
        // We have some outgoing traffic. Try
        // to send now while we hold the lock.
        int rc = nfs_service(ctx_, POLLOUT);
        if (rc || (nfs_which_events(ctx_) & POLLOUT)) {
            // Can't send, wake main loop to retry.
            wakeThread(ioLoop_);
        }
    }
    lock_.unlock();
}

int NfsConnection::serviceConnection(int fd) {
  int rc;
  struct pollfd pfd[2];

  pfd[0].fd = fd;
  pfd[0].events = nfs_which_events(ctx_) | POLLERR | POLLHUP;
  pfd[0].revents = 0;
  pfd[1].fd = wake_fd_;
  pfd[1].events = POLLIN;
  pfd[1].revents = 0;

  lock_.unlock();
  rc = poll(pfd, 2, -1);
  lock_.lock();

  if (rc < 0) {
    logger_->LOG_MSG(LOG_ERR, "Poll failed.\n");
    return rc;
  }

  if (pfd[0].revents) {
      rc = nfs_service(ctx_, pfd[0].revents & (POLLIN | POLLOUT));

      if (rc < 0) {
          logger_->LOG_MSG(LOG_INFO, "nfs_service() failed.\n");
          return rc;
      }

      if (pfd[0].revents & (POLLERR | POLLHUP)) {
          logger_->LOG_MSG(LOG_INFO, "Poll error.\n");
          return -EIO;
      }
  }

  if (pfd[1].revents) {
      struct signalfd_siginfo info;
      if (::read(wake_fd_, &info, sizeof(info)) != sizeof(info)) {
          logger_->LOG_MSG(LOG_INFO, "read(wake_fd_) failed.\n");
          return -EIO;
      }
  }

  return 0;
}

void NfsConnection::ioLoop() {
  logger_->LOG_MSG(
      LOG_DEBUG, "%s(%s) starting.\n", __func__, description_.c_str());

  lock_.lock();

  makeWakeable();

  int fd = nfs_get_fd(ctx_);

  while (!terminate_) {
    if (serviceConnection(fd)) {
      break;
    }
  }

  rpc_disconnect(nfs_get_rpc_context(ctx_), "nfsConnection::ioLoop");

  lock_.unlock();

  logger_->LOG_MSG(LOG_DEBUG, "%s(%s) done.\n", __func__, description_.c_str());
}

int NfsConnection::open(std::shared_ptr<std::string> url) {
  auto parsed_url = nfs_parse_url_full(ctx_, url->c_str());
  if (parsed_url == nullptr) {
    logger_->LOG_MSG(LOG_ERR, "Failed to parse URL '%s'.\n", url->c_str());
    return -EINVAL;
  }

  auto dir = std::string(parsed_url->path) + std::string(parsed_url->file);

  if (nfs_mount(ctx_, parsed_url->server, dir.c_str()) != 0) {
    logger_->LOG_MSG(
        LOG_ERR,
        "Failed to mount nfs share %s/%s: %s.\n",
        parsed_url->server,
        dir.c_str(),
        nfs_get_error(ctx_));
    nfs_destroy_url(parsed_url);
    return -EIO;
  }

  nfs_destroy_url(parsed_url);

  std::stringstream description;
  description << (void*)this << "(" << url << "/" << nfs_get_fd(ctx_) << ")";
  description_ = description.str();

  url_ = url;
  terminate_ = false;
  opened_ = true;
  ioLoop_ = std::thread(&NfsConnection::ioLoop, this);

  return 0;
}

int NfsConnection::close() {
  lock_.lock();

  logger_->LOG_MSG(LOG_DEBUG, "%s(%s).\n", __func__, description_.c_str());

  terminate_ = true;
  wakeThread(ioLoop_);
  lock_.unlock();

  ioLoop_.join();
  closed_ = true;

  if (wake_fd_ != -1) {
      ::close(wake_fd_);
      wake_fd_ = -1;
  }

  return 0;
}
