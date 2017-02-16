/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <cassert>
#include <cstring>
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw.h>
#include <nfsc/libnfs-raw-nfs.h>
#include "logger.h"

/// @brief internal representation of an inode.
///
/// Currently really just a reference counted NFS file handle.
///
/// NB: the only reason to roll my own ref counting rather than
/// use shared_ptr is that I cheat like a bastard and stuff pointers
/// to inodes in fuse_ino_t, which can't hold a shared_ptr instance.
class InodeInternal {
 public:
  InodeInternal(
      const nfs_fh3* fh,
      std::shared_ptr<std::string> local_cache_path)
      : refcount_(1), local_cache_path_(local_cache_path) {
    fh_.data.data_val = new char[fh->data.data_len];
    fh_.data.data_len = fh->data.data_len;
    ::memcpy(fh_.data.data_val, fh->data.data_val, fh_.data.data_len);
    local_cache_path_ = local_cache_path;
  }

  void ref() {
    assert(refcount_ >= 0);
    ++refcount_;
  };

  void deref(nfusr::Logger *logger = nullptr, const char *caller = nullptr) {
    assert(refcount_ > 0);
    if (--refcount_ == 0) {
      if (logger && caller) {
        logger->LOG_MSG(LOG_DEBUG, "%s deleting inode %p.\n", caller, this);
      }
      delete this;
    }
  };

  const nfs_fh3& getFh() const {
    return fh_;
  };

  bool hasLocalCachePath() {
    if (local_cache_path_) {
      return true;
    } else {
      return false;
    }
  }

  std::shared_ptr<std::string> getLocalCachePath() {
    return local_cache_path_;
  };

 private:
  nfs_fh3 fh_;
  ~InodeInternal() {
    assert(refcount_ == 0);
    delete[] fh_.data.data_val;
  }
  std::atomic_int refcount_;
  std::shared_ptr<std::string> local_cache_path_;
};
