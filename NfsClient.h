/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64
#include <fuse/fuse_lowlevel.h>

#include <errno.h>
#include <atomic>
#include <ctime>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "ClientStats.h"
#include "InodeInternal.h"
#include "NfsConnectionPool.h"
#include "fuse_optype.h"
#include "logger.h"

class RpcContext;
class RpcContextParentName;
class RpcContextInode;
class RpcContextInodeFile;
class ReadRpcContext;
class WriteRpcContext;
class SetattrRpcContext;
class AccessRpcContext;

enum NfsClientPermissionMode {
  InitialPerms, // always use initial permissions.
  StrictPerms, // always use proper permissions.
  SloppyPerms // use proper permissions when creating,
              // use inital perms otherwise
};

/// @brief NfsClient implements fundamental FUSE operations on a pool of NFS
/// servers.
class NfsClient {
 public:
  NfsClient(
      std::vector<std::string>& urls,
      const char* hostscript,
      const int scriptRefreshSeconds,
      size_t targetConnections,
      int nfsTimeoutMs,
      std::shared_ptr<nfusr::Logger> logger,
      std::shared_ptr<ClientStats> stats,
      bool errorInjection,
      NfsClientPermissionMode permMode);
  virtual ~NfsClient();

  bool start(std::shared_ptr<std::string> cacheRoot);
  bool checkRpcCompletion(
      RpcContext* info,
      int rpc_status,
      int nfs_status,
      bool& retry);
  bool checkRpcCall(RpcContext* info, int rpc_status);

  NfsConnectionPool& getConnectionPool() {
    return connPool_;
  }
  InodeInternal* inodeLookup(fuse_ino_t);

  std::shared_ptr<nfusr::Logger> getLogger() const {
    return logger_;
  }

  bool statsEnabled() const {
    return stats_ != nullptr;
  }

  void recordOperationStats(
      enum fuse_optype optype,
      std::chrono::microseconds elapsed) {
    if (stats_) {
      stats_->recordOperation(optype, elapsed);
    }
  }

  void recordOperationIssue() {
    if (stats_) {
      stats_->recordIssue();
    }
  }

  void recordRpcFailure(bool isTimeout) {
    if (stats_) {
      stats_->recordRpcFailure(isTimeout);
    }
  }

  bool shouldRetry(int rpc_status, RpcContext* ctx);

  static void stat_from_fattr3(struct stat* st, const struct fattr3* attr);
  virtual void replyEntry(
      RpcContext* ctx,
      const nfs_fh3* fh,
      const struct fattr3* attr,
      const struct fuse_file_info* file,
      std::shared_ptr<std::string> local_cache_path,
      const char* caller,
      fuse_ino_t parent,
      const char* name);
  void getattrWithContext(RpcContextInodeFile* ctx);
  void getattr(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file);
  void opendir(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file);
  void readdir(
      fuse_req_t req,
      fuse_ino_t inode,
      size_t size,
      off_t off,
      struct fuse_file_info* file);
  void
  releasedir(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file);
  void lookupWithContext(RpcContextParentName* ctx);
  void lookup(fuse_req_t req, fuse_ino_t parent, const char* name);
  static void lookupCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void openWithContext(RpcContextInodeFile* ctx);
  void open(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file);
  void readWithContext(ReadRpcContext* ctx);
  virtual void read(
      fuse_req_t req,
      fuse_ino_t inode,
      size_t size,
      off_t off,
      struct fuse_file_info* file);
  void writeWithContext(WriteRpcContext* ctx);
  void write(
      fuse_req_t req,
      fuse_ino_t inode,
      const char* buf,
      size_t size,
      off_t off,
      struct fuse_file_info* file);
  void static createCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void create(
      fuse_req_t req,
      fuse_ino_t parent,
      const char* name,
      mode_t mode,
      struct fuse_file_info* file);
  void mknod(
      fuse_req_t req,
      fuse_ino_t parent,
      const char* name,
      mode_t mode,
      dev_t rdev);
  void forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);
  void release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
  void static mkdirCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode);
  void setattrWithContext(SetattrRpcContext* ctx);
  void setattr(
      fuse_req_t req,
      fuse_ino_t ino,
      struct stat* attr,
      int valid,
      struct fuse_file_info* fi);
  void unlink(fuse_req_t req, fuse_ino_t parent, const char* name);
  void rmdir(fuse_req_t req, fuse_ino_t parent, const char* name);
  void static symlinkCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void symlink(
      fuse_req_t req,
      const char* link,
      fuse_ino_t parent,
      const char* name);
  void readlink(fuse_req_t req, fuse_ino_t inode);
  void static linkCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void link(
      fuse_req_t req,
      fuse_ino_t inode,
      fuse_ino_t newparent,
      const char* newname);
  void rename(
      fuse_req_t req,
      fuse_ino_t parent,
      const char* name,
      fuse_ino_t newparent,
      const char* newname);
  void
  fsync(fuse_req_t req, fuse_ino_t inode, int datasync, fuse_file_info* file);
  void statfsWithContext(RpcContextInode* ctx);
  void statfs(fuse_req_t req, fuse_ino_t inode);
  void accessWithContext(AccessRpcContext* ctx);
  void access(fuse_req_t req, fuse_ino_t inode, int mask);

  uint64_t getMaxRead() {
    return maxRead_;
  }

  std::shared_ptr<nfusr::Logger> getLogger() {
    return logger_;
  }

  /// @brief Initialize error injection system.
  ///
  /// @param min: minimum number of requests before injecting error.
  /// @param max: max number of requests allowed before injecting error.
  void initErrorInjection(uint32_t min = 50, uint32_t max = 100) {
    assert(max > min);
    eiMin_ = min;
    eiMax_ = max;
    eiCounter_ = 0;
    eiRng_.seed(time(nullptr));
  }

  /// @brief Determine whether to inject error.
  ///
  /// If error injection is enabled, randomly returns true
  /// on somewhere between eiMin and eiMax calls.
  bool errorInjection() {
    if (eiMin_) {
      if (++eiCounter_ >= eiMin_) {
        auto range = eiMax_ - eiMin_;

        if (eiRng_() % range <= eiCounter_ - eiMin_) {
          eiCounter_ = 0;
          return true;
        }
      }
    }
    return false;
  }

  static void setAttrTimeout(double to) {
    attrTimeout_ = to;
  }

  static double getAttrTimeout() {
    return attrTimeout_;
  }

 private:
  void setUidGid(RpcContext const& ctx, bool creat);
  void restoreUidGid(RpcContext const& ctx, bool creat);

  NfsConnectionPool connPool_;
  InodeInternal* rootInode_;
  uint64_t maxRead_;
  uint64_t maxWrite_;
  std::shared_ptr<nfusr::Logger> logger_;
  std::shared_ptr<ClientStats> stats_;

  static double attrTimeout_;

  // Error injection contol.
  uint32_t eiMin_;
  uint32_t eiMax_;
  uint32_t eiCounter_;
  std::mt19937 eiRng_;

  // UID/GID fun
  NfsClientPermissionMode permMode_;
  uid_t initial_uid_;
  gid_t initial_gid_;
};
