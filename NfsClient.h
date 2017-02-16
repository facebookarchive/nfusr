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
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "ClientStats.h"
#include "InodeInternal.h"
#include "NfsConnectionPool.h"
#include "fuse_optype.h"
#include "logger.h"

class RpcContext;
class ReadRpcContext;

/// @brief NfsClient implements fundamental FUSE operations on a pool of NFS
/// servers.
class NfsClient {
 public:
  NfsClient(
      std::vector<std::string>& urls,
      size_t targetConnections,
      std::shared_ptr<nfusr::Logger> logger,
      bool errorInjection);
  virtual ~NfsClient();

  bool start(
      std::shared_ptr<std::string> cacheRoot
  );
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
  bool getErrorInjection() const {
    return errorInjection_;
  }

  std::shared_ptr<nfusr::Logger> getLogger() const {
    return logger_;
  }

  int startStatsLogging(const char* fileName, const char *prefix = nullptr);
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

  static void stat_from_fattr3(
      struct stat* st,
      const struct fattr3*
      attr);
  virtual void replyEntry(
      RpcContext* ctx,
      const nfs_fh3* fh,
      const struct fattr3* attr,
      const struct fuse_file_info* file,
      std::shared_ptr<std::string> local_cache_path,
      const char* caller,
      fuse_ino_t parent,
      const char* name);
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
  void lookup(fuse_req_t req, fuse_ino_t parent, const char* name);
  static void lookupCallback(
      struct rpc_context*,
      int rpc_status,
      void* data,
      void* private_data);
  void open(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file);
  void readWithContext(ReadRpcContext* ctx);
  virtual void read(
      fuse_req_t req,
      fuse_ino_t inode,
      size_t size,
      off_t off,
      struct fuse_file_info* file);
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
  void statfs(fuse_req_t req, fuse_ino_t inode);
  void access(fuse_req_t req, fuse_ino_t inode, int mask);

  uint64_t getMaxRead() {
    return maxRead_;
  }

  std::shared_ptr<nfusr::Logger> getLogger() {
    return logger_;
  }

 private:
  NfsConnectionPool connPool_;
  InodeInternal* rootInode_;
  uint64_t maxRead_;
  uint64_t maxWrite_;
  std::shared_ptr<nfusr::Logger> logger_;
  bool errorInjection_;
  std::unique_ptr<ClientStats> stats_;
};
