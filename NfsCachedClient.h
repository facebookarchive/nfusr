/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "NfsClient.h"
#include <unistd.h>
#define CACHEBLOCKSIZE 1024

struct CacheBlock;

class NfsCachedClient : public NfsClient {
 public:
  NfsCachedClient(
      std::vector<std::string>& urls,
      const char* hostscript,
      const int script_refresh_seconds,
      size_t targetConnections,
      int nfsTimeout,
      std::shared_ptr<nfusr::Logger> logger,
      std::shared_ptr<ClientStats> stats,
      bool errorInjection,
      NfsClientPermissionMode permMode)
      : NfsClient(
            urls,
            hostscript,
            script_refresh_seconds,
            targetConnections,
            nfsTimeout,
            logger,
            stats,
            errorInjection,
            permMode) {}
  ~NfsCachedClient() override;

  void replyEntry(
      RpcContext* ctx,
      const nfs_fh3* fh,
      const struct fattr3* attr,
      const struct fuse_file_info* file,
      std::shared_ptr<std::string> local_cache_path,
      const char* caller,
      fuse_ino_t parent,
      const char* name) override;
  void read(
      fuse_req_t req,
      fuse_ino_t inode,
      size_t size,
      off_t off,
      struct fuse_file_info* file) override;
  std::unique_ptr<CacheBlock> readCache(
      std::shared_ptr<std::string> data_fname,
      size_t size,
      off_t offset);
  void writeCache(
      std::shared_ptr<std::string> data_fname,
      size_t size,
      off_t offset,
      const char* data,
      u_int data_len);
};
