/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "NfsCachedClient.h"
#include "NfsClient.h"
#include "fuse_optype.h"

/// @brief Holds information regarding a RPC call.
///
/// All RpcContexts have a NfsClient and a fuse_req. Any call
/// which takes more parameters must specialize this class.
///
/// General usage pattern: a RpcContext is created in response
/// to a FUSE request, and captures the parameters used in the
/// generation of the RPC request. Once the caller has prepared
/// the RPC request parameters, they ought call obtainConnection()
/// to get a connection to the server. If this succeeds, they may
/// then call rpc_nfs_* using getRpcCtx() as the context parameter,
/// and must thereafter unlock the connection using unlockConnection()
/// (note that obtainConnection() does an implicit lock). When the
/// call later completes, the FUSE request is completed by calling
/// one of the reply* member functions. Since this event marks the
/// end of the RPC call and it is an error to touch the object again
/// (the fuse request, in particular, has been magically deallocated
/// by libfuse) the reply* methods delete the context object.
///
/// A note on pointer parameters: you may note that / we do deep copy,
/// duplicating string parameters with strdup and  using internal
/// storage for fixed-size things like fuse_file_info.
/// This seems inefficient but is important. We are using the FUSE
/// low-level, async interface. In this interface, FUSE
/// calls us with some parameter. We initiate an async RPC
/// operation and return. FUSE is then free to deallocate the
/// object. However, if the operation fails, we may choose to
/// restart it on another server, in which case we need the parameter
/// again. Using the old, possibly deallocated pointer is a recipe
/// for woe.

class CacheBlock {
 public:
  CacheBlock(std::shared_ptr<std::string> path, size_t size, off_t offset)
      : data_(static_cast<int>(size)) {
    path_ = path;
    size_ = size;
    offset_ = offset;
  }
  ~CacheBlock(){};
  bool readCache();
  const char* getData() {
    return (const char*)data_.data();
  }
  ssize_t getSize() {
    return size_;
  }

 private:
  std::shared_ptr<std::string> path_;
  size_t size_;
  off_t offset_;
  std::vector<uint8_t> data_;
};

class RpcContext {
 public:
  RpcContext(NfsClient* client, struct fuse_req* req, enum fuse_optype optype) {
    client_ = client;
    req_ = req;
    optype_ = optype;
    if (client->statsEnabled()) {
      stats_.start_ = std::chrono::steady_clock::now();
      client->recordOperationIssue();
    }
    conn_ = nullptr;
    errnoRetries_ = 0;
  }
  virtual ~RpcContext(){};

  static void setMaxErrnoRetries(unsigned maxErrnoRetries) {
    maxErrnoRetries_ = maxErrnoRetries;
  }

  static unsigned getMaxErrnoRetries() {
    return maxErrnoRetries_;
  }

  /// @brief obtain a connection to a NFS server for this call.
  ///
  /// NB: for the caller's convenience, the returned connection
  /// is locked with NfsConnection::get. The caller must unlock
  /// the connection via ::unlockConnection after initiating the
  /// call.
  bool obtainConnection() {
    while ((conn_ = client_->getConnectionPool().get()) != nullptr) {
      conn_->get();
      if (!conn_->closed()) {
        return true;
      }
      client_->getConnectionPool().failed(conn_);
      conn_->put();
    }
    return false;
  }

  void unlockConnection() {
    assert(conn_ != nullptr);
    conn_->put();
  }

  bool hasConnection() const {
    return conn_ != nullptr;
  }

  void failConnection() {
    assert(conn_ != nullptr);
    NfsConnectionPool& pool = client_->getConnectionPool();
    pool.failed(conn_);
    conn_ = nullptr;
  }

  /// @brief Check RPC completion for success.
  ///
  /// On failure, retry is set true if the error is
  /// retryable; on non-retryable error, the FUSE request
  /// is completed in error and as a side effect, the
  /// RpcContext object is destroyed.
  bool succeeded(
      int rpc_status,
      int nfs_status,
      bool& retry,
      bool idempotent = true) {
    retry = false;
    if (rpc_status != RPC_STATUS_SUCCESS) {
      client_->getLogger()->LOG_MSG(
          LOG_WARNING,
          "%s: RPC status %d (%lu).\n",
          conn_->describe().c_str(),
          rpc_status,
          fuse_get_unique(req_));
      client_->recordRpcFailure(rpc_status == RPC_STATUS_TIMEOUT);
      failConnection();
      retry = true;
      return false;
    }

    if (client_->errorInjection() && idempotent) {
      client_->getLogger()->LOG_MSG(
          LOG_DEBUG,
          "%s: simulating failure for %lu.\n",
          __func__,
          fuse_get_unique(req_));
      failConnection();
      retry = true;
      return false;
    }

    if (client_->errorInjection() && idempotent) {
      // TODO Inject more random errors
      nfs_status = NFS3ERR_ROFS;
    }

    if (nfs_status != NFS3_OK) {
      if (idempotent && errnoRetries_ < maxErrnoRetries_ &&
          isRetryableError(nfs_status)) {
        errnoRetries_++;
        client_->getLogger()->LOG_MSG(
            LOG_INFO,
            "%s: Retrying request %lu (attempt %u/%u).\n",
            __func__,
            fuse_get_unique(req_),
            errnoRetries_,
            maxErrnoRetries_);
        failConnection();
        retry = true;
        return false;
      } else {
        if (idempotent && errnoRetries_ >= maxErrnoRetries_) {
          client_->getLogger()->LOG_MSG(
              LOG_INFO,
              "%s: Max retry attempts reached, failing operation (%u/%u).\n",
              __func__,
              errnoRetries_,
              maxErrnoRetries_);
        } else if (idempotent && !isRetryableError(nfs_status)) {
          client_->getLogger()->LOG_MSG(
              LOG_INFO,
              "%s: Error #%d is not retryable.\n",
              __func__,
              nfs_status);
        }
      }

      replyError(-nfsstat3_to_errno(nfs_status));
      return false; // error occurred.
    }
    return true; // success.
  }

  virtual void logError(int rc) {
    getClient()->getLogger()->LOG_MSG(
        errnoLoglevel(rc),
        "Operation %s failed: %s\n",
        fuse_optype_name(optype_),
        strerror(rc));
  }

  void replyError(int rc) {
    if (rc) {
      logError(rc);
    }
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu): %d\n", __func__, fuse_get_unique(req_), rc);
    fuse_reply_err(req_, rc);
    delete this;
  }

  virtual void replyBuf(const void* buf, size_t size) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu): %lu\n", __func__, fuse_get_unique(req_), size);
    fuse_reply_buf(req_, (const char*)buf, size);
    delete this;
  }

  void replyEntry(const struct fuse_entry_param* e) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req_));
    fuse_reply_entry(req_, e);
    delete this;
  }

  void replyCreate(
      const struct fuse_entry_param* e,
      const struct fuse_file_info* f) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req_));
    fuse_reply_create(req_, e, f);
    delete this;
  }

  void replyOpen(const struct fuse_file_info* f) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req_));
    fuse_reply_open(req_, f);
    delete this;
  }

  void replyAttr(const struct stat* attr, double attr_timeout) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req_));
    fuse_reply_attr(req_, attr, attr_timeout);
    delete this;
  }

  void replyWrite(size_t count) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu): %lu\n", __func__, fuse_get_unique(req_), count);
    fuse_reply_write(req_, count);
    delete this;
  }

  void replyReadlink(const char* linkname) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu): %s\n", __func__, fuse_get_unique(req_), linkname);
    fuse_reply_readlink(req_, linkname);
    delete this;
  }

  void replyStatfs(const struct statvfs* stbuf) {
    finishOperation();
    client_->getLogger()->LOG_MSG(
        LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req_));
    fuse_reply_statfs(req_, stbuf);
    delete this;
  }

  NfsClient* getClient() const {
    return client_;
  }

  struct fuse_req* getReq() const {
    return req_;
  }

  std::shared_ptr<NfsConnection> const getConn() {
    return conn_;
  }

  struct nfs_context* getNfsCtx() const {
    assert(hasConnection());
    return conn_->getNfsCtx();
  }

  struct rpc_context* getRpcCtx() const {
    assert(hasConnection());
    return nfs_get_rpc_context(conn_->getNfsCtx());
  }

  /// @brief return proper logging level for given errno.
  static int errnoLoglevel(int rc) {
    // ENOENT swamps logs in some workloads.
    return rc == ENOENT ? LOG_INFO : LOG_ERR;
  }

  bool isRetry() const {
    return errnoRetries_ > 0;
  }

 protected:
  enum fuse_optype optype_;

 private:
  void finishOperation() {
    if (client_->statsEnabled()) {
      auto end = std::chrono::steady_clock::now();
      auto delta = std::chrono::duration_cast<std::chrono::microseconds>(
          end - stats_.start_);

      client_->recordOperationStats(optype_, delta);
    }
  }

  bool isRetryableError(int nfs_status) {
    switch (nfs_status) {
      case NFS3ERR_IO:
      case NFS3ERR_SERVERFAULT:
      case NFS3ERR_ROFS:
      case NFS3ERR_PERM:
        return true;
      default:
        return false;
    }
  }

  // Track number of retries for NFS errors
  unsigned errnoRetries_;
  // Maximum number of times to retry
  static unsigned maxErrnoRetries_;

  NfsClient* client_;
  struct fuse_req* req_;
  struct {
    std::chrono::time_point<std::chrono::steady_clock> start_;
  } stats_;
  std::shared_ptr<NfsConnection> conn_;
};

/// @brief base class for operations which take an inode.
class RpcContextInode : public RpcContext {
 public:
  RpcContextInode(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode)
      : RpcContext(client, req, optype) {
    inode_ = inode;
  }
  fuse_ino_t getInode() const {
    return inode_;
  }

  void logError(int rc) override {
    this->getClient()->getLogger()->LOG_MSG(
        errnoLoglevel(rc),
        "Operation %s failed on inode %lu: %s\n",
        fuse_optype_name(optype_),
        inode_,
        strerror(rc));
  }

 private:
  fuse_ino_t inode_;
};

/// @brief base class for operations which take an inode and a file.
class RpcContextInodeFile : public RpcContextInode {
 public:
  RpcContextInodeFile(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      fuse_file_info* file)
      : RpcContextInode(client, req, optype, inode) {
    if (file) {
      ::memcpy(&file_, file, sizeof(file_));
      filePtr_ = &file_;
    } else {
      filePtr_ = nullptr;
    }
  }
  fuse_file_info* getFile() {
    return filePtr_;
  }

 private:
  fuse_file_info file_;
  fuse_file_info* filePtr_;
};

/// @brief base class for operations which take a parent inode and a name.
class RpcContextParentName : public RpcContext {
 public:
  RpcContextParentName(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t parent,
      const char* name)
      : RpcContext(client, req, optype) {
    parent_ = parent;
    name_ = ::strdup(name);
  }
  ~RpcContextParentName() override {
    ::free((void*)name_);
  }
  fuse_ino_t getParent() const {
    return parent_;
  }
  const char* getName() const {
    return name_;
  }

  void logError(int rc) override {
    this->getClient()->getLogger()->LOG_MSG(
        errnoLoglevel(rc),
        "Operation %s failed on %lu/%s : %s\n",
        fuse_optype_name(optype_),
        parent_,
        name_,
        strerror(rc));
  }

 private:
  fuse_ino_t parent_;
  const char* name_;
};

class ReadRpcContext : public RpcContextInodeFile {
 public:
  ReadRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      fuse_file_info* file,
      size_t size,
      off_t off)
      : RpcContextInodeFile(client, req, optype, inode, file) {
    size_ = size;
    off_ = off;
  }
  size_t getSize() const {
    return size_;
  }
  off_t getOff() const {
    return off_;
  }

 private:
  size_t size_;
  off_t off_;
};

class CachedReadRpcContext : public ReadRpcContext {
 public:
  CachedReadRpcContext(
      NfsCachedClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      fuse_file_info* file,
      size_t size,
      off_t off)
      : ReadRpcContext(client, req, optype, inode, file, size, off) {
    cachedClient_ = client;
  }
  void replyBuf(const void* buf, size_t size) override {
    auto inode = this->getInode();
    auto inodeObj = this->getClient()->inodeLookup(inode);
    if (inodeObj->hasLocalCachePath()) {
      auto path = inodeObj->getLocalCachePath();
      auto requestSize = getSize();
      auto off = getOff();
      this->getClient()->getLogger()->LOG_MSG(
          LOG_DEBUG,
          "%s Attempting to write %lu to cache file %s.\n",
          __func__,
          inode,
          path->c_str());
      this->getCachedClient()->writeCache(
          path, requestSize, off, (const char*)buf, size);
    }
    ReadRpcContext::replyBuf(buf, size);
  }
  NfsCachedClient* getCachedClient() const {
    return cachedClient_;
  }

 private:
  NfsCachedClient* cachedClient_;
};
