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
    }
    conn_ = nullptr;
  }
  virtual ~RpcContext(){};

  /// @brief obtain a connection to a NFS server for this call.
  ///
  /// NB: for the caller's convenience, the returned connection
  /// is locked with NfsConnection::get. The caller must unlock
  /// the connection via ::unlockConnection after initiating the
  /// call.
  bool obtainConnection() {
    if ((conn_ = client_->getConnectionPool().get()) != nullptr) {
      conn_->get();
      return true;
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

  bool shouldRetryCall(int rpc_status) {
    if (!hasConnection()) {
      // We were unable to obtain any connection. We have
      // tried all the possibilities, so this is non-retryable.
      replyError(EHOSTUNREACH);
      return false;
    }

    if (rpc_status != RPC_STATUS_SUCCESS) {
      client_->getLogger()->LOG_MSG(
          LOG_WARNING,
          "%s: RPC status %d.\n",
          conn_->describe().c_str(),
          rpc_status);
      failConnection();
      return true;
    }

    return false;
  }

  /// @brief Check RPC completion for success.
  ///
  /// On failure, retry is set true if the error is
  /// retryable; on non-retryable error, the FUSE request
  /// is completed in error and as a side effect, the
  /// RpcContext object is destroyed.
  bool succeeded(int rpc_status, int nfs_status, bool& retry) {
    retry = false;
    if (rpc_status != RPC_STATUS_SUCCESS) {
      client_->getLogger()->LOG_MSG(
          LOG_WARNING,
          "%s: RPC status %d.\n",
          conn_->describe().c_str(),
          rpc_status);
      failConnection();
      retry = true;
      return false;
    }

    if (client_->getErrorInjection()) {
      // Periodically simulate RPC failure, which exercises the
      // failover code.
      static int counter = 0;
      if (++counter % 100 == 0) {
        client_->getLogger()->LOG_MSG(
            LOG_DEBUG, "%s: simulating failure.\n", __func__);
        failConnection();
        retry = true;
        return false;
      }
    }

    if (nfs_status != NFS3_OK) {
      replyError(-nfsstat3_to_errno(nfs_status));
      return false; // error occurred.
    }
    return true; // success.
  }

  virtual void logError(int rc) {
    this->getClient()->getLogger()->LOG_MSG(
        LOG_ERR,
        "Operation %s failed: %s\n",
        fuse_optype_name(optype_),
        strerror(rc));
  }

  void replyError(int rc) {
    if (rc) {
      logError(rc);
    }
    finishOperation();
    fuse_reply_err(req_, rc);
    delete this;
  }

  virtual void replyBuf(const void* buf, size_t size) {
    finishOperation();
    fuse_reply_buf(req_, (const char*)buf, size);
    delete this;
  }

  void replyEntry(const struct fuse_entry_param* e) {
    finishOperation();
    fuse_reply_entry(req_, e);
    delete this;
  }

  void replyCreate(
      const struct fuse_entry_param* e,
      const struct fuse_file_info* f) {
    finishOperation();
    fuse_reply_create(req_, e, f);
    delete this;
  }

  void replyOpen(const struct fuse_file_info* f) {
    finishOperation();
    fuse_reply_open(req_, f);
    delete this;
  }

  void replyAttr(const struct stat* attr, double attr_timeout) {
    finishOperation();
    fuse_reply_attr(req_, attr, attr_timeout);
    delete this;
  }

  void replyWrite(size_t count) {
    finishOperation();
    fuse_reply_write(req_, count);
    delete this;
  }

  void replyReadlink(const char* linkname) {
    finishOperation();
    fuse_reply_readlink(req_, linkname);
    delete this;
  }

  void replyStatfs(const struct statvfs* stbuf) {
    finishOperation();
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

  struct rpc_context* getRpcCtx() {
    assert(hasConnection());
    return nfs_get_rpc_context(conn_->getNfsCtx());
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

  virtual void logError(int rc) {
    this->getClient()->getLogger()->LOG_MSG(
        LOG_ERR,
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
  virtual ~RpcContextParentName() {
    ::free((void*)name_);
  }
  fuse_ino_t getParent() const {
    return parent_;
  }
  const char* getName() const {
    return name_;
  }

  virtual void logError(int rc) {
    this->getClient()->getLogger()->LOG_MSG(
        LOG_ERR,
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
  virtual void replyBuf(const void* buf, size_t size) {
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
