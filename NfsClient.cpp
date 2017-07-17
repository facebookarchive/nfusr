/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NfsClient.h"
#include "RpcContext.h"
#include "logger.h"

#include <limits.h>
#include <nfsc/libnfs.h>
#include <cassert>
#include <chrono>
#include <cstring>

double NfsClient::attrTimeout_ = 0.0;

/// @brief translate a NFS fattr3 into struct stat.
void NfsClient::stat_from_fattr3(struct stat* st, const struct fattr3* attr) {
  ::memset(st, 0, sizeof(*st));
  st->st_dev = attr->fsid;
  st->st_ino = attr->fileid;
  st->st_mode = attr->mode;
  st->st_nlink = attr->nlink;
  st->st_uid = attr->uid;
  st->st_gid = attr->gid;
  st->st_rdev = makedev(attr->rdev.specdata1, attr->rdev.specdata2);
  st->st_size = attr->size;
  st->st_blksize = NFS_BLKSIZE;
  st->st_blocks = (attr->used + 511) >> 9;
  st->st_atim.tv_sec = attr->atime.seconds;
  st->st_atim.tv_nsec = attr->atime.nseconds;
  st->st_mtim.tv_sec = attr->mtime.seconds;
  st->st_mtim.tv_nsec = attr->mtime.nseconds;
  st->st_ctim.tv_sec = attr->ctime.seconds;
  st->st_ctim.tv_nsec = attr->ctime.nseconds;
  switch (attr->type) {
    case NF3REG:
      st->st_mode |= S_IFREG;
      break;
    case NF3DIR:
      st->st_mode |= S_IFDIR;
      break;
    case NF3BLK:
      st->st_mode |= S_IFBLK;
      break;
    case NF3CHR:
      st->st_mode |= S_IFCHR;
      break;
    case NF3LNK:
      st->st_mode |= S_IFLNK;
      break;
    case NF3SOCK:
      st->st_mode |= S_IFSOCK;
      break;
    case NF3FIFO:
      st->st_mode |= S_IFIFO;
      break;
  }
}

/// @brief add a new inode for the given fh and pass to fuse_reply_entry().
void NfsClient::replyEntry(
    RpcContext* ctx,
    const nfs_fh3* fh,
    const struct fattr3* attr,
    const struct fuse_file_info* file,
    std::shared_ptr<std::string> local_cache_path,
    // following parameters are purely for debugs.
    const char* caller = nullptr,
    fuse_ino_t parent = 0,
    const char* name = nullptr) {
  InodeInternal* ii;
  if (fh) {
    ii = new InodeInternal(fh, local_cache_path);
  } else {
    ii = nullptr;
  }

  if (caller && name) {
    ctx->getClient()->getLogger()->LOG_MSG(
        LOG_DEBUG,
        "%s allocated new inode %p for %lu+%s.\n",
        caller,
        ii,
        parent,
        name);
  }

  fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  stat_from_fattr3(&e.attr, attr);
  e.ino = (fuse_ino_t)(uintptr_t)ii;
  e.attr_timeout = attrTimeout_;
  e.entry_timeout = attrTimeout_;

  if (file) {
    ctx->replyCreate(&e, file);
  } else {
    ctx->replyEntry(&e);
  }
}

#define REPLY_ENTRY(fh, attr, file) \
  ctx->getClient()->replyEntry(     \
      ctx,                          \
      &(fh),                        \
      &(attr),                      \
      (file),                       \
      nullptr,                      \
      __func__,                     \
      ctx->getParent(),             \
      ctx->getName())

// libnfs can return a null result pointer on RPC errors
// (specifically timeouts). Convenience macro to extract
// result value from a possibly NULL result pointer.
#define RSTATUS(r) ((r) ? (r)->status : NFS3ERR_SERVERFAULT)

NfsClient::NfsClient(
    std::vector<std::string>& urls,
    const char* hostscript,
    const int script_refresh_seconds,
    size_t targetConnections,
    int nfsTimeoutMs,
    std::shared_ptr<nfusr::Logger> logger,
    std::shared_ptr<ClientStats> stats,
    bool errorInjection,
    NfsClientPermissionMode permMode)
    : connPool_(
          urls,
          hostscript,
          script_refresh_seconds,
          logger,
          stats,
          std::min(urls.size(), targetConnections),
          nfsTimeoutMs),
      logger_(logger),
      stats_(stats),
      permMode_(permMode),
      initial_uid_(getuid()),
      initial_gid_(getgid()) {
  if (errorInjection) {
    initErrorInjection();
  } else {
    eiMin_ = eiMax_ = 0;
  }
}

NfsClient::~NfsClient() {
  if (rootInode_) {
    rootInode_->deref();
  }
}

/// @brief get the InodeInternal corresponding to a particular fuse_ino_t.
InodeInternal* NfsClient::inodeLookup(fuse_ino_t i) {
  if (i == FUSE_ROOT_ID) {
    return rootInode_;
  }

  // All inodes other than 1 (FUSE_ROOT_ID) are actually pointers.
  //
  // Gross and scary if somebody passes us a bad inode, but very
  // efficient. And hey, we're in userspace!
  return (InodeInternal*)(uintptr_t)i;
}

extern "C" {
// libnfs does not offer a prototype for this in any public header,
// but mercifully exports it anyway.
const struct nfs_fh3* nfs_get_rootfh(struct nfs_context* nfs);
}

/// @brief start a NFS connection and get some global state from the server.
bool NfsClient::start(std::shared_ptr<std::string> cacheRoot) {
  auto conn = connPool_.get();
  if (conn) {
    auto nfs_ctx = conn->getNfsCtx();
    rootInode_ = new InodeInternal(nfs_get_rootfh(nfs_ctx), cacheRoot);
    maxRead_ = nfs_get_readmax(nfs_ctx);
    maxWrite_ = nfs_get_writemax(nfs_ctx);
    return true;
  }
  return false;
}

/// @brief Test if an RPC operation should be retried.
///
/// If the operation failed to send and we actually have a connection
/// to a server, retry.
///
/// This does *not* apply to RPCs that have been sent and need retry
/// due to a failure on the server side. For that,
/// see succeeded() in RpcContext.h
///
/// In case where no server connection is possible, completes the
/// FUSE request in error.
bool NfsClient::shouldRetry(int rpc_status, RpcContext* ctx) {
  if (rpc_status != RPC_STATUS_SUCCESS) {
    if (!ctx->hasConnection()) {
      // We were unable to obtain any connection. We have
      // tried all the possibilities, so this is non-retryable.
      ctx->replyError(EHOSTUNREACH);
      return false;
    }

    logger_->LOG_MSG(
        LOG_WARNING,
        "%s: RPC status %d (%ld).\n",
        ctx->getConn()->describe().c_str(),
        rpc_status,
        fuse_get_unique(ctx->getReq()));
    ctx->failConnection();
    return true;
  }

  return false;
}

void NfsClient::setUidGid(RpcContext const& ctx, bool creat) {
  switch (permMode_) {
    case InitialPerms:
      break;
    case SloppyPerms:
      if (creat && !ctx.isRetry()) {
        auto rpcCtx = ctx.getRpcCtx();
        auto fuseCtx = fuse_req_ctx(ctx.getReq());
        rpc_set_uid(rpcCtx, fuseCtx->uid);
        rpc_set_gid(rpcCtx, fuseCtx->gid);
      }
      break;
    case StrictPerms: {
      auto rpcCtx = ctx.getRpcCtx();
      auto fuseCtx = fuse_req_ctx(ctx.getReq());
      rpc_set_uid(rpcCtx, fuseCtx->uid);
      rpc_set_gid(rpcCtx, fuseCtx->gid);
    } break;
  }
}

void NfsClient::restoreUidGid(RpcContext const& ctx, bool creat) {
  switch (permMode_) {
    case InitialPerms:
      break;
    case StrictPerms:
      break;
    case SloppyPerms:
      if (creat) {
        auto nfsCtx = ctx.getNfsCtx();
        nfs_set_uid(nfsCtx, initial_uid_);
        nfs_set_gid(nfsCtx, initial_gid_);
      }
      break;
  }
}

/// Here starts FUSE operations.
///
/// General structure of this code: every FUSE operation is implemented by a
/// public method with the same signature as FUSE (e.g. NfsClient::open).
///
/// These public methods allocate a RpcContext object to manage the
/// lifetime of the FUSE request. All parameters to the call are stashed in
/// the RpcContext object (which is specialized as necessary for the
/// particular call; e.g. ReadRpcContext).
///
/// The public method now calls a ...WithContext method (e.g.
/// NfsClient::openWithContext). This makes obtains a connection to a NFS
/// server and issues an async call to libnfs, passing the RpcContext object
/// as context (incidentally, this is why RpcContext instances are managed
/// as raw pointers instead of smart pointers: for some of the instance
/// lifetime, the only live reference is a pointer being held by libnfs).
///
/// Operation resumes in the callback. In the event of failure, the callback
/// may failover and re-call the original method on a new NFS connection
/// (this is why the RpcContext must include all the original parameters,
/// even if they're not useful in the success path).
//
/// In case of success the callback calls the proper fuse_reply* method
/// and frees the RpcContext.
///
/// So each implemented FUSE operation has four three pieces of code:
///
/// 1) (optional): the specialization of RpcContext for the set of
///    parameters accepted by the operation (there are some common
///    signatures implemented in RpcContext.h, which is why this
///    is optional).
///
///    Example: ReadRpcContext.
///
/// 2) the callback. All callbacks have a common signature as provided
///    by libnfs, and contain boilerplate to failover, plus specialized
///    code to call the proper fuse_reply method. Note that a callback
///    is a simple C function. We could use a wrapper to make it a member
///    of NfsClient, but instead just access the NfsClient instance via
///    the getClient() method of RpcContext,
///
///    Example: readCallback().
///
/// 3) the ..WithContext method, which issues the async NFS request, passing
///    the callback and the context.
///
///    Example: readWithContext().
///
/// 4) the public interface. This has the same signature as the
///    corresponding FUSE operation. It allocates the RpcContext,
///    initiates the operation, and handles failover.
///
///    Example: read().

static void getattrCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* privateData) {
  auto ctx = (RpcContextInodeFile*)privateData;
  auto res = (GETATTR3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    struct stat st;
    ctx->getClient()->stat_from_fattr3(
        &st, &res->GETATTR3res_u.resok.obj_attributes);
    ctx->replyAttr(&st, NfsClient::getAttrTimeout());
  } else if (retry) {
    ctx->getClient()->getattrWithContext(ctx);
  }
}

void NfsClient::getattrWithContext(RpcContextInodeFile* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto inode = ctx->getInode();
  do {
    struct GETATTR3args args;
    ::memset(&args, 0, sizeof(args));
    args.object = inodeLookup(inode)->getFh();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_getattr_async(ctx->getRpcCtx(), getattrCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::getattr(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* file) {
  auto ctx = new RpcContextInodeFile(this, req, FOPTYPE_GETATTR, inode, file);
  getattrWithContext(ctx);
}

namespace {
/// @brief a growable buffer which holds READDIRPLUS results.
class FuseDirBuffer {
 public:
  FuseDirBuffer() {
    buffer_ = nullptr;
    size_ = 0;
  }
  FuseDirBuffer(FuseDirBuffer&& o) noexcept {
    buffer_ = o.buffer_;
    size_ = o.size_;
    o.buffer_ = nullptr;
    o.size_ = 0;
  }
  FuseDirBuffer& operator=(FuseDirBuffer&& o) noexcept {
    buffer_ = o.buffer_;
    size_ = o.size_;
    o.buffer_ = nullptr;
    o.size_ = 0;
    return *this;
  }
  ~FuseDirBuffer() {
    ::free(buffer_);
  }
  char* grow(size_t bytes) {
    size_t oldSize = size_;
    size_ += bytes;
    buffer_ = (char*)::realloc(buffer_, size_);
    return buffer_ ? buffer_ + oldSize : nullptr;
  }

  size_t getSize() const {
    return size_;
  }

  char* getBuffer() const {
    return buffer_;
  }

 private:
  char* buffer_;
  size_t size_;
};

class OpendirRpcContext : public RpcContextInodeFile {
 public:
  OpendirRpcContext(
      NfsClient* client,
      fuse_req_t req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      struct fuse_file_info* file)
      : RpcContextInodeFile(client, req, optype, inode, file) {
    cookie_ = 0;
    ::memset(&cookieverf_, 0, sizeof(cookieverf_));
    ii_ = client->inodeLookup(inode);
    ii_->ref();
  }

  ~OpendirRpcContext() override {
    ii_->deref(getClient()->getLogger().get(), __func__);
  }

  InodeInternal* getInternalInode() const {
    return ii_;
  }

  cookie3 getCookie() const {
    return cookie_;
  }
  void setCookie(cookie3 cookie) {
    cookie_ = cookie;
  }
  const cookieverf3* getCookieverf() const {
    return &cookieverf_;
  }
  void setCookieverf(const cookieverf3* cookieverf) {
    ::memcpy(&cookieverf_, cookieverf, sizeof(cookieverf_));
  }
  FuseDirBuffer& getFuseDirBuf() {
    return fuseDirBuf_;
  }

 private:
  InodeInternal* ii_;
  cookie3 cookie_;
  cookieverf3 cookieverf_;
  FuseDirBuffer fuseDirBuf_;
};
} // anonymous namespace

static void opendirContinue(OpendirRpcContext* ctx, bool have_connection);

static void opendirCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (OpendirRpcContext*)private_data;
  auto res = (READDIRPLUS3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    struct entryplus3* entry = res->READDIRPLUS3res_u.resok.reply.entries;
    while (entry) {
      auto newEntrySize =
          fuse_add_direntry(ctx->getReq(), nullptr, 0, entry->name, nullptr, 0);
      auto fuseEntry = ctx->getFuseDirBuf().grow(newEntrySize);
      struct stat st;

     if (entry->name_attributes.attributes_follow) {
       ctx->getClient()->stat_from_fattr3(
           &st, &entry->name_attributes.post_op_attr_u.attributes);
     } else {
         ::memset(&st, 0, sizeof(st));
     }

      fuse_add_direntry(
          ctx->getReq(),
          fuseEntry,
          newEntrySize,
          entry->name,
          &st,
          ctx->getFuseDirBuf().getSize());

      ctx->setCookie(entry->cookie);
      entry = entry->nextentry;
    }

    if (!res->READDIRPLUS3res_u.resok.reply.eof) {
      ctx->setCookieverf(&res->READDIRPLUS3res_u.resok.cookieverf);
      opendirContinue(ctx, true);
    } else {
      FuseDirBuffer* buff = new FuseDirBuffer(std::move(ctx->getFuseDirBuf()));
      ctx->getFile()->fh = (uint64_t)(uintptr_t)buff;
      ctx->getFile()->direct_io = 0;
      ctx->getFile()->keep_cache = 0;
      ctx->getFile()->nonseekable = 0;
      ctx->replyOpen(ctx->getFile());
    }
  } else if (retry) {
    opendirContinue(ctx, false);
  }
}

static void opendirContinue(OpendirRpcContext* ctx, bool have_connection) {
  int rpc_status = RPC_STATUS_ERROR;
  auto client = ctx->getClient();

  do {
    struct READDIRPLUS3args args;
    ::memset(&args, 0, sizeof(args));
    args.dir = ctx->getInternalInode()->getFh();
    args.cookie = ctx->getCookie();
    ::memcpy(&args.cookieverf, ctx->getCookieverf(), sizeof(args.cookieverf));
    args.dircount = 65536;
    args.maxcount = 65536;

    if (have_connection || ctx->obtainConnection()) {
      rpc_status = rpc_nfs3_readdirplus_async(
          ctx->getRpcCtx(), opendirCallback, &args, ctx);
      if (!have_connection) {
        ctx->unlockConnection();
      } else {
        have_connection = false;
      }
    }
  } while (client->shouldRetry(rpc_status, ctx));
}

void NfsClient::opendir(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* file) {
  auto ctx = new OpendirRpcContext(this, req, FOPTYPE_OPENDIR, inode, file);

  opendirContinue(ctx, false);
}

void NfsClient::readdir(
    fuse_req_t req,
    fuse_ino_t /* inode */,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto buff = (FuseDirBuffer*)(uintptr_t)file->fh;

  if ((size_t)off < buff->getSize()) {
    const size_t toSend = std::min(buff->getSize() - off, size);
    logger_->LOG_MSG(LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req));
    fuse_reply_buf(req, buff->getBuffer() + off, toSend);
  } else {
    logger_->LOG_MSG(LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req));
    fuse_reply_buf(req, nullptr, 0);
  }
}

void NfsClient::releasedir(
    fuse_req_t req,
    fuse_ino_t /* inode */,
    struct fuse_file_info* file) {
  auto buff = (FuseDirBuffer*)(uintptr_t)file->fh;
  file->fh = 0;
  delete buff;
  logger_->LOG_MSG(LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req));
  fuse_reply_err(req, 0);
}

void NfsClient::lookupCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextParentName*)private_data;
  auto res = (LOOKUP3res*)data;
  bool retry;

  if (rpc_status == RPC_STATUS_SUCCESS && RSTATUS(res) == NFS3ERR_NOENT) {
    // Magic special case for fuse: if we want negative cache, we
    // must not return ENOENT, must instead return success with zero inode.
    //
    // See comments in definition of fuse_entry_param in fuse header.
    struct fattr3 dummyAttr;
    ::memset(&dummyAttr, 0, sizeof(dummyAttr));
    ctx->getClient()->getLogger()->LOG_MSG(
        LOG_DEBUG,
        "Negative caching failed lookup req (%lu).\n",
        fuse_get_unique(ctx->getReq()));
    ctx->getClient()->replyEntry(
        ctx,
        nullptr /* fh */,
        // &res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes,
        &dummyAttr,
        nullptr, /* file */
        nullptr, /* local_cache_path */
        __func__,
        ctx->getParent(),
        ctx->getName());
  } else if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    assert(res->LOOKUP3res_u.resok.obj_attributes.attributes_follow);

    REPLY_ENTRY(
        res->LOOKUP3res_u.resok.object,
        res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes,
        nullptr);
  } else if (retry) {
    ctx->getClient()->lookupWithContext(ctx);
  }
}

void NfsClient::lookupWithContext(RpcContextParentName* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto parent = ctx->getParent();
  do {
    LOOKUP3args args;
    ::memset(&args, 0, sizeof(args));
    args.what.dir = inodeLookup(parent)->getFh();
    args.what.name = (char*)ctx->getName();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status = rpc_nfs3_lookup_async(
          ctx->getRpcCtx(), this->lookupCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto ctx = new RpcContextParentName(this, req, FOPTYPE_LOOKUP, parent, name);
  lookupWithContext(ctx);
}

void NfsClient::forget(
    fuse_req_t req,
    fuse_ino_t inode,
    unsigned long nlookup) {
  auto ii = inodeLookup(inode);
  while (nlookup--) {
    ii->deref(logger_.get(), __func__);
  }
  logger_->LOG_MSG(LOG_DEBUG, "%s(%lu)\n", __func__, fuse_get_unique(req));
  fuse_reply_none(req);
}

static void openCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextInodeFile*)private_data;
  auto client = ctx->getClient();
  auto res = (ACCESS3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    auto file = ctx->getFile();
    file->fh = ctx->getInode();
    file->direct_io = 0;
    file->keep_cache = 0;
    file->nonseekable = 0;

    ctx->replyOpen(file);
  } else if (retry) {
    client->openWithContext(ctx);
  }
}

void NfsClient::openWithContext(RpcContextInodeFile* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto inode = ctx->getInode();
  auto file = ctx->getFile();
  do {
    ACCESS3args args;

    ::memset(&args, 0, sizeof(args));
    args.object = inodeLookup(inode)->getFh();
    if (file->flags & O_WRONLY) {
      args.access = ACCESS3_MODIFY;
    } else if (file->flags & O_RDWR) {
      args.access = ACCESS3_READ | ACCESS3_MODIFY;
    } else {
      args.access = ACCESS3_READ;
    }

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_access_async(ctx->getRpcCtx(), openCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::open(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* file) {
  auto ctx = new RpcContextInodeFile(this, req, FOPTYPE_OPEN, inode, file);
  openWithContext(ctx);
}

void NfsClient::release(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* file) {
  // It's not strictly necessary to sync on close, but it sure seems like good
  // hygiene.
  fsync(req, inode, 0, file);
}

static void readCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (ReadRpcContext*)private_data;
  auto res = (READ3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    ctx->replyBuf(
        res->READ3res_u.resok.data.data_val, res->READ3res_u.resok.count);
  } else if (retry) {
    ctx->getClient()->readWithContext(ctx);
  }
}

void NfsClient::readWithContext(ReadRpcContext* ctx) {
  int rpc_status = RPC_STATUS_ERROR;

  // Since FUSE chops reads into 128K chunks we'll never need to
  // handle this case this for any server which has maxRead >= 128K.
  auto inode = ctx->getInode();
  auto off = ctx->getOff();
  auto size = ctx->getSize();
  assert(size <= maxRead_);

  do {
    READ3args args;
    ::memset(&args, 0, sizeof(args));
    args.file = inodeLookup(inode)->getFh();
    args.offset = off;
    args.count = size;

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_read_async(ctx->getRpcCtx(), readCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::read(
    fuse_req_t req,
    fuse_ino_t inode,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto ctx =
      new ReadRpcContext(this, req, FOPTYPE_READ, inode, file, size, off);
  readWithContext(ctx);
}

class WriteRpcContext : public RpcContextInodeFile {
 public:
  WriteRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      fuse_file_info* file,
      size_t size,
      off_t off,
      const char* buf)
      : RpcContextInodeFile(client, req, optype, inode, file) {
    size_ = size;
    off_ = off;
    buf_ = buf;
  }
  size_t getSize() const {
    return size_;
  }
  off_t getOff() const {
    return off_;
  }
  const char* getBuf() const {
    return buf_;
  }

 private:
  size_t size_;
  off_t off_;
  const char* buf_;
};

static void writeCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (WriteRpcContext*)private_data;
  auto res = (WRITE3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    ctx->replyWrite(res->WRITE3res_u.resok.count);
  } else if (retry) {
    ctx->getClient()->writeWithContext(ctx);
  }
}

void NfsClient::writeWithContext(WriteRpcContext* ctx) {
  int rpc_status = RPC_STATUS_ERROR;

  // Since FUSE chops writes into 128K chunks we'll never need to
  // do this for any server which has maxWrite >= 128K.
  auto inode = ctx->getInode();
  auto off = ctx->getOff();
  auto size = ctx->getSize();
  auto file = ctx->getFile();
  auto buf = ctx->getBuf();
  assert(size <= maxWrite_);
  do {
    WRITE3args args;
    ::memset(&args, 0, sizeof(args));
    args.file = inodeLookup(inode)->getFh();
    args.offset = off;
    args.count = size;
    args.stable = file->flags & O_SYNC ? FILE_SYNC : UNSTABLE;
    args.data.data_len = size;
    args.data.data_val = (char*)buf;

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_write_async(ctx->getRpcCtx(), writeCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::write(
    fuse_req_t req,
    fuse_ino_t inode,
    const char* buf,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto ctx = new WriteRpcContext(
      this, req, FOPTYPE_WRITE, inode, file, size, off, buf);
  writeWithContext(ctx);
}

namespace {
class CreateRpcContext : public RpcContextParentName {
 public:
  CreateRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t parent,
      const char* name,
      mode_t mode,
      struct fuse_file_info* file)
      : RpcContextParentName(client, req, optype, parent, name) {
    mode_ = mode;
    if (file) {
      ::memcpy(&file_, file, sizeof(file_));
      filePtr_ = &file_;
    } else {
      filePtr_ = nullptr;
    }
  }
  mode_t getMode() const {
    return mode_;
  }
  struct fuse_file_info* getFile() {
    return filePtr_;
  }

 private:
  mode_t mode_;
  struct fuse_file_info file_;
  struct fuse_file_info* filePtr_;
};
} // anonymous namespace

void NfsClient::createCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (CreateRpcContext*)private_data;
  auto res = (CREATE3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    assert(
        res->CREATE3res_u.resok.obj.handle_follows &&
        res->CREATE3res_u.resok.obj_attributes.attributes_follow);
    REPLY_ENTRY(
        res->CREATE3res_u.resok.obj.post_op_fh3_u.handle,
        res->CREATE3res_u.resok.obj_attributes.post_op_attr_u.attributes,
        ctx->getFile());
  } else if (retry) {
    ctx->getClient()->create(
        ctx->getReq(),
        ctx->getParent(),
        ctx->getName(),
        ctx->getMode(),
        ctx->getFile());
    delete ctx;
  }
}

void NfsClient::create(
    fuse_req_t req,
    fuse_ino_t parent,
    const char* name,
    mode_t mode,
    struct fuse_file_info* file) {
  auto ctx =
      new CreateRpcContext(this, req, FOPTYPE_CREATE, parent, name, mode, file);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    CREATE3args args;
    ::memset(&args, 0, sizeof(args));
    args.where.dir = inodeLookup(parent)->getFh();
    args.where.name = (char*)ctx->getName();
    args.how.mode = (file->flags & O_EXCL) ? GUARDED : UNCHECKED;
    args.how.createhow3_u.obj_attributes.mode.set_it = 1;
    args.how.createhow3_u.obj_attributes.mode.set_mode3_u.mode = mode;

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, true);
      rpc_status = rpc_nfs3_create_async(
          ctx->getRpcCtx(), this->createCallback, &args, ctx);
      restoreUidGid(*ctx, true);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

static void unlinkCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextParentName*)private_data;
  auto res = (REMOVE3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    ctx->replyError(0);
  } else if (retry) {
    ctx->getClient()->unlink(ctx->getReq(), ctx->getParent(), ctx->getName());
    delete ctx;
  }
}

void NfsClient::unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto ctx = new RpcContextParentName(this, req, FOPTYPE_UNLINK, parent, name);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    REMOVE3args args;
    ::memset(&args, 0, sizeof(args));
    args.object.dir = inodeLookup(parent)->getFh();
    args.object.name = (char*)ctx->getName();

    if (ctx->obtainConnection()) {
      logger_->LOG_MSG(
          LOG_INFO,
          "unlink: mode %d uid %d.\n",
          permMode_,
          fuse_req_ctx(req)->uid);
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_remove_async(ctx->getRpcCtx(), unlinkCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

class SetattrRpcContext : public RpcContextInodeFile {
 public:
  SetattrRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      struct stat* attr,
      int valid,
      struct fuse_file_info* file)
      : RpcContextInodeFile(client, req, optype, inode, file) {
    attr_ = attr;
    valid_ = valid;
  }

  struct stat* getAttr() const {
    return attr_;
  }
  int getValid() const {
    return valid_;
  }

 private:
  struct stat* attr_;
  int valid_;
};

static void setattrCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (SetattrRpcContext*)private_data;
  auto res = (SETATTR3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    assert(res->SETATTR3res_u.resok.obj_wcc.after.attributes_follow);
    struct stat st;
    ctx->getClient()->stat_from_fattr3(
        &st, &res->SETATTR3res_u.resok.obj_wcc.after.post_op_attr_u.attributes);
    ctx->replyAttr(&st, NfsClient::getAttrTimeout());
  } else if (retry) {
    ctx->getClient()->setattrWithContext(ctx);
  }
}

void NfsClient::setattrWithContext(SetattrRpcContext* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto inode = ctx->getInode();
  auto attr = ctx->getAttr();
  auto valid = ctx->getValid();
  do {
    SETATTR3args args;
    ::memset(&args, 0, sizeof(args));
    args.object = inodeLookup(inode)->getFh();

    if (valid & FUSE_SET_ATTR_SIZE) {
      logger_->LOG_MSG(
          LOG_DEBUG, "%s setting size to %lu.\n", __func__, attr->st_size);
      args.new_attributes.size.set_it = 1;
      args.new_attributes.size.set_size3_u.size = attr->st_size;
    }

    if (valid & FUSE_SET_ATTR_MODE) {
      logger_->LOG_MSG(
          LOG_DEBUG, "%s setting mode to %u.\n", __func__, attr->st_mode);
      args.new_attributes.mode.set_it = 1;
      args.new_attributes.mode.set_mode3_u.mode = attr->st_mode;
    }

    if (valid & FUSE_SET_ATTR_UID) {
      logger_->LOG_MSG(
          LOG_DEBUG, "%s setting uid to %u.\n", __func__, attr->st_uid);
      args.new_attributes.uid.set_it = 1;
      args.new_attributes.uid.set_uid3_u.uid = attr->st_uid;
    }

    if (valid & FUSE_SET_ATTR_GID) {
      logger_->LOG_MSG(
          LOG_DEBUG, "%s setting gid to %u.\n", __func__, attr->st_gid);
      args.new_attributes.gid.set_it = 1;
      args.new_attributes.gid.set_gid3_u.gid = attr->st_gid;
    }

    if (valid & FUSE_SET_ATTR_ATIME) {
      logger_->LOG_MSG(
          LOG_DEBUG,
          "%s setting atime to %lu.\n",
          __func__,
          attr->st_atim.tv_sec);
      args.new_attributes.atime.set_it = SET_TO_CLIENT_TIME;
      args.new_attributes.atime.set_atime_u.atime.seconds =
          attr->st_atim.tv_sec;
      args.new_attributes.atime.set_atime_u.atime.nseconds =
          attr->st_atim.tv_nsec;
    }

    if (valid & FUSE_SET_ATTR_MTIME) {
      logger_->LOG_MSG(
          LOG_DEBUG,
          "%s setting mtime to %lu.\n",
          __func__,
          attr->st_mtim.tv_sec);
      args.new_attributes.mtime.set_it = SET_TO_CLIENT_TIME;
      args.new_attributes.mtime.set_mtime_u.mtime.seconds =
          attr->st_mtim.tv_sec;
      args.new_attributes.mtime.set_mtime_u.mtime.nseconds =
          attr->st_mtim.tv_nsec;
    }

    if (valid & FUSE_SET_ATTR_ATIME_NOW) {
      logger_->LOG_MSG(LOG_DEBUG, "%s setting atime to now.\n", __func__);
      args.new_attributes.atime.set_it = SET_TO_SERVER_TIME;
    }

    if (valid & FUSE_SET_ATTR_MTIME_NOW) {
      logger_->LOG_MSG(LOG_DEBUG, "%s setting mtime to now.\n", __func__);
      args.new_attributes.mtime.set_it = SET_TO_SERVER_TIME;
    }

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_setattr_async(ctx->getRpcCtx(), setattrCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::setattr(
    fuse_req_t req,
    fuse_ino_t inode,
    struct stat* attr,
    int valid,
    struct fuse_file_info* file) {
  auto ctx = new SetattrRpcContext(
      this, req, FOPTYPE_SETATTR, inode, attr, valid, file);
  setattrWithContext(ctx);
}

namespace {
class MkdirRpcContext : public RpcContextParentName {
 public:
  MkdirRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t parent,
      const char* name,
      mode_t mode)
      : RpcContextParentName(client, req, optype, parent, name) {
    mode_ = mode;
  }
  mode_t getMode() const {
    return mode_;
  }

 private:
  mode_t mode_;
};
} // anonymous namespace

void NfsClient::mkdirCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (MkdirRpcContext*)private_data;
  auto res = (MKDIR3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    assert(
        res->MKDIR3res_u.resok.obj.handle_follows &&
        res->MKDIR3res_u.resok.obj_attributes.attributes_follow);

    REPLY_ENTRY(
        res->MKDIR3res_u.resok.obj.post_op_fh3_u.handle,
        res->MKDIR3res_u.resok.obj_attributes.post_op_attr_u.attributes,
        nullptr);
  } else if (retry) {
    ctx->getClient()->mkdir(
        ctx->getReq(), ctx->getParent(), ctx->getName(), ctx->getMode());
    delete ctx;
  }
}

void NfsClient::mkdir(
    fuse_req_t req,
    fuse_ino_t parent,
    const char* name,
    mode_t mode) {
  auto ctx = new MkdirRpcContext(this, req, FOPTYPE_MKDIR, parent, name, mode);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    MKDIR3args args;
    ::memset(&args, 0, sizeof(args));
    args.where.dir = inodeLookup(parent)->getFh();
    args.where.name = (char*)ctx->getName();
    args.attributes.mode.set_it = 1;
    args.attributes.mode.set_mode3_u.mode = mode;

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, true);
      rpc_status = rpc_nfs3_mkdir_async(
          ctx->getRpcCtx(), this->mkdirCallback, &args, ctx);
      restoreUidGid(*ctx, true);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

static void rmdirCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextParentName*)private_data;
  auto res = (RMDIR3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    ctx->replyError(0);
  } else if (retry) {
    ctx->getClient()->rmdir(ctx->getReq(), ctx->getParent(), ctx->getName());
    delete ctx;
  }
}

void NfsClient::rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto ctx = new RpcContextParentName(this, req, FOPTYPE_RMDIR, parent, name);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    RMDIR3args args;
    ::memset(&args, 0, sizeof(args));
    args.object.dir = inodeLookup(parent)->getFh();
    args.object.name = (char*)ctx->getName();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_rmdir_async(ctx->getRpcCtx(), rmdirCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

namespace {
class SymlinkRpcContext : public RpcContextParentName {
 public:
  SymlinkRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      const char* link,
      fuse_ino_t parent,
      const char* name)
      : RpcContextParentName(client, req, optype, parent, name) {
    link_ = ::strdup(link);
  }

  ~SymlinkRpcContext() override {
    ::free((void*)link_);
  }

  const char* getLink() const {
    return link_;
  }

 private:
  const char* link_;
};
} // anonymous namespace

void NfsClient::symlinkCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (SymlinkRpcContext*)private_data;
  auto res = (SYMLINK3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    assert(
        res->SYMLINK3res_u.resok.obj.handle_follows &&
        res->SYMLINK3res_u.resok.obj_attributes.attributes_follow);
    REPLY_ENTRY(
        res->SYMLINK3res_u.resok.obj.post_op_fh3_u.handle,
        res->SYMLINK3res_u.resok.obj_attributes.post_op_attr_u.attributes,
        nullptr);
  } else if (retry) {
    ctx->getClient()->symlink(
        ctx->getReq(), ctx->getLink(), ctx->getParent(), ctx->getName());
    delete ctx;
  }
}

void NfsClient::symlink(
    fuse_req_t req,
    const char* link,
    fuse_ino_t parent,
    const char* name) {
  auto ctx =
      new SymlinkRpcContext(this, req, FOPTYPE_SYMLINK, link, parent, name);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    SYMLINK3args args;
    ::memset(&args, 0, sizeof(args));
    args.where.dir = inodeLookup(parent)->getFh();
    args.where.name = (char*)ctx->getName();
    args.symlink.symlink_data = (char*)ctx->getLink();
    args.symlink.symlink_attributes.mode.set_it = 1;
    args.symlink.symlink_attributes.mode.set_mode3_u.mode = S_IRUSR | S_IWUSR |
        S_IXUSR | S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH;

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, true);
      rpc_status = rpc_nfs3_symlink_async(
          ctx->getRpcCtx(), this->symlinkCallback, &args, ctx);
      restoreUidGid(*ctx, true);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

static void readlinkCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextInode*)private_data;
  auto res = (READLINK3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    ctx->replyReadlink(res->READLINK3res_u.resok.data);
  } else if (retry) {
    ctx->getClient()->readlink(ctx->getReq(), ctx->getInode());
    delete ctx;
  }
}

void NfsClient::readlink(fuse_req_t req, fuse_ino_t inode) {
  auto ctx = new RpcContextInode(this, req, FOPTYPE_READLINK, inode);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    READLINK3args args;
    ::memset(&args, 0, sizeof(args));
    args.symlink = inodeLookup(inode)->getFh();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status = rpc_nfs3_readlink_async(
          ctx->getRpcCtx(), readlinkCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

namespace {
class RenameRpcContext : public RpcContextParentName {
 public:
  RenameRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t parent,
      const char* name,
      fuse_ino_t newparent,
      const char* newname)
      : RpcContextParentName(client, req, optype, parent, name) {
    newparent_ = newparent;
    newname_ = ::strdup(newname);
  }

  ~RenameRpcContext() override {
    ::free((void*)newname_);
  }

  fuse_ino_t getNewParent() const {
    return newparent_;
  }
  const char* getNewName() const {
    return newname_;
  }

 private:
  fuse_ino_t newparent_;
  const char* newname_;
};
} // anonymous namespace

static void renameCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RenameRpcContext*)private_data;
  auto res = (RENAME3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    ctx->replyError(0);
  } else if (retry) {
    ctx->getClient()->rename(
        ctx->getReq(),
        ctx->getParent(),
        ctx->getName(),
        ctx->getNewParent(),
        ctx->getNewName());
    delete ctx;
  }
}

void NfsClient::rename(
    fuse_req_t req,
    fuse_ino_t parent,
    const char* name,
    fuse_ino_t newparent,
    const char* newname) {
  auto ctx = new RenameRpcContext(
      this, req, FOPTYPE_RENAME, parent, name, newparent, newname);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    RENAME3args args;
    ::memset(&args, 0, sizeof(args));
    args.from.dir = inodeLookup(parent)->getFh();
    args.from.name = (char*)ctx->getName();
    args.to.dir = inodeLookup(newparent)->getFh();
    args.to.name = (char*)ctx->getNewName();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_rename_async(ctx->getRpcCtx(), renameCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

namespace {
class LinkRpcContext : public RpcContextParentName {
 public:
  LinkRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      fuse_ino_t parent,
      const char* name)
      : RpcContextParentName(client, req, optype, parent, name) {
    inode_ = inode;
  }

  fuse_ino_t getInode() const {
    return inode_;
  }

 private:
  fuse_ino_t inode_;
};
} // anonymous namespace

void NfsClient::linkCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (LinkRpcContext*)private_data;
  auto client = ctx->getClient();
  auto res = (LINK3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry, false)) {
    assert(res->LINK3res_u.resok.file_attributes.attributes_follow);
    auto fh = client->inodeLookup(ctx->getInode())->getFh();
    REPLY_ENTRY(
        fh,
        res->LINK3res_u.resok.file_attributes.post_op_attr_u.attributes,
        nullptr);
  } else if (retry) {
    client->link(
        ctx->getReq(), ctx->getInode(), ctx->getParent(), ctx->getName());
    delete ctx;
  }
}

void NfsClient::link(
    fuse_req_t req,
    fuse_ino_t inode,
    fuse_ino_t newparent,
    const char* newname) {
  auto ctx =
      new LinkRpcContext(this, req, FOPTYPE_LINK, inode, newparent, newname);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    LINK3args args;
    ::memset(&args, 0, sizeof(args));
    args.file = inodeLookup(inode)->getFh();
    args.link.dir = inodeLookup(newparent)->getFh();
    args.link.name = (char*)ctx->getName();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_link_async(ctx->getRpcCtx(), this->linkCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

namespace {
class FsyncRpcContext : public RpcContextInodeFile {
 public:
  FsyncRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      int datasync,
      fuse_file_info* file)
      : RpcContextInodeFile(client, req, optype, inode, file) {
    datasync_ = datasync;
  }

  int getDatasync() const {
    return datasync_;
  }

 private:
  int datasync_;
};
} // anonymous namespace

static void fsyncCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (FsyncRpcContext*)private_data;
  auto res = (COMMIT3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    ctx->replyError(0);
  } else if (retry) {
    ctx->getClient()->fsync(
        ctx->getReq(), ctx->getInode(), ctx->getDatasync(), ctx->getFile());
    delete ctx;
  }
}

void NfsClient::fsync(
    fuse_req_t req,
    fuse_ino_t inode,
    int datasync,
    fuse_file_info* file) {
  auto ctx =
      new FsyncRpcContext(this, req, FOPTYPE_FSYNC, inode, datasync, file);
  int rpc_status = RPC_STATUS_ERROR;

  do {
    COMMIT3args args;
    ::memset(&args, 0, sizeof(args));
    args.file = inodeLookup(inode)->getFh();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_commit_async(ctx->getRpcCtx(), fsyncCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

static void statfsCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (RpcContextInode*)private_data;
  auto res = (FSSTAT3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    struct statvfs st;
    ::memset(&st, 0, sizeof(st));
    st.f_bsize = NFS_BLKSIZE;
    st.f_frsize = NFS_BLKSIZE;
    st.f_blocks = res->FSSTAT3res_u.resok.tbytes / NFS_BLKSIZE;
    st.f_bfree = res->FSSTAT3res_u.resok.fbytes / NFS_BLKSIZE;
    st.f_bavail = res->FSSTAT3res_u.resok.abytes / NFS_BLKSIZE;
    st.f_files = res->FSSTAT3res_u.resok.tfiles;
    st.f_ffree = res->FSSTAT3res_u.resok.ffiles;
    st.f_favail = res->FSSTAT3res_u.resok.afiles;
    st.f_fsid = 0xc0ffee;
    st.f_flag =
        ST_NODEV; // unless we implement mknod - but why would we do that?
    st.f_namemax = NAME_MAX;

    ctx->replyStatfs(&st);
  } else if (retry) {
    ctx->getClient()->statfsWithContext(ctx);
  }
}

void NfsClient::statfsWithContext(RpcContextInode* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto inode = ctx->getInode();
  do {
    FSSTAT3args args;
    ::memset(&args, 0, sizeof(args));
    args.fsroot = inodeLookup(inode)->getFh();

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_fsstat_async(ctx->getRpcCtx(), statfsCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::statfs(fuse_req_t req, fuse_ino_t inode) {
  auto ctx = new RpcContextInode(this, req, FOPTYPE_STATFS, inode);
  statfsWithContext(ctx);
}

class AccessRpcContext : public RpcContext {
 public:
  AccessRpcContext(
      NfsClient* client,
      struct fuse_req* req,
      enum fuse_optype optype,
      fuse_ino_t inode,
      int mask)
      : RpcContext(client, req, optype) {
    inode_ = inode;
    mask_ = mask;
  }

  fuse_ino_t getInode() const {
    return inode_;
  }

  int getMask() const {
    return mask_;
  }

 private:
  fuse_ino_t inode_;
  int mask_;
};

static void accessCallback(
    struct rpc_context* /* rpc */,
    int rpc_status,
    void* data,
    void* private_data) {
  auto ctx = (AccessRpcContext*)private_data;
  auto res = (ACCESS3res*)data;
  bool retry;

  if (ctx->succeeded(rpc_status, RSTATUS(res), retry)) {
    ctx->replyError(0);
  } else if (retry) {
    ctx->getClient()->accessWithContext(ctx);
  }
}

void NfsClient::accessWithContext(AccessRpcContext* ctx) {
  int rpc_status = RPC_STATUS_ERROR;
  auto inode = ctx->getInode();
  auto mask = ctx->getMask();
  do {
    ACCESS3args args;
    ::memset(&args, 0, sizeof(args));
    args.object = inodeLookup(inode)->getFh();
    if (mask & O_WRONLY) {
      args.access = ACCESS3_MODIFY;
    } else if (mask & O_RDWR) {
      args.access = ACCESS3_READ | ACCESS3_MODIFY;
    } else {
      args.access = ACCESS3_READ;
    }

    if (ctx->obtainConnection()) {
      setUidGid(*ctx, false);
      rpc_status =
          rpc_nfs3_access_async(ctx->getRpcCtx(), accessCallback, &args, ctx);
      restoreUidGid(*ctx, false);
      ctx->unlockConnection();
    }
  } while (shouldRetry(rpc_status, ctx));
}

void NfsClient::access(fuse_req_t req, fuse_ino_t inode, int mask) {
  auto ctx = new AccessRpcContext(this, req, FOPTYPE_ACCESS, inode, mask);
  accessWithContext(ctx);
}
