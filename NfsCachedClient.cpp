/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NfsCachedClient.h"
#include "logger.h"
#include "RpcContext.h"

#include <nfsc/libnfs.h>
#include <cassert>
#include <cstring>
#include <chrono>
#include <unistd.h>

NfsCachedClient::~NfsCachedClient() {
}

bool CacheBlock::readCache() {
    auto cache_fname = std::string(*path_.get());
    cache_fname += ".swp";

    int swpfd = ::open(cache_fname.c_str(), O_RDONLY, S_IRWXU);
    if (swpfd < 0) {
      perror("open");
      return false;
    }
    off_t sz = ::lseek(swpfd, 0L, SEEK_END); // Get .swp file size
    if (sz < 0) {
      perror("lseek");
      ::close(swpfd);
      return false;
    }
    auto swpbuf = std::vector<uint8_t>(sz);
    ssize_t readret = ::pread(swpfd, swpbuf.data(), sz, 0L);
    if (readret < 0) {
      perror("read");
      ::close(swpfd);
      return false;
    }
    ::close(swpfd);

    long swpcover = sz * CACHEBLOCKSIZE;
    long swpoffset = (static_cast<long>(offset_) / CACHEBLOCKSIZE);
    long swpsize = (static_cast<long>(size_) / CACHEBLOCKSIZE);
    long cachecover = static_cast<long>(size_) + static_cast<long>(offset_);
    bool cached = false;
    if (swpcover >= cachecover) {
      cached = true;
      auto swpbufdata = swpbuf.data();
      for (long i = swpoffset; i < swpoffset + swpsize; i++) {
        if ((swpbufdata[i / 8] & (1 << (i % 8))) == 0) {
          cached = false;
        }
      }
    }
    if (cached) {
      auto fid = ::open(path_->c_str(), O_RDONLY);
      ssize_t ret = ::pread(fid, (char*)(data_.data()), size_, offset_);
      ::close(fid);
      if (ret == -1) {
        perror("pread");
        return false;
      }
      return true;
    }
    return false;
}

void NfsCachedClient::writeCache(
    std::shared_ptr<std::string> data_fname,
    size_t size,
    off_t offset,
    const char* data,
    u_int data_len) {
  auto cache_fname = std::make_unique<std::string>(*data_fname);
  *cache_fname += ".swp";

  auto fid = ::open(data_fname->c_str(), O_RDWR | O_CREAT,
                              S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  if (fid < 0) {
    perror("open");
    ::close(fid);
    return;
  }
  //XXX: not sure this is necessary with pwrite
  auto posix_ret = ::posix_fallocate(fid, offset, data_len);
  if (posix_ret != 0) {
    //posix_fallocate doesn't set errno
    fprintf(stderr, "posix_fallocate failed with %d\n", posix_ret);
    ::close(fid);
    return;
  }
  ssize_t ret = ::pwrite(fid, data, data_len, offset);
  if (ret <0 ) {
    perror("pwrite");
    ::close(fid);
    return;
  }
  ::close(fid);

  int swpfd = ::open(cache_fname->c_str(), O_RDWR | O_CREAT, S_IRWXU);
  if (swpfd < 0) {
    perror("open");
    ::close(swpfd);
    return;
  }
  off_t sz = lseek(swpfd, 0L, SEEK_END); //Get length of file
  if (sz < 0) {
    perror("lseek");
    close(swpfd);
    return;
  }
  auto swpbuf = std::vector<uint8_t>(sz);
  ssize_t readret = ::pread(swpfd, swpbuf.data(), sz, 0L); //Read entire cache
  if (readret < 0) {
    perror("read");
    close(swpfd);
    return;
  }
  //Assuming offset and size are exact multiples
  auto new_sz = (size + offset) / CACHEBLOCKSIZE;
  if (new_sz > sz) {
    //Initialize with 0
    swpbuf.resize(new_sz, 0);
    sz = new_sz;
  }
  //Assuming offset and size are exact multiples
  long swpoffset = (offset / CACHEBLOCKSIZE);
  long swpsize = (size / CACHEBLOCKSIZE);
  //Assuming 8bits is one char!
  auto swpbufdata = swpbuf.data();
  for (long i = swpoffset; i < swpoffset + swpsize; i++) {
    swpbufdata[i / 8] = swpbufdata[i / 8] | (1 << (i % 8));
  }
  ssize_t writeret = ::pwrite(swpfd, swpbuf.data(), sz, 0L);
  if (writeret < 0) {
    perror("write");
  }
  ::close(swpfd);
  return;
}

namespace {
static const double attrTimeout = 1.0;
}; // anonymous namespace

/// @brief add a new inode for the given fh and pass to fuse_reply_entry().
void NfsCachedClient::replyEntry(
    RpcContext* ctx,
    const nfs_fh3* fh,
    const struct fattr3* attr,
    const struct fuse_file_info* file,
    std::shared_ptr<std::string> local_cache_path,
    // following parameters are purely for debugs.
    const char* caller = nullptr,
    fuse_ino_t parent = 0,
    const char* name = nullptr) {
  std::shared_ptr<std::string> local_cache_path_;
  if (local_cache_path) {
    local_cache_path_ = local_cache_path;
  } else {
    auto pii = ctx->getClient()->inodeLookup(parent);
    if (pii->hasLocalCachePath()) {
      this->getLogger()->LOG_MSG(
          LOG_DEBUG,
          "%s Attempting to create directory %s.\n",
          __func__,
          pii->getLocalCachePath()->c_str());
      ::mkdir(pii->getLocalCachePath()->c_str(), S_IRWXU);
      //We don't care if mkdir fails (directory exists etc.)
      //We just need to have a directory to write to.
      struct stat s = {0};
      if (::stat(pii->getLocalCachePath()->c_str(), &s)) {
        perror("stat");
      } else {
        if ((s.st_mode & S_IFDIR) && (s.st_mode & S_IRWXU)) {
          local_cache_path_ =
              std::make_shared<std::string>(*pii->getLocalCachePath());
          local_cache_path_->append("/");
          local_cache_path_->append(name);
        }
      }
    }
  }
  NfsClient::replyEntry(
      ctx, fh, attr, file, local_cache_path_, caller, parent, name);
}

void NfsCachedClient::read(
    fuse_req_t req,
    fuse_ino_t inode,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  // Since FUSE chops reads into 128K chunks we'll never need to
  // handle this case this for any server which has maxRead >= 128K.
  assert(size <= this->getMaxRead());

  auto inodeObj = this->inodeLookup(inode);
  if (inodeObj->hasLocalCachePath()) {
    auto path = inodeObj->getLocalCachePath();
    auto cache = CacheBlock(path, size, off);
    if (cache.readCache()) {
      this->getLogger()->LOG_MSG(
          LOG_DEBUG,
          "%s Reading inode %lu from cache file %s.\n",
          __func__,
          inode,
          path->c_str());
      fuse_reply_buf(req, cache.getData(), cache.getSize());
      return;
    }
  }
  this->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s Reading inode %lu over network.\n",
      __func__,
      inode);
  auto ctx =
      new CachedReadRpcContext(this, req, FOPTYPE_READ, inode, file, size, off);
  this->readWithContext(ctx);
}
