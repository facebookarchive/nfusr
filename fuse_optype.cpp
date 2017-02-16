/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "fuse_optype.h"

const char* fuse_optype_name(enum fuse_optype type) {
  switch (type) {
    case FOPTYPE_LOOKUP:
      return "lookup";
    case FOPTYPE_FORGET:
      return "forget";
    case FOPTYPE_GETATTR:
      return "getattr";
    case FOPTYPE_SETATTR:
      return "setattr";
    case FOPTYPE_READLINK:
      return "readlink";
    case FOPTYPE_SYMLINK:
      return "symlink";
    case FOPTYPE_MKNOD:
      return "mknod";
    case FOPTYPE_MKDIR:
      return "mkdir";
    case FOPTYPE_UNLINK:
      return "unlink";
    case FOPTYPE_RMDIR:
      return "rmdir";
    case FOPTYPE_RENAME:
      return "rename";
    case FOPTYPE_LINK:
      return "link";
    case FOPTYPE_OPEN:
      return "open";
    case FOPTYPE_READ:
      return "read";
    case FOPTYPE_WRITE:
      return "write";
    case FOPTYPE_STATFS:
      return "statfs";
    case FOPTYPE_RELEASE:
      return "release";
    case FOPTYPE_FSYNC:
      return "fsync";
    case FOPTYPE_SETXATTR:
      return "setxattr";
    case FOPTYPE_GETXATTR:
      return "getxattr";
    case FOPTYPE_LISTXATTR:
      return "listxattr";
    case FOPTYPE_REMOVEXATTR:
      return "removexattr";
    case FOPTYPE_FLUSH:
      return "flush";
    case FOPTYPE_INIT:
      return "init";
    case FOPTYPE_OPENDIR:
      return "opendir";
    case FOPTYPE_READDIR:
      return "readdir";
    case FOPTYPE_RELEASEDIR:
      return "releasedir";
    case FOPTYPE_FSYNCDIR:
      return "fsyncdir";
    case FOPTYPE_GETLK:
      return "getlk";
    case FOPTYPE_SETLK:
      return "setlk";
    case FOPTYPE_SETLKW:
      return "setlkw";
    case FOPTYPE_ACCESS:
      return "access";
    case FOPTYPE_CREATE:
      return "create";
    case FOPTYPE_INTERRUPT:
      return "interrupt";
    case FOPTYPE_BMAP:
      return "bmap";
    case FOPTYPE_DESTROY:
      return "destroy";
    case FOPTYPE_IOCTL:
      return "ioctl";
    case FOPTYPE_POLL:
      return "poll";
    case FOPTYPE_NOTIFY_REPLY:
      return "notify_reply";
    case FOPTYPE_BATCH_FORGET:
      return "batch_forget";
    case FOPTYPE_FALLOCATE:
      return "fallocate";
    case FOPTYPE_READDIRPLUS:
      return "readdirplus";
    case FOPTYPE_RENAME2:
      return "rename2";
  }
  return "invalid";
}
