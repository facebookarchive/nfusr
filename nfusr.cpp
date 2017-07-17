/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

#include <syslog.h>
#include <unistd.h>

#include "NfsCachedClient.h"
#include "NfsClient.h"
#include "RpcContext.h"

static int g_foreground = 1;
static const int defaultNfsTimeoutMs = 60000;
static const double defaultAttrTimeout = 60.0;
static const unsigned int defaultMaxErrnoRetries = 0;
unsigned RpcContext::maxErrnoRetries_ = defaultMaxErrnoRetries;

static void
nfs_ll_getattr(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->getattr(req, inode, file);
}

static void nfs_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      parent,
      name);
  client->lookup(req, parent, name);
}

static void
nfs_ll_opendir(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->opendir(req, inode, file);
}

static void nfs_ll_readdir(
    fuse_req_t req,
    fuse_ino_t inode,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu@:%lu@%lu)\n",
      __func__,
      fuse_get_unique(req),
      inode,
      size,
      off);
  client->readdir(req, inode, size, off, file);
}

static void nfs_ll_releasedir(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->releasedir(req, inode, file);
}

static void
nfs_ll_forget(fuse_req_t req, fuse_ino_t inode, unsigned long nlookup) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu, %lu)\n",
      __func__,
      fuse_get_unique(req),
      inode,
      nlookup);
  client->forget(req, inode, nlookup);
}

static void
nfs_ll_open(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->open(req, inode, file);
}

static void
nfs_ll_release(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info* fi) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->release(req, inode, fi);
}

static void nfs_ll_read(
    fuse_req_t req,
    fuse_ino_t inode,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu:%lu@%lu)\n",
      __func__,
      fuse_get_unique(req),
      inode,
      size,
      off);
  client->read(req, inode, size, off, file);
}

static void nfs_ll_write(
    fuse_req_t req,
    fuse_ino_t inode,
    const char* buf,
    size_t size,
    off_t off,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu:%lu@%lu)\n",
      __func__,
      fuse_get_unique(req),
      inode,
      size,
      off);
  client->write(req, inode, buf, size, off, file);
}

static void nfs_ll_create(
    fuse_req_t req,
    fuse_ino_t parent,
    const char* name,
    mode_t mode,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      parent,
      name);
  client->create(req, parent, name, mode, file);
}

static void nfs_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      parent,
      name);
  client->unlink(req, parent, name);
}

static void nfs_ll_setattr(
    fuse_req_t req,
    fuse_ino_t inode,
    struct stat* attr,
    int valid,
    struct fuse_file_info* fi) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->setattr(req, inode, attr, valid, fi);
}

static void
nfs_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%s)\n", __func__, fuse_get_unique(req), name);
  client->mkdir(req, parent, name, mode);
}

static void nfs_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      parent,
      name);
  client->rmdir(req, parent, name);
}

static void nfs_ll_symlink(
    fuse_req_t req,
    const char* link,
    fuse_ino_t parent,
    const char* name) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%s -> %lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      link,
      parent,
      name);
  client->symlink(req, link, parent, name);
}

static void nfs_ll_readlink(fuse_req_t req, fuse_ino_t inode) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->readlink(req, inode);
}

static void nfs_ll_link(
    fuse_req_t req,
    fuse_ino_t inode,
    fuse_ino_t newparent,
    const char* newname) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu -> %lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      inode,
      newparent,
      newname);
  client->link(req, inode, newparent, newname);
}

static void nfs_ll_rename(
    fuse_req_t req,
    fuse_ino_t parent,
    const char* name,
    fuse_ino_t newparent,
    const char* newname) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG,
      "%s(%lu)(%lu+%s -> %lu+%s)\n",
      __func__,
      fuse_get_unique(req),
      parent,
      name,
      newparent,
      newname);
  client->rename(req, parent, name, newparent, newname);
}

static void nfs_ll_fsync(
    fuse_req_t req,
    fuse_ino_t inode,
    int datasync,
    struct fuse_file_info* file) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->fsync(req, inode, datasync, file);
}

static void nfs_ll_flush(
    fuse_req_t req,
    fuse_ino_t inode,
    struct fuse_file_info* /* file */) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);

  // FLUSH is a no-op; as per FUSE docs, despite the name it is not expected to
  // sync data,
  // just clean up locks etc. Sync is done in release(). The only reason to
  // implement this method is to make FUSE debug traces look less ugly.
  fuse_reply_err(req, 0);
}

static void nfs_ll_statfs(fuse_req_t req, fuse_ino_t inode) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->statfs(req, inode);
}

static void nfs_ll_access(fuse_req_t req, fuse_ino_t inode, int mask) {
  auto client = reinterpret_cast<NfsClient*>(fuse_req_userdata(req));
  client->getLogger()->LOG_MSG(
      LOG_DEBUG, "%s(%lu)(%lu)\n", __func__, fuse_get_unique(req), inode);
  client->access(req, inode, mask);
}

static void init_readonly_nfs_ll_ops(struct fuse_lowlevel_ops& ops) {
  ::memset(&ops, 0, sizeof(ops));
  ops.opendir = nfs_ll_opendir;
  ops.readdir = nfs_ll_readdir;
  ops.releasedir = nfs_ll_releasedir;
  ops.lookup = nfs_ll_lookup;
  ops.forget = nfs_ll_forget;
  ops.getattr = nfs_ll_getattr;
  ops.open = nfs_ll_open;
  ops.release = nfs_ll_release;
  ops.read = nfs_ll_read;
  ops.readlink = nfs_ll_readlink;
  ops.statfs = nfs_ll_statfs;
  ops.access = nfs_ll_access;
}

static void init_nfs_ll_ops(struct fuse_lowlevel_ops& ops) {
  ::memset(&ops, 0, sizeof(ops));
  ops.opendir = nfs_ll_opendir;
  ops.readdir = nfs_ll_readdir;
  ops.releasedir = nfs_ll_releasedir;
  ops.lookup = nfs_ll_lookup;
  ops.forget = nfs_ll_forget;
  ops.getattr = nfs_ll_getattr;
  ops.open = nfs_ll_open;
  ops.create = nfs_ll_create;
  ops.release = nfs_ll_release;
  ops.read = nfs_ll_read;
  ops.write = nfs_ll_write;
  ops.unlink = nfs_ll_unlink;
  ops.setattr = nfs_ll_setattr;
  ops.mkdir = nfs_ll_mkdir;
  ops.rmdir = nfs_ll_rmdir;
  ops.symlink = nfs_ll_symlink;
  ops.readlink = nfs_ll_readlink;
  ops.rename = nfs_ll_rename;
  ops.link = nfs_ll_link;
  ops.fsync = nfs_ll_fsync;
  ops.flush = nfs_ll_flush;
  ops.statfs = nfs_ll_statfs;
  ops.access = nfs_ll_access;
  // The following FUSE operations are not implemented:
  //  init / destroy: no need yet.
  //  fsyncdir: NFS doesn't implement, FUSE handles absence gracefully.
  //  mknod: who wants to make device nodes on NFS mounts?!
  //  setxattr / getxattr / listxattr / removexattr: no extended attributes on
  //  NFS3.
  //  getlk / setlk: no lock support on NFS3.
  //  bmap: not a block device.
  //  ioctl: no need yet.
  //  poll: optional and difficult to implement.
  //
  // notify_reply: not sure what this is, inotify support?
  // batch_forget: looks useful!
  // fallocate: no NFS support?
  // readdirplus: looks useful!
  // rename2: takes some flags (NOREPLACE?), doesn't look necessary.
}

/// @brief determine if it is safe to set allow_others option.
bool can_set_allow_others() {
  if (getuid() == 0) {
    // Root can always set allow_others.
    return true;
  }

  // Non-root user can only set allow_others if /etc/fuse.conf
  // contains user_allow_other.
  std::ifstream fuse_conf("/etc/fuse.conf");
  if (fuse_conf) {
    std::string line;

    while (std::getline(fuse_conf, line)) {
      if (line.find('#') != std::string::npos) {
        // Yes, if there is a '#' *anywhere* in the line it is a comment.
        // This matches libfuse parsing (see strip_line() in libfuse source).
        continue;
      }

      // Trim leading and trailing whitespace.
      size_t start = 0;
      while (start < line.length() && isspace(line[start])) {
        ++start;
      }
      line.erase(0, start);

      size_t end = line.length();
      while (end > 0 && isspace(line[end - 1])) {
        --end;
      }
      line.erase(end);

      if (line.compare("user_allow_other") == 0) {
        return true;
      }
    }
  }

  return false;
}

/// @brief Do normal unix daemonization, wait for child to tell us result.
///
/// We want to daemonize before starting any threads, because threads
/// and fork are a terrible mix. Normal daemon() (or fuse_daemonize())
/// immediately exit the parent process with zero, telling the world
/// we succeeded. But we need to start several threads before we are
/// fully mounted. .
///
/// So: create a pipe. In parent process, instead of immediately
/// exiting, wait for child process to tell us the result via the
/// pipe. In the child process, this function returns the fd by
/// which we may return exit status to the parent; in the parent
/// process it never returns.
///
/// @param log: name of the log file for the child process (used
///             only in error messages).
///
/// @return -1 on error, pipe fd otherwise.
int nfusr_daemonize(const char* log) {
  int nullfd;
  int pipefd[2];
  int parentrc;

  if (pipe(pipefd)) {
    return -1;
  }

  switch (fork()) {
    case -1:
      return -1;
    case 0:
      close(pipefd[0]);
      break;
    default:
      // We're the parent process. Wait for child to tell us how things
      // went by writing their half of pipefd.
      close(pipefd[1]);
      if (read(pipefd[0], &parentrc, sizeof(parentrc)) != sizeof(parentrc)) {
        parentrc = 1;
      }
      if (parentrc) {
        printf("Mount failed. More information may be available in %s.\n", log);
      }
      exit(parentrc);
  }

  if (setsid() == -1) {
    return -1;
  }

  chdir("/");

  if ((nullfd = open("/dev/null", O_RDWR)) != -1) {
    dup2(nullfd, STDIN_FILENO);
    dup2(nullfd, STDOUT_FILENO);
    dup2(nullfd, STDERR_FILENO);
    if (nullfd > 2) {
      close(nullfd);
    }
  }
  return pipefd[1];
}

struct nfusr_mount_options {
  unsigned maxConnections;
  int nfsTimeout;
  double attrTimeout;
  unsigned maxErrnoRetries;
  char* permMode;
};

std::array<struct fuse_opt, 6> nfusr_mount_opt_descriptors = {
    {{"max_conn=%u", offsetof(struct nfusr_mount_options, maxConnections), 0},
     {"timeout=%d", offsetof(struct nfusr_mount_options, nfsTimeout), 0},
     {"actimeo=%lf", offsetof(struct nfusr_mount_options, attrTimeout), 0},
     {"maxerrorretries=%u",
      offsetof(struct nfusr_mount_options, maxErrnoRetries),
      0},
     {"perm_mode=%s", offsetof(struct nfusr_mount_options, permMode), 0},
     FUSE_OPT_END}};

int main(int argc, char* argv[]) {
  struct fuse_args fuse_args = FUSE_ARGS_INIT(0, nullptr);
  struct fuse_chan* chan = nullptr;
  struct fuse_session* session = nullptr;
  std::shared_ptr<nfusr::Logger> logger;
  std::shared_ptr<ClientStats> stats;
  std::unique_ptr<NfsClient> nfs_client;
  char* mount = nullptr;
  int rc = EXIT_FAILURE;
  int multithreaded;
  struct fuse_lowlevel_ops nfs_ll_ops;
  struct nfusr_mount_options mnt_options;
  std::vector<std::string> urls;
  bool errorInjection = false;
  bool logFifo = false;
  bool readOnly = false;
  char* logFile = nullptr;
  char* statsFile = nullptr;
  int logLevel;
  char* hostscript = nullptr;
  int scriptRefreshSeconds = 60;
  std::shared_ptr<std::string> cacheRoot = nullptr;
  char* cacheRootStr;
  int result_fd = -1;
  NfsClientPermissionMode permMode = StrictPerms;

  logger = std::make_shared<nfusr::Logger>();

  mnt_options.maxConnections = 1;
  mnt_options.nfsTimeout = defaultNfsTimeoutMs;
  mnt_options.attrTimeout = defaultAttrTimeout;
  mnt_options.maxErrnoRetries = defaultMaxErrnoRetries;
  mnt_options.permMode = nullptr;

  for (int i = 0; i < argc; ++i) {
    if (!::strncmp(argv[i], "nfs://", 6)) {
      urls.push_back(argv[i]);
    } else if (!::strcmp(argv[i], "--error-injection")) {
      errorInjection = true;
    } else if (!::strcmp(argv[i], "--log-fifo")) {
      logFifo = true;
    } else if (!::strcmp(argv[i], "--read-only")) {
      readOnly = true;
    } else if (::sscanf(argv[i], "--cache-root=%ms", &cacheRootStr) == 1) {
      cacheRoot = std::make_shared<std::string>(cacheRootStr);
      free(cacheRootStr);
      readOnly = true; // read only caching for now
      // we trust this path exists
    } else if (::sscanf(argv[i], "--log-file=%ms", &logFile) == 1) {
      // handled when we daemonize below,
    } else if (::sscanf(argv[i], "--stats-file=%ms", &statsFile) == 1) {
      // handled when we create client below.
    } else if (::sscanf(argv[i], "--log-level=%d", &logLevel) == 1) {
      logger->setMask(LOG_UPTO(logLevel));
    } else if (::sscanf(argv[i], "--host-script=%ms", &hostscript) == 1) {
      // get host script name
    } else if (::sscanf(argv[i],
                        "--host-refresh-seconds=%d",
                        &scriptRefreshSeconds) == 1) {
      // get host script refresh time
    } else {
      if (fuse_opt_add_arg(&fuse_args, argv[i])) {
        logger->LOG_MSG(
            LOG_ERR, "Out of memory parsing command line arguments.\n");
        goto cleanup;
      }
    }
  }

  if (readOnly) {
    init_readonly_nfs_ll_ops(nfs_ll_ops);
  } else {
    init_nfs_ll_ops(nfs_ll_ops);
  }

  fuse_opt_add_arg(&fuse_args, "-obig_writes");
  if (can_set_allow_others()) {
    fuse_opt_add_arg(&fuse_args, "-oallow_other");
  }

  fuse_opt_parse(
      &fuse_args, &mnt_options, nfusr_mount_opt_descriptors.data(), nullptr);

  if (mnt_options.maxConnections == 0) {
    logger->LOG_MSG(LOG_ERR, "max_conn option must be non-zero.\n");
    goto cleanup;
  }

  NfsClient::setAttrTimeout(mnt_options.attrTimeout);
  RpcContext::setMaxErrnoRetries(mnt_options.maxErrnoRetries);
  if (mnt_options.permMode) {
    if (!strcmp(mnt_options.permMode, "initial")) {
      permMode = InitialPerms;
    } else if (!strcmp(mnt_options.permMode, "sloppy")) {
      permMode = SloppyPerms;
    } else if (!strcmp(mnt_options.permMode, "strict")) {
      permMode = StrictPerms;
    } else {
      logger->LOG_MSG(
          LOG_ERR,
          "perm_mode option must be one of: initial, sloppy, strict.\n");
      goto cleanup;
    }
    ::free(mnt_options.permMode);
    mnt_options.permMode = nullptr;
  }

  if (fuse_parse_cmdline(&fuse_args, &mount, &multithreaded, &g_foreground) <
      0) {
    logger->LOG_MSG(LOG_ERR, "Cannot parse FUSE command line.\n");
    goto cleanup;
  }

  if (urls.empty() && ! hostscript) {
    logger->LOG_MSG(LOG_ERR, "Required NFS host option missing.\n");
    goto cleanup;
  }

  chan = fuse_mount(mount, &fuse_args);
  if (chan == nullptr) {
    logger->LOG_MSG(LOG_ERR, "Cannot mount %s.\n", mount);
    goto cleanup;
  }

  if (!g_foreground) {
    if ((result_fd = nfusr_daemonize(logFile ? logFile : "syslog")) == -1) {
      logger->LOG_MSG(LOG_ERR, "Cannot daemonize.\n");
      goto cleanup;
    }
    if (logFile) {
      logger->openFile(logFile, logFifo, /* autoflush = */ true);
    } else {
      logger->syslogMode();
    }
  }

  if (statsFile) {
    stats = std::make_shared<ClientStats>();
    if (stats->start(statsFile, mount)) {
      stats = nullptr;
      logger->LOG_MSG(
          LOG_ERR, "Unable to start stats logger for %s.\n", statsFile);
    }
  }

  if (cacheRoot) {
    nfs_client = std::make_unique<NfsCachedClient>(
        urls,
        hostscript,
        scriptRefreshSeconds,
        mnt_options.maxConnections,
        mnt_options.nfsTimeout,
        logger,
        stats,
        errorInjection,
        permMode);
  } else {
    nfs_client = std::make_unique<NfsClient>(
        urls,
        hostscript,
        scriptRefreshSeconds,
        mnt_options.maxConnections,
        mnt_options.nfsTimeout,
        logger,
        stats,
        errorInjection,
        permMode);
  }

  session = fuse_lowlevel_new(
      &fuse_args, &nfs_ll_ops, sizeof(nfs_ll_ops), nfs_client.get());
  if (session == nullptr) {
    logger->LOG_MSG(LOG_ERR, "Cannot create FUSE session.\n");
    goto cleanup;
  }

  if (fuse_set_signal_handlers(session) < 0) {
    logger->LOG_MSG(LOG_ERR, "Cannot install signal handlers.\n");
    goto cleanup;
  }

  fuse_session_add_chan(session, chan);

  if (!nfs_client->start(cacheRoot)) {
    logger->LOG_MSG(LOG_ERR, "Initial NFS connection failed.\n");
    rc = -1;
    goto cleanup;
  }

  logger->LOG_MSG(LOG_INFO, "Mounted %s.\n", mount);

  rc = 0;
  write(result_fd, &rc, sizeof(rc));
  close(result_fd);
  result_fd = -1;

  if (multithreaded) {
    rc = fuse_session_loop_mt(session);
  } else {
    rc = fuse_session_loop(session);
  }

  rc = rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;

  fuse_remove_signal_handlers(session);
  fuse_session_remove_chan(chan);

  logger->LOG_MSG(LOG_INFO, "Dismounted %s.\n", mount);

cleanup:

  nfs_client.reset();

  if (session != nullptr) {
    fuse_session_destroy(session);
  }

  if (chan != nullptr) {
    fuse_unmount(mount, chan);
  }

  ::free(mount);
  ::free(logFile);
  ::free(statsFile);
  ::free(hostscript);

  fuse_opt_free_args(&fuse_args);

  if (result_fd != -1) {
    write(result_fd, &rc, sizeof(rc));
    close(result_fd);
    result_fd = -1;
  }

  return rc;
}
