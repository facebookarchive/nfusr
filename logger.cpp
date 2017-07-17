/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "logger.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdarg>
#include <cstdio>
#include <cstring>

namespace nfusr {

Logger::Logger()
    : fp_(nullptr),
      mask_(LOG_UPTO(LOG_NOTICE)),
      mode_(stdout_mode),
      autoFlush_(false) {}

Logger::~Logger() {
  if (fp_) {
    ::fclose(fp_);
    fp_ = nullptr;
  }
}

int Logger::openFile(const char* name, bool fifoMode, bool autoFlush) {
  if (!::strcmp(name, "-")) {
    fp_ = stdout;
  } else {
    if (fifoMode) {
      if (::mkfifo(name, S_IWUSR | S_IRUSR)) {
        if (errno != EEXIST) {
          ::fprintf(
              stderr, "Cannot create FIFO %s: %s.\n", name, ::strerror(errno));
        }
      }
      int fd = ::open(name, O_RDWR | O_NONBLOCK, 0);
      if (fd == -1) {
        ::fprintf(stderr, "Cannot open %s: %s.\n", name, ::strerror(errno));
        return errno;
      }

      fp_ = ::fdopen(fd, "a");
    } else {
      fp_ = fopen(name, "a");
    }
    if (!fp_) {
      ::fprintf(stderr, "Cannot fdopen %s: %s.\n", name, ::strerror(errno));
      return errno;
    } else {
      mode_ = file_mode;
      autoFlush_ = autoFlush;
    }
  }

  return 0;
}

static char level_char(int level) {
  switch (level) {
    case LOG_EMERG:
      return 'M';
    case LOG_ALERT:
      return 'A';
    case LOG_CRIT:
      return 'C';
    case LOG_ERR:
      return 'E';
    case LOG_WARNING:
      return 'W';
    case LOG_NOTICE:
      return 'N';
    case LOG_INFO:
      return 'I';
    case LOG_DEBUG:
      return 'D';
  }

  return '?';
}

void Logger::log_msg(
    const char* file,
    int line,
    const char* func,
    int log_level,
    const char* fmt,
    ...) {
  va_list ap;
  va_start(ap, fmt);
  char* fmtstr = nullptr;

  if (mask_ & LOG_MASK(log_level)) {
    switch (mode_) {
      case file_mode:
        char timestr[128];
        struct tm tm;
        time_t now;

        ::time(&now);
        ::localtime_r(&now, &tm);
        ::strftime(timestr, sizeof(timestr), "%F %T", &tm);
        if (::asprintf(
                &fmtstr,
                "[%s] %c [%s:%d:%s] nfusr-client: %s",
                timestr,
                level_char(log_level),
                file,
                line,
                func,
                fmt) < 0) {
          ::vfprintf(fp_, fmt, ap);
        } else {
          ::vfprintf(fp_, fmtstr, ap);
        }
        if (autoFlush_) {
          ::fflush(fp_);
        }
        break;
      case syslog_mode:
        if (::asprintf(
                &fmtstr, "[%s:%d:%s] nfusr-client: %s", file, line, func, fmt) <
            0) {
          ::vsyslog(log_level | LOG_DAEMON, fmt, ap);
        } else {
          ::vsyslog(log_level | LOG_DAEMON, fmtstr, ap);
        }
        break;
      case stdout_mode:
        if (::asprintf(
                &fmtstr, "[%s:%d:%s] nfusr-client: %s", file, line, func, fmt) <
            0) {
          ::vprintf(fmt, ap);
        } else {
          ::vprintf(fmtstr, ap);
        }
        break;
    }
  }

  ::free(fmtstr);
  va_end(ap);
}

void Logger::printf(const char* fmt, ...) {
  va_list ap;

  va_start(ap, fmt);
  ::vfprintf(fp_, fmt, ap);
  va_end(ap);
}

int Logger::flush() {
  return ::fflush(fp_);
}
};
