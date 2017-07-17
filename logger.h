/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <syslog.h>
#include <cstdio>
#include <iostream>

#define LOG_MSG(fmt, ...) \
  log_msg(__FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

namespace nfusr {
class Logger {
 public:
  Logger();
  ~Logger();

  void log_msg(
      const char* file,
      int line,
      const char* func,
      int log_level,
      const char* fmt,
      ...) __attribute__((__format__(__printf__, 6, 7)));

  void printf(const char* fmt, ...)
      __attribute__((__format__(__printf__, 2, 3)));

  int openFile(const char* name, bool fifoMode, bool autoFlush);
  void setMask(int mask) {
    mask_ = mask;
  }
  void syslogMode() {
      mode_ = syslog_mode;
  }

  int flush();

 private:
  FILE* fp_;
  int mask_;
  enum { stdout_mode, file_mode, syslog_mode } mode_;
  bool autoFlush_;
};
};
