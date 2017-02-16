#!/bin/bash

# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

if [ "$(which gluster 2>/dev/null)" ]; then
  sudo ./storage/gluster/nfusr/tests/integration_test.sh
else
  echo "Gluster not installed, test skipped."
fi
