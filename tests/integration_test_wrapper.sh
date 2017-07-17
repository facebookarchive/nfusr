#!/bin/bash
if [ "$(which gluster 2>/dev/null)" ]; then
  sudo ./storage/gluster/nfusr/tests/integration_test.sh
else
  echo "Gluster not installed, test skipped."
fi
