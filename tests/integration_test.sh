#!/bin/bash

# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

host="$(hostname --fqdn)"
volume=nfusr-test
brickdir=/tmp/$volume
mountpoint=/tmp/$volume-mnt

function cleanup {
  umount $mountpoint >/dev/null 2>&1
  gluster --mode=script volume stop $volume >/dev/null 2>&1
  gluster --mode=script volume delete $volume >/dev/null 2>&1
  rm -rf $brickdir >/dev/null 2>&1
  rm -rf $mountpoint >/dev/null 2>&1
  if [ -n "$testvec" ]; then
     rm -f "$testvec" >/dev/null 2>&1
  fi
}

function report_error {
   echo "Error on line $1 of $0"
}

trap cleanup EXIT

/etc/init.d/glusterd start
cleanup
mkdir $brickdir
mkdir $mountpoint
testvec=$(mktemp)
gluster --mode=script volume create $volume "$host:$brickdir" force
gluster --mode=script volume start $volume

if ! timeout 30 bash -c "while ! showmount -e | grep -q $volume; do sleep 1; done"; then
  echo "NFS volume $volume did not become available within timeout."
  exit 1
fi

set -e
trap 'report_error $LINENO' ERR

./buck-out/gen/storage/gluster/nfusr/nfusr --error-injection -s "nfs://$host/$volume" $mountpoint
dd if=/dev/urandom of="$testvec" bs=1M count=10
cp "$testvec" $mountpoint/funfun
cmp "$testvec" $mountpoint/funfun
mkdir $mountpoint/subdir
cp $mountpoint/funfun $mountpoint/subdir/
mv $mountpoint/subdir/funfun $mountpoint/subdir/somuchfun
cmp $mountpoint/funfun $mountpoint/subdir/somuchfun
ln $mountpoint/subdir/somuchfun $mountpoint/hardlink
ln -s $mountpoint/subdir/somuchfun $mountpoint/softlink
cmp $mountpoint/hardlink $mountpoint/funfun
cmp $mountpoint/hardlink $mountpoint/softlink
ls -l $mountpoint/funfun >/dev/null
chmod a+s $mountpoint/subdir/somuchfun
[ -u $mountpoint/subdir/somuchfun ]
ls -l $mountpoint/subdir >/dev/null
rm -rf $mountpoint/subdir

echo "Integration test passed."
exit 0
