#!/bin/bash

host="$(hostname --fqdn)"
mountpoint=nfusr-int-cache-test-mount
volume=/tmp/nfusr-int-cache-test-volume
cachedir=/tmp/nfusr-int-cache-test-cache

function cleanup {
  fusermount -u "$(readlink -f $mountpoint)" >/dev/null 2>&1
  rm -rf $mountpoint
  rm -rf $cachedir
  rm -rf $volume
  if [ -n "$testvec" ]; then
     rm -f "$testvec"
  fi
}

function report_error {
   echo "Error on line $1 of $0"
}

trap cleanup EXIT

cleanup
mkdir $mountpoint
mkdir $volume
testvec=$(mktemp)

set -e
trap 'report_error $LINENO' ERR

./buck-out/gen/storage/gluster/nfusr/nfusr --log-level=9 --log-file=/tmp/nfusr-log "nfs://$host$volume" $mountpoint
dd if=/dev/urandom of="$testvec" bs=1M count=10
cp "$testvec" $mountpoint/funfun
mkdir $mountpoint/subdir
cp $mountpoint/funfun $mountpoint/subdir/
mv $mountpoint/subdir/funfun $mountpoint/subdir/somuchfun
ls -l $mountpoint/funfun >/dev/null 2>&1

fusermount -u "$(readlink -f $mountpoint)" >/dev/null 2>&1
rm -rf $mountpoint
mkdir $mountpoint
mkdir $cachedir
if [ -e /tmp/nfusr-cached-log ]; then
   rm -f /tmp/nfusr-cached-log
fi

./buck-out/gen/storage/gluster/nfusr/nfusr --log-level=9 --log-file=/tmp/nfusr-cached-log --cache-root=$cachedir "nfs://$host$volume" $mountpoint
sleep 10 # fails without sleep (takes time to setup)
ls -l $mountpoint/funfun >/dev/null 2>&1
cmp "$testvec" $mountpoint/funfun
cmp "$testvec" $mountpoint/funfun
cmp "$testvec" $mountpoint/funfun
cmp "$testvec" $mountpoint/funfun
cmp "$testvec" $mountpoint/subdir/somuchfun
cmp "$testvec" $mountpoint/subdir/somuchfun
cmp "$testvec" $mountpoint/subdir/somuchfun
cmp "$testvec" $mountpoint/subdir/somuchfun
ls -l $cachedir # >/dev/null 2>&1
# cat /tmp/nfusr-cached-log

if [ -e /tmp/nfusr-cached-log ]; then
   rm -f /tmp/nfusr-cached-log
fi

echo "Integration test passed."
exit 0
