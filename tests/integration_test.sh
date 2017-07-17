#!/bin/bash

host="$(hostname --fqdn)"
volume=nfusr-test
brickdir=/tmp/$volume
mountpoint=/tmp/$volume-mnt

function cleanup {
  umount -lf --no-canonicalize $mountpoint >/dev/null 2>&1 || true
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

service glusterd start
cleanup
mkdir $brickdir
mkdir $mountpoint
testvec=$(mktemp)
gluster --mode=script volume create $volume "$host:$brickdir" force
gluster --mode=script volume set $volume nfs.disable off
gluster --mode=script volume start $volume

systemctl stop nfs
systemctl start glusterd-nfsd

if ! timeout 30 bash -c "while ! showmount -e | grep -q $volume; do sleep 1; done"; then
  echo "NFS volume $volume did not become available within timeout."
  exit 1
fi

set -e
trap 'report_error $LINENO' ERR

NFUSR_CMD="./buck-out/gen/storage/gluster/nfusr/nfusr -omaxerrorretries=20 --error-injection \
    -operm_mode=strict \
    nfs://bogus.host/$volume nfs://$host/$volume nfs://$host/$volume $mountpoint"
if [ -n "$DBG_LEVEL" ]; then
  NFUSR_CMD="$NFUSR_CMD --log-level=$DBG_LEVEL --log-file=/tmp/nfusr.log"
fi
if [ -n "$USE_VALGRIND" ]; then
    valgrind --trace-children=yes --trace-children-skip='*/mount' \
               --log-file=/tmp/valgrind.log --error-exitcode=99 \
    $NFUSR_CMD
else
    $NFUSR_CMD
fi

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
umount $mountpoint
if [ -n "$USE_VALGRIND" ]; then
  if grep Invalid /tmp/valgrind.log; then
   echo 'Valgrind sadness!'
   exit 1
  fi
fi

echo "Integration test passed."
exit 0
