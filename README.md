# nfusr

nfusr is a userspace FUSE client for accessing NFSv3 servers based on
libnfs.

## Usage

NFS filesystems can be directly mounted with nfusr:

    nfusr nfs://nfsserver.example.com/volume /mntpoint

(see libnfs documentation for the format of the nfs:// URI).

Alternatively one can mount using typical mount syntax (using the
helper script installed in /sbin/mount.nfusr):

    mount -t nfusr nfs://nfsserver.example.com/volume /mntpoint

which enables use of automount, etc.

In either case, FUSE mount options can be specified by adding a '-o' option,
e.g.:

    mount -t nfusr nfs://nfsserver.example.com/volume /mntpoint -o allow_other,max_read=128K

(see FUSE documentation for available FUSE mount options).

Note that NFSv4 is not supported in any way.

## Building

To build from source:

    ./bootstrap
    ./configure
    make
    sudo make install

## Debugging and output

By default nfusr deamonizes itself and messages are written to syslog.
Alternatively output can be directed to a file using the --log-file
option, e.g.

    nfusr --log-file=/tmp/nfusr.log nfs://server/vol /mntpoint

In all instances the '--log-level' option controls the verbosity of the
output using the levels defined in setlogmask(3) (i.e. --log-level=7
yields maximum verbosity).

The '-f' option forces nfusr to run in the foregound. In this case
output is written to stdout.

The '-d' option enables libfuse debugging and implies -f.

## Multi-server support

A novel feature of nfusr is support for multiple servers hosting the same
volume, with round-robin load balancing and failover.

To mount a volume using multiple servers, specify multiple NFS URLs on the
command line, and optionally specify the max_conn mount option. For example:

    mount -t nfusr nfs://server1/vol,nfs://server2/vol /mntpoint

or:

    nfusr nfs://server1/vol nfs://server2/vol nfs://server3/vol /mntpoint -o max_conn=2

When multiple servers are specified, nfusr will attempt to establish
connections to max_conn servers (default 1) and round-robin requests amongst
those connections.

If connection to a server is lost during a request, nfusr will attempt to
connect to the next server and resend the request, thus failing over.

This feature comes with several caveats: first and foremost, the specified
servers must be serving exactly the same volume, identical down to the
structure of the NFS file handle (which is usually opaque). The only system
against which this is known to reliably work at this time is Red Hat's Gluster
distributed filesystem.

Other important caveats:

* fsync etc. should be considered broken in round-robin mode (currently the
  commit is sent to only one server, meaning writes sent to any others are
  not necessarily flushed despite a successful return).

* failover of non-idempotent requests can yield spurious errors. For example,
  if an unlink request is sent to a server and succeeds, but the connection is
  lost before the response is received by nfusr, the request will be replayed
  to another server. Since the file has actually been unlinked, the replayed 
  request will now fail with ENOENT.

In short, multiple server mode is recommended only if you really, really know
what you're doing.

## License

nfusr is BSD-licensed. We also provide an additional patent grant.
