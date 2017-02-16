EXTRA_DIST=bootstrap LICENSE PATENTS

# mount helper scripts must go in /sbin, regardless of
# prefix
root_sbindir="/sbin"

bin_PROGRAMS = nfusr
nfusr_SOURCES = ClientStats.cpp fuse_optype.cpp logger.cpp NfsClient.cpp \
                NfsConnection.cpp NfsConnectionPool.cpp nfusr.cpp \
				NfsCachedClient.cpp \
				ClientStats.h InodeInternal.h NfsClient.h NfsConnectionPool.h \
                fuse_optype.h logger.h NfsConnection.h RpcContext.h \
				NfsCachedClient.h

root_sbin_SCRIPTS = mount.nfusr
CLEANFILES=mount.nfusr mount.nfusr-t
EXTRA_DIST+=mount.nfusr.in
mount.nfusr: mount.nfusr.in
	@rm -f $@-t $@
	@sed -e 's|@bindir[@]|$(bindir)|g' <$< >$@-t && chmod a-w $@-t && mv $@-t $@
