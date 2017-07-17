#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([nfusr], [1.0.0], [kvigor@fb.com])
AC_CONFIG_SRCDIR([NfsConnection.cpp])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_LN_S
AX_CXX_COMPILE_STDCXX_14
# Checks for libraries.
AC_SEARCH_LIBS([fuse_mount], [fuse], [], AC_MSG_ERROR([required libfuse-dev not found]))
AC_SEARCH_LIBS([nfs_mkdir2], [nfs], [], AC_MSG_ERROR([required libnfs-dev >= 2.0 not found]))
AC_SEARCH_LIBS([pthread_create], [pthread], [], AC_MSG_ERROR([required libpthread not found]))

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h limits.h sys/time.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_TYPE_UINT64_T

# Checks for library functions.
AC_HEADER_MAJOR
AC_FUNC_REALLOC
AC_CHECK_FUNCS([localtime_r memset mkdir mkfifo rmdir strdup strerror])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
