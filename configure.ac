#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([Weaver], [0.2.0], [dubey@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign subdir-objects])
AM_SILENT_RULES([yes])
LT_INIT
AM_PATH_PYTHON([2.6])
AC_CONFIG_SRCDIR([db/shard.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])
: ${CFLAGS=""}
: ${CXXFLAGS=""}

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

# Checks for libraries.
AC_PYTHON_DEVEL([>= 2.7])
AC_CHECK_LIB([rt], [clock_gettime], [], [AC_MSG_ERROR([
-----------------------------------------------------------
Weaver requires the rt library; please install to continue.
-----------------------------------------------------------])])
AC_CHECK_LIB([popt], [poptGetContext], [], [AC_MSG_ERROR([
-------------------------------------------------------------
Weaver requires the popt library; please install to continue.
-------------------------------------------------------------])])


PKG_CHECK_MODULES([PO6], [libpo6 >= 0.5])
PKG_CHECK_MODULES([E], [libe >= 0.7])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.5])
PKG_CHECK_MODULES([HYPERLEVELDB], [libhyperleveldb >= 1.0])
PKG_CHECK_MODULES([REPLICANT], [replicant >= 0.6])
PKG_CHECK_MODULES([HYPERDEX], [hyperdex-client >= 1.0])

# Checks for header files.
AC_CHECK_HEADERS([limits.h mach/mach.h stdint.h stdlib.h string.h sys/time.h])
#AC_CHECK_HEADER([glog/logging.h],,[AC_MSG_ERROR([
#-------------------------------------------------------------
#Weaver requires the glog library; please install to continue.
#-------------------------------------------------------------])])
AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
-------------------------------------------------------------
Weaver requires the popt library; please install to continue.
-------------------------------------------------------------])])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MALLOC
AC_CHECK_FUNCS([alarm clock_gettime memmove memset strtoull])

AC_ARG_ENABLE(debug,
AS_HELP_STRING([--enable-debug],
    [enable debugging, default: no]),
[case "${enableval}" in
    yes) debug=true ;;
    no)  debug=false ;;
    *)   AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;; esac],
[debug=false])

AM_CONDITIONAL(DEBUG, test x"$debug" = x"true")

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
