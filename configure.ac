#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.66])
AC_INIT([chronos], [0.1.dev], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-bzip2 dist-lzma])
AM_SILENT_RULES([yes])
AM_PATH_PYTHON([2.6])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([chronos.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG(C++)

# Checks for libraries.
AC_PYTHON_DEVEL([>= 2.6])
AC_CHECK_LIB([popt], [poptGetContext], [], [AC_MSG_ERROR([
-------------------------------------------------
Chronos relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])

# Checks for header files.
AC_CHECK_HEADERS([stdint.h stdlib.h])
AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
-------------------------------------------------
Chronos relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
PKG_CHECK_MODULES([E], [libe >= 0.2])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
