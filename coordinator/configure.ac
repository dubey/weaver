#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([Weaver], [0.1], [dubey@cs.cornell.edu])
AM_INIT_AUTOMAKE
LT_INIT
AC_CONFIG_SRCDIR([symtable.c])
AC_CONFIG_HEADERS([config.h])
: ${CFLAGS=""}
: ${CXXFLAGS=""}

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lbusybee':
AC_CHECK_LIB([busybee], [main])
# FIXME: Replace `main' with a function in `-lchronos':
AC_CHECK_LIB([chronos], [main])
# FIXME: Replace `main' with a function in `-lreplicant':
AC_CHECK_LIB([replicant], [main])
# FIXME: Replace `main' with a function in `-lrt':
AC_CHECK_LIB([rt], [main])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h sys/time.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MALLOC
AC_CHECK_FUNCS([alarm memmove strtoull])

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
