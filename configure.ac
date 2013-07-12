dnl configure.ac for memtier_benchmark
dnl Copyright (C) 2011-2013 Garantia Data Ltd.

dnl This file is part of memtier_benchmark.

dnl memtier_benchmark is free software: you can redistribute it and/or modify
dnl it under the terms of the GNU General Public License as published by
dnl the Free Software Foundation, version 2.

dnl This program is distributed in the hope that it will be useful,
dnl but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl GNU General Public License for more details.

dnl You should have received a copy of the GNU General Public License
dnl along with this program.  If not, see <http://www.gnu.org/licenses/>.

AC_PREREQ(2.59)
AC_INIT(memtier_benchmark,1.0.2,yossigo@gmail.com)
AC_CONFIG_SRCDIR([memtier_benchmark.cpp])
AC_CONFIG_HEADER([config.h])
AM_INIT_AUTOMAKE
LT_INIT

CXXFLAGS="-O2 -g -Wall"

# Checks for programs.
AC_PROG_CXX

# Checks for header files.
AC_HEADER_STDC
AC_HEADER_DIRENT
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h getopt.h limits.h malloc.h stdlib.h unistd.h utime.h assert.h sys/socket.h sys/types.h])
AC_CHECK_HEADERS([fcntl.h netinet/tcp.h])
AC_CHECK_HEADERS([pthread.h])
AC_CHECK_HEADERS([pcre.h zlib.h])
AC_CHECK_HEADERS([event2/event.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STAT
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_CHECK_FUNCS([gettimeofday memchr memset socket strerror random_r drand48])

AC_CHECK_LIB([pcre], [pcre_compile], , AC_MSG_ERROR([pcre is required; try installing libpcre3-dev.]))
AC_CHECK_LIB([z], [deflateInit_], , AC_MSG_ERROR([zlib is required; try installing zlib-dev.]))
AC_CHECK_LIB([rt], [clock_gettime], , AC_MSG_ERROR([rt is required for libevent.]))
AC_CHECK_LIB([pthread], [pthread_create], , AC_MSG_ERROR([pthread is required.]))
AC_CHECK_LIB([socket], [gai_strerror])

# libevent
PKG_CHECK_MODULES(LIBEVENT,
    [libevent >= 2.0.10],
    AC_SUBST(LIBEVENT_CFLAGS) AC_SUBST(LIBEVENT_LIBS)
)

AC_CONFIG_FILES([
   	Makefile
])
AC_OUTPUT
