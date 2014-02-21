# 
# ===============================================================
#    Description:  Automake Makefile for Weaver.
# 
#        Created:  2014-02-18 12:00:00
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2014, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

CC=gcc-4.8
CXX=g++-4.8
CYTHON=cython

AM_CFLAGS=-Wall -Wextra
AM_CXXFLAGS=-std=c++0x -Wall -Wextra
CYTHON_FLAGS=--cplus
LIBS=-lbusybee -le -pthread -lrt -lhyperdex-client -lchronos -lreplicant

if DEBUG
    AM_CFLAGS += -g3 -gdwarf-2 -Og
    AM_CXXFLAGS += -g3 -gdwarf-2 -Og
else
    AM_CFLAGS += -O2
    AM_CXXFLAGS += -O2
endif

bin_PROGRAMS=
lib_LTLIBRARIES=
pkgpyexec_LTLIBRARIES=

# timestamper
bin_PROGRAMS+=		timestamper
timestamper_SOURCES=common/comm_wrapper.cc \
                    common/configuration.cc \
                    common/server.cc \
                    common/ids.cc \
                    common/serialization.cc \
					coordinator/timestamper.cc
timestamper_CFLAGS= 	$(AM_CFLAGS)
timestamper_CXXFLAGS= 	$(AM_CXXFLAGS)

# server manager
lib_LTLIBRARIES+=			libservermanager.la
libservermanager_la_SOURCES=coordinator/server_barrier.cc \
							coordinator/server_manager.cc \
							coordinator/transitions.cc \
							coordinator/symtable.c \
							common/server.cc \
							common/ids.cc \
							common/serialization.cc
libservermanager_la_CFLAGS= 	$(AM_CFLAGS)
libservermanager_la_CXXFLAGS= 	$(AM_CXXFLAGS)

# shard
bin_PROGRAMS+=	shard
shard_SOURCES=	common/ids.cc \
				common/serialization.cc \
				common/server.cc \
				common/configuration.cc \
				common/server_manager_link.cc \
                common/comm_wrapper.cc \
				db/server_manager_link_wrapper.cc \
                db/hyper_stub.cc \
                db/queue_manager.cc \
				db/shard.cc
shard_CFLAGS= 	$(AM_CFLAGS)
shard_CXXFLAGS=	$(AM_CXXFLAGS)

# python client
pkgpyexec_LTLIBRARIES+=	libclient.la
libclient_la_SOURCES=	bindings/python/client.cpp
bindings/python/client.cpp:	bindings/python/client.pyx
	$(CYTHON) $(CYTHON_FLAGS) $<
libclient_la_CFLAGS= 	-fno-strict-aliasing $(AM_CFLAGS)
libclient_la_CXXFLAGS=	-fno-strict-aliasing $(AM_CXXFLAGS)
libclient_la_CPPFLAGS=	-I/usr/include/python2.7
libclient_la_LIBS=		-lpython2.7

# cpp tests
bin_PROGRAMS+=				tests/cpp/tests
tests_cpp_tests_SOURCES=	tests/cpp/run.cc
tests_cpp_tests_CFLAGS= 	$(AM_CFLAGS)
tests_cpp_tests_CXXFLAGS= 	$(AM_CXXFLAGS)
