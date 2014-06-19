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
pkgpyexec_DATA=
CLEANFILES=

# timestamper
bin_PROGRAMS+=		timestamper
timestamper_SOURCES=common/comm_wrapper.cc \
                    common/configuration.cc \
                    common/server.cc \
                    common/ids.cc \
                    common/serialization.cc \
                    common/server_manager_link.cc \
                    common/server_manager_link_wrapper.cc \
                    common/hyper_stub_base.cc \
                    common/event_order.cc \
                    common/nmap_stub.cc \
                    common/vclock.cc \
                    common/message.cc \
                    node_prog/edge_list.cc \
                    node_prog/prop_list.cc \
                    node_prog/reach_program.cc \
                    node_prog/clustering_program.cc \
                    node_prog/pathless_reach_program.cc \
                    node_prog/two_neighborhood_program.cc \
                    node_prog/edge_count_program.cc \
                    node_prog/edge_get_program.cc \
                    node_prog/read_edges_props_program.cc \
                    node_prog/read_n_edges_program.cc \
                    node_prog/read_node_props_program.cc \
                    db/element.cc \
                    db/property.cc \
                    db/edge.cc \
                    db/node.cc \
                    coordinator/hyper_stub.cc \
					coordinator/timestamper.cc
#timestamper_CFLAGS= 	$(AM_CFLAGS)
#timestamper_CXXFLAGS= 	$(AM_CXXFLAGS)

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
                common/comm_wrapper.cc \
				common/server_manager_link.cc \
				common/server_manager_link_wrapper.cc \
                common/hyper_stub_base.cc \
                common/event_order.cc \
                common/nmap_stub.cc \
                common/clock.cc \
                common/vclock.cc \
                common/message.cc \
                common/message_graph_elem.cc \
                common/message_cache_context.cc \
                node_prog/edge_list.cc \
                node_prog/prop_list.cc \
                node_prog/reach_program.cc \
                node_prog/clustering_program.cc \
                node_prog/pathless_reach_program.cc \
                node_prog/two_neighborhood_program.cc \
                node_prog/edge_count_program.cc \
                node_prog/edge_get_program.cc \
                node_prog/read_edges_props_program.cc \
                node_prog/read_n_edges_program.cc \
                node_prog/read_node_props_program.cc \
                db/hyper_stub.cc \
                db/queue_manager.cc \
                db/element.cc \
                db/property.cc \
                db/edge.cc \
                db/node.cc \
				db/shard.cc
#shard_CFLAGS= 	$(AM_CFLAGS)
#shard_CXXFLAGS=	$(AM_CXXFLAGS)

# python client
pkgpyexec_DATA+=	bindings/python/__init__.py
pkgpyexec_LTLIBRARIES+=	libclient.la
CLEANFILES+=	bindings/python/libclient.cpp
libclient_la_SOURCES=	common/ids.cc \
                        common/serialization.cc \
                        common/server.cc \
                        common/configuration.cc \
                        common/comm_wrapper.cc \
                        common/vclock.cc \
                        common/message.cc \
                        common/event_order.cc \
                        node_prog/prop_list.cc \
                        node_prog/edge_list.cc \
                        node_prog/clustering_program.cc \
                        node_prog/edge_count_program.cc \
                        node_prog/edge_get_program.cc \
                        node_prog/pathless_reach_program.cc \
                        node_prog/reach_program.cc \
                        node_prog/read_edges_props_program.cc \
                        node_prog/read_n_edges_program.cc \
                        node_prog/read_node_props_program.cc \
                        node_prog/two_neighborhood_program.cc \
                        db/element.cc \
                        db/property.cc \
                        client/client.cc \
                        bindings/python/libclient.cpp
bindings/python/libclient.cpp:	bindings/python/libclient.pyx
	$(CYTHON) $(CYTHON_FLAGS) $<
libclient_la_CFLAGS= 	-fno-strict-aliasing $(AM_CFLAGS)
libclient_la_CXXFLAGS=	-fno-strict-aliasing $(AM_CXXFLAGS)
libclient_la_CPPFLAGS=	-I/usr/include/python2.7
libclient_la_LIBS=		-lpython2.7 $(LIBS)

# cpp tests
bin_PROGRAMS+=				tests/cpp/tests
tests_cpp_tests_SOURCES=	tests/cpp/run.cc \
							common/clock.cc \
                            common/message.cc
#tests_cpp_tests_CFLAGS= 	$(AM_CFLAGS)
#tests_cpp_tests_CXXFLAGS= 	$(AM_CXXFLAGS)
