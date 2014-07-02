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
#AM_CPPFLAGS=-I$(abs_top_srcdir)
AM_CPPFLAGS=
CYTHON_FLAGS=--cplus
LIBS=-lbusybee -le -pthread -lrt -lhyperdex-client -lchronos -lreplicant -lyaml -lpopt

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
include_HEADERS=
noinst_HEADERS=
sysconf_DATA=
EXTRA_DIST=

noinst_HEADERS+=	node_prog/base_classes.h \
					node_prog/edge_count_program.h \
					node_prog/n_hop_reach_program.h \
					node_prog/pathless_reach_program.h \
					node_prog/read_edges_props_program.h \
					node_prog/triangle_program.h \
					node_prog/cache_response.h \
					node_prog/edge_get_program.h \
					node_prog/node.h \
					node_prog/property.h \
					node_prog/read_n_edges_program.h \
					node_prog/two_neighborhood_program.h \
					node_prog/clustering_program.h \
					node_prog/edge.h \
					node_prog/node_program.h \
					node_prog/prop_list.h \
					node_prog/read_node_props_program.h \
					node_prog/dijkstra_program.h \
					node_prog/edge_list.h \
					node_prog/node_prog_type.h \
					node_prog/reach_program.h \
					node_prog/traverse_with_props.h \
					common/cache_constants.h \
					common/config_constants.h \
					common/hyper_stub_base.h \
					common/message.h \
					common/server.h \
					common/server_manager_returncode.h \
					common/weaver_constants.h \
					common/clock.h \
					common/configuration.h \
					common/ids.h \
					common/nmap_stub.h \
					common/server_manager_link.h \
					common/transaction.h \
					common/comm_wrapper.h \
					common/event_order.h \
					common/message_constants.h \
					common/serialization.h \
					common/server_manager_link_wrapper.h \
					common/vclock.h \
                    common/bool_vector.h

# timestamper
noinst_HEADERS+=	coordinator/current_prog.h \
					coordinator/current_tx.h \
					coordinator/hyper_stub.h  \
					coordinator/server_barrier.h  \
					coordinator/server_manager.h  \
					coordinator/timestamper.h  \
					coordinator/transitions.h  \
					coordinator/util.h
bin_PROGRAMS+=				weaver-timestamper
weaver_timestamper_SOURCES=	common/comm_wrapper.cc \
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
                            common/config_constants.cc \
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
		                    node_prog/traverse_with_props.cc \
		                    db/element.cc \
		                    db/property.cc \
		                    db/edge.cc \
		                    db/node.cc \
		                    coordinator/hyper_stub.cc \
							coordinator/timestamper.cc

# server manager
lib_LTLIBRARIES+=					libweaverservermanager.la
libweaverservermanager_la_SOURCES=	coordinator/server_barrier.cc \
									coordinator/server_manager.cc \
									coordinator/transitions.cc \
									coordinator/symtable.c \
									common/server.cc \
									common/ids.cc \
									common/serialization.cc
libweaverservermanager_la_CFLAGS= 	$(AM_CFLAGS)
libweaverservermanager_la_CXXFLAGS=	$(AM_CXXFLAGS)

# shard
noinst_HEADERS+=	db/cache_entry.h \
					db/del_obj.h \
					db/element.h \
					db/message_wrapper.h \
					db/nop_data.h \
					db/queued_request.h \
					db/remote_node.h \
					db/shard.h \
					db/deferred_write.h \
					db/edge.h \
					db/hyper_stub.h \
					db/node.h \
					db/property.h \
					db/queue_manager.h \
					db/shard_constants.h
bin_PROGRAMS+=	weaver-shard
weaver_shard_SOURCES=	common/ids.cc \
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
                        common/config_constants.cc \
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
		                node_prog/traverse_with_props.cc \
		                db/hyper_stub.cc \
		                db/queue_manager.cc \
		                db/element.cc \
		                db/property.cc \
		                db/edge.cc \
		                db/node.cc \
						db/shard.cc

# c++ client
noinst_HEADERS+=	client/client_constants.h \
					client/weaver_client.h
lib_LTLIBRARIES+=			libweaverclient.la
libweaverclient_la_SOURCES=	common/ids.cc \
		                    common/serialization.cc \
		                    common/server.cc \
		                    common/configuration.cc \
		                    common/comm_wrapper.cc \
		                    common/vclock.cc \
		                    common/message.cc \
		                    common/event_order.cc \
                            common/config_constants.cc \
                            common/server_manager_link.cc \
                            common/server_manager_link_wrapper.cc \
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
		                    node_prog/traverse_with_props.cc \
		                    db/element.cc \
		                    db/property.cc \
		                    client/client.cc
libweaverclient_la_CFLAGS=	$(AM_CFLAGS)
libweaverclient_la_CXXFLAGS=$(AM_CXXFLAGS)
include_HEADERS+=			client/weaver_client.h

# python client
EXTRA_DIST+=						bindings/python/__init__.py \
									bindings/python/client.pyx
pkgpyexec_DATA+=					bindings/python/__init__.py
pkgpyexec_LTLIBRARIES+=				bindings/python/client.la
CLEANFILES+=						bindings/python/client.cpp
bindings_python_client_la_SOURCES=	bindings/python/client.cpp
bindings_python_client_la_LIBADD=	libweaverclient.la
bindings/python/client.cpp:			bindings/python/client.pyx
	$(CYTHON) $(CYTHON_FLAGS) $<
bindings_python_client_la_CFLAGS= 	-fno-strict-aliasing $(AM_CFLAGS)
bindings_python_client_la_CXXFLAGS=	-fno-strict-aliasing $(AM_CXXFLAGS)
bindings_python_client_la_CPPFLAGS=	-I/usr/include/python2.7 $(AM_CPPFLAGS)
bindings_python_client_la_LDFLAGS=	-module
bindings_python_client_la_LIBS=		-lpython2.7 $(LIBS)

# configuration files
EXTRA_DIST+=	conf/weaver_shards.conf
EXTRA_DIST+=	conf/weaver.yaml
EXTRA_DIST+=	startup_scripts/start_weaver_support.sh
EXTRA_DIST+=	startup_scripts/kill_weaver_support.sh
EXTRA_DIST+=	startup_scripts/ps_weaver_support.sh
sysconf_DATA+=	conf/weaver_shards.conf
sysconf_DATA+=	conf/weaver.yaml

# python example programs
EXTRA_DIST+=	tests/python/assoc_count_basic.py
EXTRA_DIST+=	tests/python/delete_tx.py
EXTRA_DIST+=	tests/python/ft_shard_deletes.py
EXTRA_DIST+=	tests/python/multiline_cache_testing.py
EXTRA_DIST+=	tests/python/read_properties.py
EXTRA_DIST+=	tests/python/test_base.py
EXTRA_DIST+=	tests/python/assoc_range_basic.py
EXTRA_DIST+=	tests/python/deletion.py
EXTRA_DIST+=	tests/python/graph_of_gods_demo.py
EXTRA_DIST+=	tests/python/nwx_reachability.py
EXTRA_DIST+=	tests/python/sequential_pathless_reachability_bench.py
EXTRA_DIST+=	tests/python/clustering_basic_test.py
EXTRA_DIST+=	tests/python/demo.py
EXTRA_DIST+=	tests/python/old_reachability_bench.py
EXTRA_DIST+=	tests/python/transactions.py
EXTRA_DIST+=	tests/python/clustering_bench.py
EXTRA_DIST+=	tests/python/dijkstra_basic_test.py
EXTRA_DIST+=	tests/python/line_cache_testing.py
EXTRA_DIST+=	tests/python/reachability_bench.py
EXTRA_DIST+=	tests/python/simple_client.py
EXTRA_DIST+=	tests/python/clustering_migration.py
EXTRA_DIST+=	tests/python/empty_graph_sanity_checks.py
EXTRA_DIST+=	tests/python/line_properties.py
EXTRA_DIST+=	tests/python/reachable_migration.py
EXTRA_DIST+=	tests/python/soc_net_bench.py
EXTRA_DIST+=	tests/python/two_neighborhood_bench.py
EXTRA_DIST+=	tests/python/create_graph.py
EXTRA_DIST+=	tests/python/ft_shard_basic.py
EXTRA_DIST+=	tests/python/line_reachability.py
EXTRA_DIST+=	tests/python/read_only_vertex_bench.py
EXTRA_DIST+=	tests/python/star_cache_testing.py
EXTRA_DIST+=	tests/python/two_neighborhood_testing.py
