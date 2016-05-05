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
AM_CPPFLAGS=-I$(top_srcdir)/client
CYTHON_FLAGS=--cplus
LIBS=-lpo6 -le -lbusybee -pthread -lrt -lhyperdex-client -lreplicant -lyaml -lpopt -lglog -lpugixml

if DEBUG
    AM_CFLAGS += -g3 -gdwarf-2 -O0
    AM_CXXFLAGS += -g3 -gdwarf-2 -O0
else
    AM_CFLAGS += -O2
    AM_CXXFLAGS += -O2
endif

pkgpyexecdir = $(pyexecdir)/weaver
weaverincludedir = $(includedir)/weaver

bin_PROGRAMS=
lib_LTLIBRARIES=
pkgpyexec_LTLIBRARIES=
pkgpyexec_DATA=
CLEANFILES=
weaverinclude_HEADERS=
noinst_HEADERS=
sysconf_DATA=
TESTS =
TESTS_ENVIRONMENT= . $(abs_top_srcdir)/tests/sh/env.sh "${abs_top_srcdir}" "${abs_top_builddir}";
EXTRA_DIST=LICENSE

weaverexecdir = $(libexecdir)/$(PACKAGE)-$(VERSION)
weaverheaderdir = $(includedir)/weaver
pkgconfigdir = $(libdir)/pkgconfig

noinst_HEADERS+=	node_prog/base_classes.h \
					node_prog/edge_count_program.h \
					node_prog/n_hop_reach_program.h \
					node_prog/pathless_reach_program.h \
					node_prog/triangle_program.h \
					node_prog/cache_response.h \
					node_prog/edge_get_program.h \
					node_prog/node_get_program.h \
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
					node_prog/discover_paths.h \
					node_prog/parse_btc_tx.h \
					node_prog/cause_and_effect.h \
					node_prog/n_gram_path.h \
					node_prog/get_btc_block.h \
					node_prog/get_btc_tx.h \
					node_prog/get_btc_addr.h \
					common/cache_constants.h \
					common/config_constants.h \
					common/hyper_stub_base.h \
					common/stl_serialization.h \
					common/weaver_serialization.h \
					common/enum_serialization.h \
					common/message.h \
					common/server.h \
					common/server_manager_returncode.h \
					common/weaver_constants.h \
					common/clock.h \
					common/configuration.h \
					common/ids.h \
					common/server_manager_link.h \
					common/transaction.h \
					common/comm_wrapper.h \
					common/event_order.h \
					common/message_constants.h \
					common/server_manager_link_wrapper.h \
					common/vclock.h \
                    common/bool_vector.h \
                    common/types.h \
                    common/utils.h \
                    common/MurmurHash3.h \
                    common/property_predicate.h

# timestamper
noinst_HEADERS+=			coordinator/current_prog.h \
							coordinator/blocked_prog.h \
							coordinator/hyper_stub.h  \
							coordinator/server_barrier.h  \
							coordinator/server_manager.h  \
							coordinator/timestamper.h  \
							coordinator/transitions.h  \
							coordinator/util.h \
							coordinator/vt_constants.h
bin_PROGRAMS+=				weaver-timestamper
weaver_timestamper_SOURCES=	common/comm_wrapper.cc \
		                    common/configuration.cc \
		                    common/server.cc \
		                    common/ids.cc \
		                    common/server_manager_link.cc \
		                    common/server_manager_link_wrapper.cc \
		                    common/hyper_stub_base.cc \
		                    common/event_order.cc \
		                    common/clock.cc \
		                    common/vclock.cc \
                            common/transaction.cc \
		                    common/stl_serialization.cc \
		                    common/weaver_serialization.cc \
		                    common/enum_serialization.cc \
		                    common/message.cc \
		                    common/message_graph_elem.cc \
                            common/config_constants.cc \
                            common/MurmurHash3.cpp \
                            common/property_predicate.cc \
							chronos/chronos.cc \
							chronos/chronos_c_wrappers.cc \
							chronos/chronos_cmp_encode.cc \
		                    node_prog/edge_list.cc \
		                    node_prog/prop_list.cc \
		                    node_prog/reach_program.cc \
		                    node_prog/clustering_program.cc \
		                    node_prog/pathless_reach_program.cc \
		                    node_prog/two_neighborhood_program.cc \
		                    node_prog/edge_count_program.cc \
		                    node_prog/edge_get_program.cc \
		                    node_prog/node_get_program.cc \
		                    node_prog/read_n_edges_program.cc \
		                    node_prog/read_node_props_program.cc \
		                    node_prog/traverse_with_props.cc \
		                    node_prog/discover_paths.cc \
		                    node_prog/parse_btc_tx.cc \
							node_prog/cause_and_effect.cc \
							node_prog/n_gram_path.cc \
		                    node_prog/get_btc_block.cc \
		                    node_prog/get_btc_tx.cc \
		                    node_prog/get_btc_addr.cc \
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
									common/ids.cc
libweaverservermanager_la_CFLAGS= 	$(AM_CFLAGS)
libweaverservermanager_la_CXXFLAGS=	$(AM_CXXFLAGS)

# shard
noinst_HEADERS+=		db/cache_entry.h \
						db/del_obj.h \
						db/element.h \
						db/message_wrapper.h \
						db/queued_request.h \
						db/remote_node.h \
						db/shard.h \
						db/deferred_write.h \
						db/edge.h \
						db/hyper_stub.h \
						db/node.h \
						db/property.h \
						db/queue_manager.h \
						db/shard_constants.h \
						db/utils.h \
						db/types.h \
						db/node_entry.h
bin_PROGRAMS+=			weaver-shard
weaver_shard_SOURCES=	common/ids.cc \
						common/server.cc \
						common/configuration.cc \
		                common/comm_wrapper.cc \
						common/server_manager_link.cc \
						common/server_manager_link_wrapper.cc \
		                common/hyper_stub_base.cc \
		                common/event_order.cc \
		                common/clock.cc \
		                common/vclock.cc \
                        common/transaction.cc \
		                common/stl_serialization.cc \
		                common/weaver_serialization.cc \
		                common/enum_serialization.cc \
		                common/message.cc \
		                common/message_graph_elem.cc \
		                common/message_cache_context.cc \
                        common/config_constants.cc \
                        common/MurmurHash3.cpp \
                        common/property_predicate.cc \
						chronos/chronos.cc \
						chronos/chronos_c_wrappers.cc \
						chronos/chronos_cmp_encode.cc \
		                node_prog/edge_list.cc \
		                node_prog/prop_list.cc \
		                node_prog/reach_program.cc \
		                node_prog/clustering_program.cc \
		                node_prog/pathless_reach_program.cc \
		                node_prog/two_neighborhood_program.cc \
		                node_prog/edge_count_program.cc \
		                node_prog/edge_get_program.cc \
                        node_prog/node_get_program.cc \
		                node_prog/read_n_edges_program.cc \
		                node_prog/read_node_props_program.cc \
		                node_prog/traverse_with_props.cc \
		                node_prog/discover_paths.cc \
						node_prog/parse_btc_tx.cc \
						node_prog/cause_and_effect.cc \
						node_prog/n_gram_path.cc \
		                node_prog/get_btc_block.cc \
		                node_prog/get_btc_tx.cc \
		                node_prog/get_btc_addr.cc \
		                db/hyper_stub.cc \
		                db/queue_manager.cc \
		                db/element.cc \
		                db/property.cc \
		                db/edge.cc \
		                db/node.cc \
						db/shard.cc

# c++ client
noinst_HEADERS+=			client/client_constants.h \
							client/comm_wrapper.h
lib_LTLIBRARIES+=			libweaverclient.la
libweaverclient_la_SOURCES=	common/ids.cc \
		                    common/server.cc \
		                    common/configuration.cc \
		                    common/comm_wrapper.cc \
		                    common/vclock.cc \
                            common/transaction.cc \
		                    common/stl_serialization.cc \
		                    common/weaver_serialization.cc \
		                    common/enum_serialization.cc \
		                    common/message.cc \
		                    common/event_order.cc \
                            common/config_constants.cc \
                            common/server_manager_link.cc \
                            common/server_manager_link_wrapper.cc \
                            common/MurmurHash3.cpp \
                            common/property_predicate.cc \
							chronos/chronos.cc \
							chronos/chronos_c_wrappers.cc \
							chronos/chronos_cmp_encode.cc \
		                    node_prog/prop_list.cc \
		                    node_prog/edge_list.cc \
		                    node_prog/clustering_program.cc \
		                    node_prog/edge_count_program.cc \
		                    node_prog/edge_get_program.cc \
		                    node_prog/node_get_program.cc \
		                    node_prog/pathless_reach_program.cc \
		                    node_prog/reach_program.cc \
		                    node_prog/read_n_edges_program.cc \
		                    node_prog/read_node_props_program.cc \
		                    node_prog/two_neighborhood_program.cc \
		                    node_prog/traverse_with_props.cc \
		                    node_prog/discover_paths.cc \
		                    node_prog/parse_btc_tx.cc \
							node_prog/cause_and_effect.cc \
							node_prog/n_gram_path.cc \
		                    node_prog/get_btc_block.cc \
		                    node_prog/get_btc_tx.cc \
		                    node_prog/get_btc_addr.cc \
		                    db/element.cc \
		                    db/property.cc \
		                    client/comm_wrapper.cc \
		                    client/weaver_returncode.cc \
		                    client/client.cc \
		                    client/c.cc
libweaverclient_la_CFLAGS=	$(AM_CFLAGS)
libweaverclient_la_CXXFLAGS=$(AM_CXXFLAGS)
weaverinclude_HEADERS+=		client/client.h \
							client/datastructures.h \
							client/weaver/weaver_returncode.h \
							client/weaver/weaver_client.h

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
EXTRA_DIST+=	conf/weaver.yaml
EXTRA_DIST+=	startup_scripts/start_weaver.sh
EXTRA_DIST+=	startup_scripts/kill_weaver.sh
EXTRA_DIST+=	startup_scripts/ps_weaver.sh
sysconf_DATA+=	conf/weaver.yaml

# python example programs
EXTRA_DIST+=	tests/python/test_base.py
EXTRA_DIST+=	tests/python/demo.py
EXTRA_DIST+=	tests/python/simple_client.py
EXTRA_DIST+=	tests/python/create_graph.py
EXTRA_DIST+=	tests/python/assoc_count_basic.py
EXTRA_DIST+=	tests/python/assoc_range_basic.py
EXTRA_DIST+=	tests/python/correctness/simple_test.py
EXTRA_DIST+=	tests/python/correctness/simple_test_aux_index.py
EXTRA_DIST+=	tests/python/correctness/delete_tx.py
EXTRA_DIST+=	tests/python/correctness/transactions.py
EXTRA_DIST+=	tests/python/correctness/transactions_ring.py
EXTRA_DIST+=	tests/python/correctness/concurrent_clients.py
EXTRA_DIST+=	tests/python/correctness/multiple_del.py
EXTRA_DIST+=	tests/python/correctness/ft_shard_deletes.py
EXTRA_DIST+=	tests/python/correctness/multiline_cache_testing.py
EXTRA_DIST+=	tests/python/correctness/read_properties.py
EXTRA_DIST+=	tests/python/correctness/line_properties.py
EXTRA_DIST+=	tests/python/correctness/ft_shard_basic.py
EXTRA_DIST+=	tests/python/correctness/line_reachability.py
EXTRA_DIST+=	tests/python/correctness/clustering_basic_test.py
EXTRA_DIST+=	tests/python/correctness/dijkstra_basic_test.py
EXTRA_DIST+=	tests/python/correctness/empty_graph_sanity_checks.py
EXTRA_DIST+=	tests/python/correctness/two_neighborhood_testing.py
EXTRA_DIST+=	tests/python/benchmarks/nwx_reachability.py
EXTRA_DIST+=	tests/python/benchmarks/sequential_pathless_reachability_bench.py
EXTRA_DIST+=	tests/python/benchmarks/clustering_bench.py
EXTRA_DIST+=	tests/python/benchmarks/reachability_bench.py
EXTRA_DIST+=	tests/python/benchmarks/clustering_migration.py
EXTRA_DIST+=	tests/python/benchmarks/reachable_migration.py
EXTRA_DIST+=	tests/python/benchmarks/soc_net_bench.py
EXTRA_DIST+=	tests/python/benchmarks/two_neighborhood_bench.py
EXTRA_DIST+=	tests/python/benchmarks/read_only_vertex_bench.py

bin_PROGRAMS+=					weaver-parse-config
weaver_parse_config_SOURCES=	common/config_constants.cc \
								startup_scripts/parse_config.cc

# chronos
noinst_HEADERS+=	chronos/chronos_cmp_encode.h \
					chronos/chronos_stats_encode.h \
					chronos/event_dependency_graph.h \
					chronos/network_constants.h \
					chronos/chronos.h

lib_LTLIBRARIES+=				libweaverchronosd.la
libweaverchronosd_la_SOURCES=	common/config_constants.cc \
								common/stl_serialization.cc \
								chronos/chronos_cmp_encode.cc \
		        				chronos/chronos_stats_encode.cc \
		        				chronos/chronosd.cc \
		        				chronos/event_dependency_graph.cc \
		        				chronos/replicant-shim.c
libweaverchronosd_la_CFLAGS= 	$(AM_CFLAGS)
libweaverchronosd_la_CXXFLAGS=	$(AM_CXXFLAGS)

bin_PROGRAMS+=				weaver-test-bench
noinst_HEADERS+=			tests/cpp/read_only_vertex_bench.h
weaver_test_bench_SOURCES=	tests/cpp/run.cc \
							common/clock.cc
weaver_test_bench_LDADD=	libweaverclient.la

bin_PROGRAMS+=				weaver-test-hs
weaver_test_hs_SOURCES=	tests/cpp/hyper_stub_perf.cc \
						common/hyper_stub_base.cc \
                        common/event_order.cc \
						common/clock.cc \
						common/vclock.cc \
						common/transaction.cc \
						common/stl_serialization.cc \
						common/weaver_serialization.cc \
						common/enum_serialization.cc \
						common/message.cc \
						common/message_graph_elem.cc \
						common/message_cache_context.cc \
						common/config_constants.cc \
						common/MurmurHash3.cpp \
						common/property_predicate.cc \
                        chronos/chronos.cc \
                        chronos/chronos_c_wrappers.cc \
                        chronos/chronos_cmp_encode.cc \
						node_prog/edge_list.cc \
						node_prog/prop_list.cc \
		                node_prog/reach_program.cc \
		                node_prog/clustering_program.cc \
		                node_prog/pathless_reach_program.cc \
		                node_prog/two_neighborhood_program.cc \
		                node_prog/edge_count_program.cc \
		                node_prog/edge_get_program.cc \
                        node_prog/node_get_program.cc \
		                node_prog/read_n_edges_program.cc \
		                node_prog/read_node_props_program.cc \
		                node_prog/traverse_with_props.cc \
		                node_prog/discover_paths.cc \
						node_prog/parse_btc_tx.cc \
						node_prog/cause_and_effect.cc \
						node_prog/n_gram_path.cc \
		                node_prog/get_btc_block.cc \
		                node_prog/get_btc_tx.cc \
		                node_prog/get_btc_addr.cc \
						db/hyper_stub.cc \
						db/element.cc \
						db/property.cc \
						db/edge.cc \
						db/node.cc

TESTS +=		tests/sh/empty_graph.sh \
				tests/sh/simple_test.sh \
				tests/sh/simple_test_aux_index.sh \
				tests/sh/read_properties.sh \
				tests/sh/line_reachability.sh \
				tests/sh/line_properties.sh \
				tests/sh/concurrent_clients.sh \
				tests/sh/multiple_del.sh \
				tests/sh/transactions.sh
EXTRA_DIST+=	tests/sh/env.sh \
				tests/sh/setup.sh \
				tests/sh/clean.sh \
				tests/sh/empty_graph.sh \
				tests/sh/simple_test.sh \
				tests/sh/simple_test_aux_index.sh \
				tests/sh/read_properties.sh \
				tests/sh/line_reachability.sh \
				tests/sh/line_properties.sh \
				tests/sh/concurrent_clients.sh \
				tests/sh/multiple_del.sh \
				tests/sh/transactions.sh

bin_PROGRAMS+=		weaver
weaver_SOURCES=		weaver.cc
weaver_CPPFLAGS=	-DWEAVER_EXEC_DIR=\""$(weaverexecdir)\""
