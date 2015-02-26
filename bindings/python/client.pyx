# distutils: language = c++

# 
# ===============================================================
#    Description:  Python wrapper for a Weaver client.
# 
#        Created:  11/10/2013 01:40:00 AM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

from __future__ import print_function
import sys

# begin <stolen from Hyperdex/bindings/client.pyx>
cdef extern from 'stdint.h':

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t
    cdef uint64_t UINT64_MAX

# end <stolen from Hyperdex/bindings/client.pyx>

from libcpp.string cimport string
from cython.operator cimport dereference as deref, preincrement as inc

cdef extern from '<utility>' namespace 'std':
    cdef cppclass pair[T1, T2]:
        T1 first
        T2 second

cdef extern from '<memory>' namespace 'std':
    cdef cppclass unique_ptr[T]:
        pass
    cdef cppclass shared_ptr[T]:
        T& operator*()
        pass

cdef extern from '<vector>' namespace 'std':
    cdef cppclass vector[T]:
        cppclass iterator:
            iterator()
            T operator*()
            iterator operator++()
            bint operator==(iterator)
            bint operator!=(iterator)
        vector()
        void push_back(T&)
        T& operator[](int)
        T& at(int)
        iterator begin()
        iterator end()
        size_t size()
        void reserve(size_t)
        void clear()

cdef extern from '<unordered_map>' namespace 'std':
    cdef cppclass unordered_map[T1, T2]:
        cppclass iterator:
            pair[T1, T2] operator*()
            iterator operator++()
            bint operator==(iterator)
            bint operator!=(iterator)
        unordered_map()
        iterator begin()
        iterator end()
        size_t size()
        pair[iterator, bint] emplace(T1, T2)

cdef extern from '<unordered_set>' namespace 'std':
    cdef cppclass unordered_set[T]:
        cppclass iterator:
            T operator*()
            iterator operator++()
            bint operator==(iterator)
            bint operator!=(iterator)
        unordered_set()
        iterator begin()
        iterator end()
        size_t size()

cdef extern from '<deque>' namespace 'std':
    cdef cppclass deque[T]:
        cppclass iterator:
            T operator*()
            iterator operator++()
            bint operator==(iterator)
            bint operator!=(iterator)
        iterator begin()
        iterator end()
        void push_back(T&)
        void clear()

def initialize_member_remotenode(param, member):
    if param is None:
        return RemoteNode()
    else:
        return param

def initialize_member_dict(param, name):
    if isinstance(param, dict):
        return param
    else:
        return {}

def initialize_member_list(param, name):
    if isinstance(param, list):
        return param
    else:
        return []


cdef extern from 'node_prog/node_prog_type.h' namespace 'node_prog':
    cdef enum prog_type:
        DEFAULT
        REACHABILITY
        PATHLESS_REACHABILITY
        N_HOP_REACHABILITY
        TRIANGLE_COUNT
        DIJKSTRA
        CLUSTERING
        TWO_NEIGHBORHOOD
        READ_NODE_PROPS
        READ_EDGES_PROPS

cdef extern from 'common/types.h':
    ctypedef string node_handle_t
    ctypedef string edge_handle_t
    ctypedef string cache_key_t

cdef extern from 'db/remote_node.h' namespace 'db':
    cdef cppclass remote_node:
        remote_node(uint64_t loc, const node_handle_t &i)
        remote_node()
        uint64_t loc
        node_handle_t handle
    cdef remote_node coordinator

cdef extern from 'node_prog/property.h' namespace 'node_prog':
    cdef cppclass property:
        string key
        string value
        property()
        property(const string &k, const string &v)

cdef extern from 'client/datastructures.h' namespace 'cl':
    cdef cppclass edge:
        string handle
        string start_node
        string end_node
        vector[shared_ptr[property]] properties

    cdef cppclass node:
        string handle
        vector[shared_ptr[property]] properties
        unordered_map[string, edge] out_edges
        unordered_set[string] aliases

class Edge:
    def __init__(self, handle='', start_node='', end_node='', properties=None):
        self.handle = handle
        self.start_node = start_node
        self.end_node = end_node
        self.properties = initialize_member_dict(properties, 'properties')

class Node:
    def __init__(self, handle='', properties=None, out_edges=None, aliases=None):
        self.handle = handle
        self.properties = initialize_member_dict(properties, 'properties')
        self.out_edges = initialize_member_dict(out_edges, 'out_edges')
        self.aliases = initialize_member_list(aliases, 'aliases')

class RemoteNode:
    def __init__(self, handle='', loc=0):
        self.handle = handle
        self.loc = loc

cdef extern from 'node_prog/reach_program.h' namespace 'node_prog':
    cdef cppclass reach_params:
        reach_params()
        bint _search_cache
        cache_key_t _cache_key
        bint returning
        remote_node prev_node
        node_handle_t dest
        vector[pair[string, string]] edge_props
        uint32_t hops
        bint reachable
        vector[remote_node] path

class ReachParams:
    def __init__(self, returning=False, prev_node=None, dest='', hops=0, reachable=False, caching=False, edge_props=None, path=None):
        self._search_cache = caching
        self._cache_key = dest
        self.returning = returning
        self.prev_node = initialize_member_remotenode(prev_node)
        self.dest = dest
        self.hops = hops
        self.reachable = reachable
        self.edge_props = initialize_member_list(edge_props, 'edge_props')
        self.path = initialize_member_list(path, 'path')

cdef extern from 'node_prog/pathless_reach_program.h' namespace 'node_prog':
    cdef cppclass pathless_reach_params:
        pathless_reach_params()
        bint returning
        remote_node prev_node
        node_handle_t dest
        vector[pair[string, string]] edge_props
        bint reachable

class PathlessReachParams:
    def __init__(self, returning=False, prev_node=None, dest='', reachable=False, edge_props=None):
        self.returning = returning
        self.prev_node = initialize_member_remotenode(prev_node)
        self.dest = dest
        self.reachable = reachable
        self.edge_props = initialize_member_list(edge_props, 'edge_props')

cdef extern from 'node_prog/clustering_program.h' namespace 'node_prog':
    cdef cppclass clustering_params:
        bint _search_cache
        cache_key_t _cache_key
        bint is_center
        remote_node center
        bint outgoing
        vector[node_handle_t] neighbors
        double clustering_coeff

class ClusteringParams:
    def __init__(self, is_center=True, outgoing=True, caching=False, clustering_coeff=0.0):
        self._search_cache = caching
        self.is_center = is_center
        self.outgoing = outgoing
        self.clustering_coeff = clustering_coeff

cdef extern from 'node_prog/two_neighborhood_program.h' namespace 'node_prog':
    cdef cppclass two_neighborhood_params:
        bint _search_cache
        bint cache_update
        string prop_key
        uint32_t on_hop
        bint outgoing
        remote_node prev_node
        vector[pair[node_handle_t, string]] responses

class TwoNeighborhoodParams:
    def __init__(self, caching=False, cache_update=False, prop_key='', on_hop=0, outgoing=True, prev_node=None, responses=None):
        self._search_cache = caching;
        self.cache_update = cache_update;
        self.prop_key = prop_key
        self.on_hop = on_hop
        self.outgoing = outgoing
        self.prev_node = initialize_member_remotenode(prev_node)
        self.responses = initialize_member_list(responses, 'responses')

cdef extern from 'node_prog/read_node_props_program.h' namespace 'node_prog':
    cdef cppclass read_node_props_params:
        vector[string] keys
        vector[pair[string, string]] node_props

class ReadNodePropsParams:
    def __init__(self, keys=None, node_props=None):
        self.keys = initialize_member_list(keys, 'keys')
        self.node_props = initialize_member_list(node_props, 'node_props')

cdef extern from 'node_prog/read_n_edges_program.h' namespace 'node_prog':
    cdef cppclass read_n_edges_params:
        uint64_t num_edges
        vector[pair[string, string]] edges_props
        vector[edge_handle_t] return_edges

class ReadNEdgesParams:
    def __init__(self, num_edges=UINT64_MAX, edges_props=None, return_edges=None):
        self.num_edges = num_edges
        self.edges_props = initialize_member_list(edges_props, 'edge_props')
        self.return_edges = initialize_member_list(return_edges, 'edge_props')

cdef extern from 'node_prog/edge_count_program.h' namespace 'node_prog':
    cdef cppclass edge_count_params:
        vector[pair[string, string]] edges_props
        uint64_t edge_count

class EdgeCountParams:
    def __init__(self, edges_props=None, edge_count=0):
        initialize_member_list(edges_props, self.edges_props, 'edge_props')
        self.edge_count = edge_count

cdef extern from 'node_prog/edge_get_program.h' namespace 'node_prog':
    cdef cppclass edge_get_params:
        vector[node_handle_t] nbrs
        vector[edge_handle_t] request_edges
        vector[edge] response_edges

class EdgeGetParams:
    def __init__(self, nbrs=None, request_edges=None, response_edges=None):
        self.nbrs = initialize_member_list(nbrs, 'nbrs')
        self.request_edges = initialize_member_list(request_edges, 'request_edges')
        self.response_edges = initialize_member_list(response_edges, 'response_edges')

cdef extern from 'node_prog/node_get_program.h' namespace 'node_prog':
    cdef cppclass node_get_params:
        bint props
        bint edges
        bint aliases
        node node

cdef extern from 'node_prog/traverse_with_props.h' namespace 'node_prog':
    cdef cppclass traverse_props_params:
        traverse_props_params()
        remote_node prev_node
        deque[vector[string]] node_aliases
        deque[vector[pair[string, string]]] node_props
        deque[vector[pair[string, string]]] edge_props
        bint collect_nodes
        bint collect_edges
        unordered_set[node_handle_t] return_nodes
        unordered_set[edge_handle_t] return_edges

class TraversePropsParams:
    def __init__(self, node_aliases=None, node_props=None, edge_props=None, return_nodes=None, return_edges=None, collect_n=False, collect_e=False):
        self.node_aliases = initialize_member_list(node_aliases, 'node_aliases')
        self.node_props = initialize_member_list(node_props, 'node_props')
        self.edge_props = initialize_member_list(edge_props, 'edge_props')
        self.return_nodes = initialize_member_list(return_nodes, 'return_nodes')
        self.return_edges = initialize_member_list(return_edges, 'return_edges')
        self.collect_nodes = collect_n
        self.collect_edges = collect_e

cdef extern from 'client/client.h' namespace 'cl':
    cdef cppclass client:
        client(const char *coordinator, uint16_t port, const char *config_file)

        bint begin_tx()
        bint create_node(string &handle, const vector[string] &aliases)
        bint create_edge(string &handle, const string &node1, const string &node1_alias, const string &node2, const string &node2_alias)
        bint delete_node(const string &node, const string &alias)
        bint delete_edge(const string &edge, const string &node, const string &node_alias)
        bint set_node_property(const string &node, const string &alias, string key, string value)
        bint set_edge_property(const string &node, const string &alias, const string &edge, string key, string value)
        bint add_alias(const string &alias, const string &node)
        bint end_tx() nogil
        bint run_reach_program(vector[pair[string, reach_params]] &initial_args, reach_params&) nogil
        bint run_pathless_reach_program(vector[pair[string, pathless_reach_params]] &initial_args, pathless_reach_params&) nogil
        bint run_clustering_program(vector[pair[string, clustering_params]] &initial_args, clustering_params&) nogil
        bint run_two_neighborhood_program(vector[pair[string, two_neighborhood_params]] &initial_args, two_neighborhood_params&) nogil
        bint read_node_props_program(vector[pair[string, read_node_props_params]] &initial_args, read_node_props_params&) nogil
        bint read_n_edges_program(vector[pair[string, read_n_edges_params]] &initial_args, read_n_edges_params&) nogil
        bint edge_count_program(vector[pair[string, edge_count_params]] &initial_args, edge_count_params&) nogil
        bint edge_get_program(vector[pair[string, edge_get_params]] &initial_args, edge_get_params&) nogil
        bint node_get_program(vector[pair[string, node_get_params]] &initial_args, node_get_params&) nogil
        bint traverse_props_program(vector[pair[string, traverse_props_params]] &initial_args, traverse_props_params&) nogil
        void start_migration()
        void single_stream_migration()
        void exit_weaver()
        vector[uint64_t] get_node_count()
        bint aux_index()

class WeaverError(Exception):
    pass

cdef class Client:
    cdef client *thisptr
    cdef string traverse_start_node
    cdef object traverse_node_aliases
    cdef object traverse_node_props
    cdef object traverse_edge_props
    def __cinit__(self, coordinator, port, config_file=''):
        self.thisptr = new client(coordinator, port, config_file)
        self.traverse_start_node = ''
        self.traverse_node_aliases = []
        self.traverse_node_props = []
        self.traverse_edge_props = []

    def __dealloc__(self):
        del self.thisptr

    def begin_tx(self):
        if not self.thisptr.begin_tx():
            raise WeaverError('no more than one concurrent transaction per client')

    def create_node(self, handle='', **kwargs):
        cdef string cc_handle
        aliases = []
        if handle != '':
            cc_handle = handle
        if 'aliases' in kwargs:
            aliases = kwargs['aliases']
        if self.thisptr.create_node(cc_handle, aliases):
            return str(cc_handle)
        else:
            raise WeaverError('no active transaction')

    def create_edge(self, node1=None, node2=None, handle=None, **kwargs):
        handle1 = ''
        handle2 = ''
        alias1 = ''
        alias2 = ''
        cdef string cc_handle
        if node1 is None:
            if 'node1_alias' not in kwargs:
                raise WeaverError('provide either node handle or node alias')
            else:
                alias1 = kwargs['node1_alias']
        else:
            handle1 = node1
        if node2 is None:
            if 'node2_alias' not in kwargs:
                raise WeaverError('provide either node handle or node alias')
            else:
                alias2 = kwargs['node2_alias']
        else:
            handle2 = node2
        if handle is not None:
            cc_handle = handle
        if self.thisptr.create_edge(cc_handle, handle1, alias1, handle2, alias2):
            return str(cc_handle)
        else:
            raise WeaverError('no active transaction')

    def delete_node(self, handle='', **kwargs):
        alias = ''
        if handle is None:
            if 'alias' not in kwargs:
                raise WeaverError('provide either node handle or node alias')
            else:
                alias = kwargs['alias']
        if not self.thisptr.delete_node(handle, alias):
            raise WeaverError('no active transaction')

    def delete_edge(self, edge, node='', **kwargs):
        alias = ''
        if node == '' and 'node_alias' in kwargs:
            alias = kwargs['node_alias']
        if not self.thisptr.delete_edge(edge, node, alias):
            raise WeaverError('no active transaction')

    def set_node_property(self, key, value, node='', **kwargs):
        alias = ''
        if node == '':
            if 'node_alias' not in kwargs:
                raise WeaverError('provide either node handle or node alias')
            else:
                alias = kwargs['node_alias']
        if not self.thisptr.set_node_property(node, alias, str(key), str(value)):
            raise WeaverError('no active transaction')

    def set_node_properties(self, properties, node='', **kwargs):
        alias = ''
        if node == '':
            if 'node_alias' not in kwargs:
                raise WeaverError('provide either node handle or node alias')
            else:
                alias = kwargs['node_alias']
        if not isinstance(properties, dict):
            raise WeaverError('properties should be a dictionary')
        else:
            for k in properties:
                if isinstance(properties[k], list):
                    for v in properties[k]:
                        self.set_node_property(str(k), str(v), node, node_alias=alias)
                else:
                    self.set_node_property(str(k), str(properties[k]), node, node_alias=alias)

    def set_edge_property(self, edge, key, value, node='', **kwargs):
        alias = ''
        if node == '' and 'node_alias' in kwargs:
            alias = kwargs['node_alias']
        if not self.thisptr.set_edge_property(node, alias, edge, str(key), str(value)):
            raise WeaverError('no active transaction')

    def set_edge_properties(self, edge, properties, node='', **kwargs):
        alias = ''
        if 'node_alias' in kwargs:
            alias = kwargs['node_alias']
        if not isinstance(properties, dict):
            raise WeaverError('properties should be a dictionary')
        else:
            for k in properties:
                if isinstance(properties[k], list):
                    for v in properties[k]:
                        self.set_edge_property(edge, str(k), str(v), node, node_alias=alias)
                else:
                    self.set_edge_property(edge, str(k), str(properties[k]), node, node_alias=alias)

    def add_alias(self, alias, node):
        if not self.thisptr.add_alias(alias, node):
            raise WeaverError('add_alias transaction error')

    def end_tx(self):
        cdef bint ret
        with nogil:
            ret = self.thisptr.end_tx()
        if not ret:
            raise WeaverError('transaction commit error')

    def run_reach_program(self, init_args):
        cdef vector[pair[string, reach_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, reach_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second._search_cache = rp[1]._search_cache
            arg_pair.second._cache_key = rp[1].dest
            arg_pair.second.returning = rp[1].returning
            arg_pair.second.dest = rp[1].dest
            arg_pair.second.reachable = rp[1].reachable
            arg_pair.second.prev_node = coordinator
            arg_pair.second.edge_props.clear()
            arg_pair.second.edge_props.reserve(len(rp[1].edge_props))
            for p in rp[1].edge_props:
                arg_pair.second.edge_props.push_back(p)
            c_args.push_back(arg_pair)

        cdef reach_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.run_reach_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        foundpath = []
        for rn in c_rp.path:
            foundpath.append(rn.handle)
        response = ReachParams(path=foundpath, hops=c_rp.hops, reachable=c_rp.reachable)
        return response

    # warning! set prev_node loc to vt_id if somewhere in params
    def run_pathless_reach_program(self, init_args):
        cdef vector[pair[string, pathless_reach_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, pathless_reach_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.returning = rp[1].returning
            arg_pair.second.dest = rp[1].dest
            arg_pair.second.reachable = rp[1].reachable
            arg_pair.second.prev_node = coordinator
            arg_pair.second.edge_props.clear()
            arg_pair.second.edge_props.reserve(len(rp[1].edge_props))
            for p in rp[1].edge_props:
                arg_pair.second.edge_props.push_back(p)
            c_args.push_back(arg_pair)

        cdef pathless_reach_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.run_pathless_reach_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = PathlessReachParams(reachable=c_rp.reachable)
        return response

    def run_clustering_program(self, init_args):
        cdef vector[pair[string, clustering_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, clustering_params] arg_pair
        for cp in init_args:
            arg_pair.first = cp[0]
            arg_pair.second._search_cache = cp[1]._search_cache 
            arg_pair.second._cache_key = cp[0] # cache key is center node handle
            arg_pair.second.is_center = cp[1].is_center
            arg_pair.second.outgoing = cp[1].outgoing
            c_args.push_back(arg_pair)

        cdef clustering_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.run_clustering_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = ClusteringParams(clustering_coeff=c_rp.clustering_coeff)
        return response

    def run_two_neighborhood_program(self, init_args):
        cdef vector[pair[string, two_neighborhood_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, two_neighborhood_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second._search_cache = rp[1]._search_cache
            arg_pair.second.cache_update = rp[1].cache_update
            arg_pair.second.prop_key = rp[1].prop_key
            arg_pair.second.on_hop = rp[1].on_hop
            arg_pair.second.outgoing = rp[1].outgoing
            arg_pair.second.prev_node = coordinator
            c_args.push_back(arg_pair)

        cdef two_neighborhood_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.run_two_neighborhood_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = TwoNeighborhoodParams(responses = c_rp.responses)
        return response

    def read_node_props(self, init_args):
        cdef vector[pair[string, read_node_props_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, read_node_props_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.keys = rp[1].keys
            c_args.push_back(arg_pair)

        cdef read_node_props_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.read_node_props_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = ReadNodePropsParams(node_props=c_rp.node_props)
        return response

    def read_n_edges(self, init_args):
        cdef vector[pair[string, read_n_edges_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, read_n_edges_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.num_edges = rp[1].num_edges
            arg_pair.second.edges_props.clear()
            arg_pair.second.edges_props.reserve(len(rp[1].edges_props))
            for p in rp[1].edges_props:
                arg_pair.second.edges_props.push_back(p)
            c_args.push_back(arg_pair)

        cdef read_n_edges_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.read_n_edges_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = ReadNEdgesParams(return_edges=c_rp.return_edges)
        return response

    def edge_count(self, init_args):
        cdef vector[pair[string, edge_count_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, edge_count_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.edges_props.clear()
            arg_pair.second.edges_props.reserve(len(rp[1].edges_props))
            for p in rp[1].edges_props:
                arg_pair.second.edges_props.push_back(p)
            c_args.push_back(arg_pair)

        cdef edge_count_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.edge_count_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = EdgeCountParams(edge_count=c_rp.edge_count)
        return response

    cdef __convert_vector_props_to_dict(self, vector[shared_ptr[property]] pvec, pdict):
        cdef vector[shared_ptr[property]].iterator prop_iter = pvec.begin()
        while prop_iter != pvec.end():
            key = str(deref(deref(prop_iter)).key)
            value = str(deref(deref(prop_iter)).value)
            if key in pdict:
                pdict[key].append(value)
            else:
                pdict[key] = [value]
            inc(prop_iter)

    cdef __convert_edge_to_client_edge(self, edge c_edge, py_edge):
        py_edge.handle = str(c_edge.handle)
        py_edge.start_node = str(c_edge.start_node)
        py_edge.end_node = str(c_edge.end_node)
        self.__convert_vector_props_to_dict(c_edge.properties, py_edge.properties)

    cdef get_edges(self, nbrs=None, edges=None, node=''):
        if node == '':
            if edges is None:
                raise WeaverError('provide one of node handle, node alias, or edge handle')
            elif not isinstance(edges, list):
                raise WeaverError('edges should be list')
            else:
                node = edges[0]
        cdef pair[string, edge_get_params] arg_pair
        arg_pair.first = node
        if nbrs is not None:
            arg_pair.second.nbrs.reserve(len(nbrs))
            for nbr in nbrs:
                arg_pair.second.nbrs.push_back(nbr)
        if edges is not None:
            arg_pair.second.request_edges.reserve(len(edges))
            for e in edges:
                arg_pair.second.request_edges.push_back(e)
        cdef vector[pair[string, edge_get_params]] c_args
        c_args.push_back(arg_pair)

        cdef edge_get_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.edge_get_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = []
        cdef vector[edge].iterator resp_iter = c_rp.response_edges.begin()
        while resp_iter != c_rp.response_edges.end():
            response.append(Edge())
            self.__convert_edge_to_client_edge(deref(resp_iter), response[-1])
            inc(resp_iter)
        return response

    def get_edge(self, edge, node=''):
        if node == '':
            node = edge
        response = self.get_edges(nbrs=None, edges=[edge], node=node)
        assert(len(response) < 2)
        if len(response) == 1:
            return response[0]
        else:
            raise WeaverError('edge not found or some other error')

    cdef __convert_node_to_client_node(self, node c_node, py_node):
        py_node.handle = str(c_node.handle)
        self.__convert_vector_props_to_dict(c_node.properties, py_node.properties)
        cdef unordered_map[string, edge].iterator edge_iter = c_node.out_edges.begin()
        while edge_iter != c_node.out_edges.end():
            new_edge = Edge()
            self.__convert_edge_to_client_edge(deref(edge_iter).second, new_edge)
            py_node.out_edges[str(deref(edge_iter).first)] = new_edge
            inc(edge_iter)
        cdef unordered_set[string].iterator alias_iter = c_node.aliases.begin()
        while alias_iter != c_node.aliases.end():
            py_node.aliases.append(str(deref(alias_iter)))
            inc(alias_iter)

    def get_node(self, node, get_props=True, get_edges=True, get_aliases=True):
        cdef pair[string, node_get_params] arg_pair
        arg_pair.second.props = get_props
        arg_pair.second.edges = get_edges
        arg_pair.second.aliases = get_aliases
        arg_pair.first = node
        cdef vector[pair[string, node_get_params]] c_args
        c_args.push_back(arg_pair)

        cdef node_get_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.node_get_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        new_node = Node()
        self.__convert_node_to_client_node(c_rp.node, new_node)
        return new_node

    def get_node_properties(self, node):
        return self.get_node(node, get_edges=False, get_aliases=False).properties

    def get_node_edges(self, node):
        return self.get_node(node, get_props=False, get_aliases=False).out_edges

    def get_node_aliases(self, node):
        return self.get_node(node, get_props=False, get_edges=False).aliases

    def traverse_props(self, init_args):
        cdef vector[pair[string, traverse_props_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, traverse_props_params] arg_pair
        cdef vector[pair[string, string]] props
        cdef vector[string] aliases
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.prev_node = coordinator
            arg_pair.second.collect_nodes = rp[1].collect_nodes
            arg_pair.second.collect_edges = rp[1].collect_edges
            arg_pair.second.node_aliases.clear()
            arg_pair.second.node_props.clear()
            arg_pair.second.edge_props.clear()
            for p_vec in rp[1].node_aliases:
                aliases.clear()
                aliases.reserve(len(p_vec))
                for p in p_vec:
                    aliases.push_back(p)
                arg_pair.second.node_aliases.push_back(aliases)
            for p_vec in rp[1].node_props:
                props.clear()
                props.reserve(len(p_vec))
                for p in p_vec:
                    props.push_back(p)
                arg_pair.second.node_props.push_back(props)
            for p_vec in rp[1].edge_props:
                props.clear()
                props.reserve(len(p_vec))
                for p in p_vec:
                    props.push_back(p)
                arg_pair.second.edge_props.push_back(props)
            c_args.push_back(arg_pair)

        cdef traverse_props_params c_rp
        cdef bint success
        with nogil:
            success = self.thisptr.traverse_props_program(c_args, c_rp)

        if not success:
            raise WeaverError('node prog error')

        response = TraversePropsParams()
        for n in c_rp.return_nodes:
            response.return_nodes.append(n)
        for e in c_rp.return_edges:
            response.return_edges.append(e)
        return response

    def traverse(self, start_node, node_props=None, node_aliases=None):
        self.traverse_start_node = start_node
        self.traverse_node_aliases = []
        self.traverse_node_props = []
        self.traverse_edge_props = []
        if node_aliases is None:
            self.traverse_node_aliases.append([])
        else:
            self.traverse_node_aliases.append(node_aliases)
        if node_props is None:
            self.traverse_node_props.append({})
        else:
            self.traverse_node_props.append(node_props)
        return self

    def out_edge(self, edge_props=None):
        if edge_props is None:
            self.traverse_edge_props.append({})
        else:
            self.traverse_edge_props.append(edge_props)
        return self

    def node(self, node_props=None, node_aliases=None):
        if node_aliases is None:
            self.traverse_node_aliases.append([])
        else:
            self.traverse_node_aliases.append(node_aliases)
        if node_props is None:
            self.traverse_node_props.append({})
        else:
            self.traverse_node_props.append(node_props)
        return self

    def __convert_props_dict_to_list(self, dprops):
        lprops = []
        for d in dprops:
            cur_list = []
            if d:
                for k in d:
                    if isinstance(d[k], list):
                        for v in d[k]:
                            cur_list.append((k,v))
                    else:
                        cur_list.append((k, d[k]))
            lprops.append(cur_list)
        return lprops

    def execute(self, collect_nodes=False, collect_edges=False):
        num_node_aliases = len(self.traverse_node_aliases)
        num_node_props = len(self.traverse_node_props)
        num_edge_props = len(self.traverse_edge_props)
        assert (num_node_aliases == num_node_props)
        assert ((num_node_props == (num_edge_props+1)) or (num_node_props == num_edge_props))

        params = TraversePropsParams(self.traverse_node_aliases, \
                                     self.__convert_props_dict_to_list(self.traverse_node_props), \
                                     self.__convert_props_dict_to_list(self.traverse_edge_props), \
                                     collect_n=collect_nodes, \
                                     collect_e=collect_edges)
        response = self.traverse_props([(self.traverse_start_node, params)])
        return response.return_nodes + response.return_edges

    def collect(self):
        return self.execute(collect_nodes=True, collect_edges=True)

    def collect_nodes(self):
        return self.execute(collect_nodes=True)

    def collect_edges(self):
        return self.execute(collect_edges=True)

    def start_migration(self):
        self.thisptr.start_migration()
    def single_stream_migration(self):
        self.thisptr.single_stream_migration()
    def exit_weaver(self):
        self.thisptr.exit_weaver()
    def get_node_count(self):
        cdef vector[uint64_t] node_count = self.thisptr.get_node_count()
        count = []
        cdef vector[uint64_t].iterator iter = node_count.begin()
        while iter != node_count.end():
            count.append(deref(iter))
            inc(iter)
        return count
    def aux_index(self):
        return self.thisptr.aux_index()
