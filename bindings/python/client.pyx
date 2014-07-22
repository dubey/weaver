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

cdef extern from '<vector>' namespace 'std':
    cdef cppclass vector[T]:
        cppclass iterator:
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
    ctypedef uint64_t node_id_t
    ctypedef string node_handle_t
    ctypedef string edge_handle_t
    ctypedef uint64_t cache_key_t

cdef extern from 'db/remote_node.h' namespace 'db::element':
    cdef cppclass remote_node:
        remote_node(uint64_t loc, node_id_t i)
        remote_node()
        uint64_t loc
        node_id_t id
    cdef remote_node coordinator

class RemoteNode:
    def __init__(self, id=0, loc=0):
        self.id = id
        self.loc = loc

cdef extern from 'node_prog/reach_program.h' namespace 'node_prog':
    cdef cppclass reach_params:
        reach_params()
        bint _search_cache
        cache_key_t _cache_key
        bint returning
        remote_node prev_node
        node_id_t dest
        vector[pair[string, string]] edge_props
        uint32_t hops
        bint reachable
        vector[remote_node] path

class ReachParams:
    def __init__(self, returning=False, prev_node=None, dest=0, hops=0, reachable=False, caching=False, edge_props=None, path=None):
        self._search_cache = caching
        self._cache_key = dest
        self.returning = returning
        if prev_node is None:
            self.prev_node = RemoteNode(0,0)
        else:
            self.prev_node = prev_node
        self.dest= dest
        self.hops = hops
        self.reachable = reachable
        if edge_props is None:
            self.edge_props = []
        else:
            self.edge_props = edge_props
        if path is None:
            self.path = []
        else:
            self.path = path

cdef extern from 'node_prog/pathless_reach_program.h' namespace 'node_prog':
    cdef cppclass pathless_reach_params:
        pathless_reach_params()
        bint returning
        remote_node prev_node
        node_id_t dest
        vector[pair[string, string]] edge_props
        bint reachable

class PathlessReachParams:
    def __init__(self, returning=False, prev_node=None, dest=0, reachable=False, edge_props=None):
        self.returning = returning
        if prev_node is None:
            self.prev_node = RemoteNode(0,0)
        else:
            self.prev_node = prev_node
        self.dest = dest
        self.reachable = reachable
        if edge_props is None:
            self.edge_props = []
        else:
            self.edge_props = edge_props

cdef extern from 'node_prog/clustering_program.h' namespace 'node_prog':
    cdef cppclass clustering_params:
        bint _search_cache
        cache_key_t _cache_key
        bint is_center
        remote_node center
        bint outgoing
        vector[node_id_t] neighbors
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
        vector[pair[node_id_t, string]] responses

class TwoNeighborhoodParams:
    def __init__(self, caching=False, cache_update=False, prop_key="", on_hop=0, outgoing=True, prev_node=None, responses=None):
        self._search_cache = caching;
        self.cache_update = cache_update;
        self.prop_key = prop_key
        self.on_hop = on_hop
        self.outgoing = outgoing
        if prev_node is None:
            self.prev_node = RemoteNode(0,0)
        else:
            self.prev_node = prev_node
        if responses is None:
            self.responses = []
        else:
            self.responses = responses
'''
cdef extern from 'node_prog/dijkstra_program.h' namespace 'node_prog':
    cdef cppclass dijkstra_params:
        uint64_t src_id
        remote_node src_handle
        uint64_t dst_handle
        string edge_weight_name
        bint is_widest_path
        bint adding_nodes
        uint64_t prev_node
        vector[pair[uint64_t, remote_node]] entries_to_add
        remote_node next_node
        vector[pair[uint64_t, uint64_t]] final_path
        uint64_t cost

class DijkstraParams:
    def __init__(self, src_id=0, src_handle=RemoteNode(), dst_handle=0, edge_weight_name="weight", is_widest_path=False,
            adding_nodes=False, prev_node=RemoteNode(), entries_to_add=[], next_node=0, final_path=[], cost=0):
        self.src_id = src_id
        self.src_handle = src_handle
        self.dst_handle = dst_handle
        self.edge_weight_name = edge_weight_name
        self.is_widest_path = is_widest_path
        self.adding_nodes = adding_nodes
        self.prev_node = prev_node
        self.entries_to_add = entries_to_add
        self.next_node = next_node
        self.final_path = final_path
        self.cost = cost
'''

cdef extern from 'node_prog/read_node_props_program.h' namespace 'node_prog':
    cdef cppclass read_node_props_params:
        vector[string] keys
        vector[pair[string, string]] node_props

class ReadNodePropsParams:
    def __init__(self, keys=None, node_props=None):
        if keys is None:
            self.keys = []
        else:
            self.keys = keys
        if node_props is None:
            self.node_props = []
        else:
            self.node_props = node_props

cdef extern from 'node_prog/read_edges_props_program.h' namespace 'node_prog':
    cdef cppclass read_edges_props_params:
        vector[edge_handle_t] edges
        vector[string] keys
        vector[pair[edge_handle_t, vector[pair[string, string]]]] edges_props

class ReadEdgesPropsParams:
    def __init__(self, edges=None, keys=None, edges_props=None):
        if edges is None:
            self.edges = []
        else:
            self.edges = edges
        if edges_props is None:
            self.edges_props = []
        else:
            self.edges_props = edges_props
        if edges_props is None:
            self.edges_props = []
        else:
            self.edges_props = edges_props

cdef extern from 'node_prog/read_n_edges_program.h' namespace 'node_prog':
    cdef cppclass read_n_edges_params:
        uint64_t num_edges
        vector[pair[string, string]] edges_props
        vector[edge_handle_t] return_edges

class ReadNEdgesParams:
    def __init__(self, num_edges=UINT64_MAX, edges_props=None, return_edges=None):
        self.num_edges = num_edges
        if edges_props is None:
            self.edges_props = []
        else:
            self.edges_props = edges_props
        if return_edges is None:
            self.return_edges = []
        else:
            self.return_edges = return_edges

cdef extern from 'node_prog/edge_count_program.h' namespace 'node_prog':
    cdef cppclass edge_count_params:
        vector[pair[string, string]] edges_props
        uint64_t edge_count

class EdgeCountParams:
    def __init__(self, edges_props=None, edge_count=0):
        if edges_props is None:
            self.edges_props = []
        else:
            self.edges_props = edges_props
        self.edge_count = edge_count

cdef extern from 'node_prog/edge_get_program.h' namespace 'node_prog':
    cdef cppclass edge_get_params:
        node_id_t nbr_id
        vector[pair[string, string]] edges_props
        vector[edge_handle_t] return_edges

class EdgeGetParams:
    def __init__(self, nbr_id=UINT64_MAX, edges_props=None, return_edges=None):
        self.nbr_id = nbr_id
        if edges_props is None:
            self.edges_props = []
        else:
            self.edges_props = edges_props
        if return_edges is None:
            self.return_edges = []
        else:
            self.return_edges = return_edges

cdef extern from 'node_prog/traverse_with_props.h' namespace 'node_prog':
    cdef cppclass traverse_props_params:
        traverse_props_params()
        remote_node prev_node
        deque[vector[pair[string, string]]] node_props
        deque[vector[pair[string, string]]] edge_props
        bint collect_nodes
        bint collect_edges
        unordered_set[node_handle_t] return_nodes
        unordered_set[edge_handle_t] return_edges

class TraversePropsParams:
    def __init__(self, node_props=None, edge_props=None, return_nodes=None, return_edges=None, collect_n=False, collect_e=False):
        if node_props is None:
            self.node_props = []
        else:
            self.node_props = node_props
        if edge_props is None:
            self.edge_props = []
        else:
            self.edge_props = edge_props
        if return_nodes is None:
            self.return_nodes = []
        else:
            self.return_nodes = return_nodes
        if return_edges is None:
            self.return_edges = []
        else:
            self.return_edges = return_edges
        self.collect_nodes = collect_n
        self.collect_edges = collect_e

cdef extern from 'client/weaver_client.h' namespace 'cl':
    cdef cppclass client:
        client(const char *coordinator, uint16_t port, const char *config_file)

        void begin_tx()
        string create_node(string &handle)
        string create_edge(string &handle, string &node1, string &node2)
        void delete_node(string &node)
        void delete_edge(string &edge, string &node)
        void set_node_property(string &node, string key, string value)
        void set_edge_property(string &node, string &edge, string key, string value)
        bint end_tx() nogil
        reach_params run_reach_program(vector[pair[string, reach_params]] &initial_args) nogil
        pathless_reach_params run_pathless_reach_program(vector[pair[string, pathless_reach_params]] &initial_args) nogil
        clustering_params run_clustering_program(vector[pair[string, clustering_params]] &initial_args) nogil
        two_neighborhood_params run_two_neighborhood_program(vector[pair[string, two_neighborhood_params]] &initial_args) nogil
        #dijkstra_params run_dijkstra_program(vector[pair[string, dijkstra_params]] &initial_args) nogil
        read_node_props_params read_node_props_program(vector[pair[string, read_node_props_params]] &initial_args) nogil
        read_edges_props_params read_edges_props_program(vector[pair[string, read_edges_props_params]] &initial_args) nogil
        read_n_edges_params read_n_edges_program(vector[pair[string, read_n_edges_params]] &initial_args) nogil
        edge_count_params edge_count_program(vector[pair[string, edge_count_params]] &initial_args) nogil
        edge_get_params edge_get_program(vector[pair[string, edge_get_params]] &initial_args) nogil
        traverse_props_params traverse_props_program(vector[pair[string, traverse_props_params]] &initial_args) nogil
        void start_migration()
        void single_stream_migration()
        void commit_graph()
        void exit_weaver()
        vector[uint64_t] get_node_count()

cdef class Client:
    cdef client *thisptr
    cdef string traverse_start_node
    cdef object traverse_node_props
    cdef object traverse_edge_props
    def __cinit__(self, coordinator, port, config_file='/usr/local/etc/weaver.yaml'):
        self.thisptr = new client(coordinator, port, config_file)
        self.traverse_start_node = ''
        self.traverse_node_props = []
        self.traverse_edge_props = []
    def __dealloc__(self):
        del self.thisptr
    def begin_tx(self):
        self.thisptr.begin_tx()
    def create_node(self, handle=''):
        return self.thisptr.create_node(handle)
    def create_edge(self, node1, node2, handle=''):
        return self.thisptr.create_edge(handle, node1, node2)
    def delete_node(self, node):
        self.thisptr.delete_node(node)
    def delete_edge(self, edge, node):
        self.thisptr.delete_edge(edge, node)
    def set_node_property(self, node, key, value):
        self.thisptr.set_node_property(node, key, value)
    def set_edge_property(self, node, edge, key, value):
        self.thisptr.set_edge_property(node, edge, key, value)
    def end_tx(self):
        cdef bint ret
        with nogil:
            ret = self.thisptr.end_tx()
        return ret
    def run_reach_program(self, init_args):
        cdef vector[pair[string, reach_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, reach_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second._search_cache = rp[1]._search_cache
            arg_pair.second._cache_key = rp[1].dest
            arg_pair.second.returning = rp[1].returning
            arg_pair.second.dest= rp[1].dest
            arg_pair.second.reachable = rp[1].reachable
            arg_pair.second.prev_node = coordinator
            arg_pair.second.edge_props.clear()
            arg_pair.second.edge_props.reserve(len(rp[1].edge_props))
            for p in rp[1].edge_props:
                arg_pair.second.edge_props.push_back(p)
            c_args.push_back(arg_pair)
        with nogil:
            c_rp = self.thisptr.run_reach_program(c_args)
        foundpath = []
        for rn in c_rp.path:
            foundpath.append(rn.id)
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
            arg_pair.second.dest= rp[1].dest
            arg_pair.second.reachable = rp[1].reachable
            arg_pair.second.prev_node = coordinator
            arg_pair.second.edge_props.clear()
            arg_pair.second.edge_props.reserve(len(rp[1].edge_props))
            for p in rp[1].edge_props:
                arg_pair.second.edge_props.push_back(p)
            c_args.push_back(arg_pair)
        with nogil:
            c_rp = self.thisptr.run_pathless_reach_program(c_args)
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
        with nogil:
            c_cp = self.thisptr.run_clustering_program(c_args)
        response = ClusteringParams(clustering_coeff=c_cp.clustering_coeff)
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
        with nogil:
            c_rp = self.thisptr.run_two_neighborhood_program(c_args)
        response = TwoNeighborhoodParams(responses = c_rp.responses)
        return response
    '''
    def run_dijkstra_program(self, init_args):
        cdef vector[pair[string, dijkstra_params]] c_args
        cdef pair[string, dijkstra_params] arg_pair
        for cp in init_args:
            arg_pair.first = cp[0]
            arg_pair.second.is_widest_path = cp[1].is_widest_path;
            arg_pair.second.src_id = cp[1].src_id;
            arg_pair.second.dst_handle = cp[1].dst_handle;
            arg_pair.second.edge_weight_name = cp[1].edge_weight_name;
            c_args.push_back(arg_pair)
        with nogil:
            c_dp = self.thisptr.run_dijkstra_program(c_args)
        response = DijkstraParams(final_path=c_dp.final_path, cost=c_dp.cost)
        return response
    '''
    def read_node_props(self, init_args):
        cdef vector[pair[string, read_node_props_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, read_node_props_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.keys = rp[1].keys
            c_args.push_back(arg_pair)
        with nogil:
            c_rp = self.thisptr.read_node_props_program(c_args)
        response = ReadNodePropsParams(node_props=c_rp.node_props)
        return response

    def read_edges_props(self, init_args):
        cdef vector[pair[string, read_edges_props_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, read_edges_props_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            #arg_pair.second.edges = rp[1].edges
            arg_pair.second.keys = rp[1].keys
            c_args.push_back(arg_pair)
        with nogil:
            c_rp = self.thisptr.read_edges_props_program(c_args)
        response = ReadEdgesPropsParams(edges_props=c_rp.edges_props)
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
        with nogil:
            c_rp = self.thisptr.read_n_edges_program(c_args)
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
        with nogil:
            c_rp = self.thisptr.edge_count_program(c_args)
        response = EdgeCountParams(edge_count=c_rp.edge_count)
        return response

    def edge_get(self, init_args):
        cdef vector[pair[string, edge_get_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, edge_get_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.nbr_id = rp[1].nbr_id
            arg_pair.second.edges_props.clear()
            arg_pair.second.edges_props.reserve(len(rp[1].edges_props))
            for p in rp[1].edges_props:
                arg_pair.second.edges_props.push_back(p)
            c_args.push_back(arg_pair)
        with nogil:
            c_rp = self.thisptr.edge_get_program(c_args)
        response = EdgeGetParams(return_edges=c_rp.return_edges)
        return response

    def traverse_props(self, init_args):
        cdef vector[pair[string, traverse_props_params]] c_args
        c_args.reserve(len(init_args))
        cdef pair[string, traverse_props_params] arg_pair
        cdef vector[pair[string, string]] props
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.prev_node = coordinator
            arg_pair.second.collect_nodes = rp[1].collect_nodes
            arg_pair.second.collect_edges = rp[1].collect_edges
            arg_pair.second.node_props.clear()
            arg_pair.second.edge_props.clear()
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
        with nogil:
            c_rp = self.thisptr.traverse_props_program(c_args)
        response = TraversePropsParams()
        for n in c_rp.return_nodes:
            response.return_nodes.append(n)
        for e in c_rp.return_edges:
            response.return_edges.append(e)
        return response

    def traverse(self, start_node, node_props=None):
        self.traverse_start_node = start_node
        self.traverse_node_props = []
        self.traverse_edge_props = []
        if node_props is None:
            self.traverse_node_props.append([])
        else:
            self.traverse_node_props.append(node_props)
        return self

    def out_edge(self, edge_props=None):
        if edge_props is None:
            self.traverse_edge_props.append([])
        else:
            self.traverse_edge_props.append(edge_props)
        return self

    def node(self, node_props=None):
        if node_props is None:
            self.traverse_node_props.append([])
        else:
            self.traverse_node_props.append(node_props)
        return self

    def execute(self, collect_nodes=False, collect_edges=False):
        num_node_props = len(self.traverse_node_props)
        num_edge_props = len(self.traverse_edge_props)
        assert ((num_node_props == (num_edge_props+1)) or (num_node_props == num_edge_props))
        params = TraversePropsParams(node_props=self.traverse_node_props, edge_props=self.traverse_edge_props, collect_n=collect_nodes, collect_e=collect_edges)
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
    def commit_graph(self):
        self.thisptr.commit_graph()
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
