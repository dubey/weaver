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
from cpython cimport bool

cdef extern from 'stdint.h':

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t
    cdef uint64_t UINT64_MAX

MAX_UINT64 = UINT64_MAX

# end <stolen from Hyperdex/bindings/client.pyx>

cdef extern from '<utility>' namespace 'std':
    cdef cppclass pair[T1, T2]:
        T1 first
        T2 second

cdef extern from '<memory>' namespace 'std':
    cdef cppclass unique_ptr[T]:
        pass

cdef extern from '<vector>' namespace 'std':
    cdef cppclass vector[T]:
        void push_back(T& t)

cdef extern from 'common/weaver_constants.h':
    # messaging constants
    cdef uint64_t ID_INCR
    cdef uint64_t COORD_ID
    cdef uint64_t COORD_SM_ID
    cdef uint64_t CLIENT_ID
    cdef char *SHARDS_DESC_FILE
    # weaver setup
    cdef uint64_t NUM_SHARDS
    cdef uint64_t NUM_VTS
    cdef uint64_t SHARD_ID_INCR
    cdef uint64_t NUM_THREADS
    cdef uint64_t ID_BITS
    cdef uint64_t TOP_MASK
    cdef char *GRAPH_FILE
    # node programs
    cdef uint64_t BATCH_MSG_SIZE
    # migration
    cdef uint64_t START_MIGR_ID
    # coordinator
    cdef uint64_t VT_BB_TIMEOUT
    cdef uint64_t VT_NOP_TIMEOUT
    cdef uint64_t VT_INITIAL_CLKUPDATE_DELAY
    # hyperdex
    cdef char *HYPERDEX_COORD_IPADDR
    cdef uint64_t HYPERDEX_COORD_PORT
    # kronos
    cdef char *KRONOS_IPADDR
    cdef uint64_t KRONOS_PORT

_ID_INCR                    = ID_INCR
_COORD_ID                   = COORD_ID
_COORD_SM_ID                = COORD_SM_ID
_CLIENT_ID                  = CLIENT_ID
_SHARDS_DESC_FILE           = SHARDS_DESC_FILE
_NUM_SHARDS                 = NUM_SHARDS
_NUM_VTS                    = NUM_VTS
_SHARD_ID_INCR              = SHARD_ID_INCR
_NUM_THREADS                = NUM_THREADS
_ID_BITS                    = ID_BITS
_TOP_MASK                   = TOP_MASK
_GRAPH_FILE                 = GRAPH_FILE
_BATCH_MSG_SIZE             = BATCH_MSG_SIZE
_START_MIGR_ID              = START_MIGR_ID
_VT_BB_TIMEOUT              = VT_BB_TIMEOUT
_VT_NOP_TIMEOUT             = VT_NOP_TIMEOUT
_VT_INITIAL_CLKUPDATE_DELAY = VT_INITIAL_CLKUPDATE_DELAY
_HYPERDEX_COORD_IPADDR      = HYPERDEX_COORD_IPADDR
_HYPERDEX_COORD_PORT        = HYPERDEX_COORD_PORT
_KRONOS_IPADDR              = KRONOS_IPADDR
_KRONOS_PORT                = KRONOS_PORT

cdef extern from 'node_prog/node_prog_type.h' namespace 'node_prog':
    cdef enum prog_type:
        DEFAULT
        REACHABILITY
        N_HOP_REACHABILITY
        TRIANGLE_COUNT
        DIJKSTRA
        CLUSTERING

cdef extern from 'db/element/remote_node.h' namespace 'db::element':
    cdef cppclass remote_node:
        remote_node(uint64_t loc, uint64_t handle)
        uint64_t loc
        uint64_t handle

class RemoteNode:
    def __init__(self, loc=0, handle=0):
        self.loc = loc
        self.handle = handle

cdef extern from 'node_prog/reach_program.h' namespace 'node_prog':
    cdef cppclass reach_params:
        reach_params()
        bint mode
        remote_node prev_node
        uint64_t dest
        uint32_t hops
        bint reachable

class ReachParams:
    def __init__(self, mode=False, prev_node=RemoteNode(), dest=0, hops=0, reachable=False):
        self.mode = mode
        self.prev_node = prev_node
        self.dest = dest
        self.hops = hops
        self.reachable = reachable

cdef extern from 'node_prog/clustering_program.h' namespace 'node_prog':
    cdef cppclass clustering_params:
        pass

ctypedef fused ParamsType:
    reach_params
    clustering_params

cdef extern from 'client/client.h' namespace 'client':
    cdef cppclass client:
        client(uint64_t my_id, uint64_t vt_id)

        uint64_t begin_tx()
        uint64_t create_node(uint64_t tx_id)
        uint64_t create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2)
        void delete_node(uint64_t tx_id, uint64_t node)
        void delete_edge(uint64_t tx_id, uint64_t edge)
        void end_tx(uint64_t tx_id)
        reach_params run_reach_program(vector[pair[uint64_t, reach_params]] initial_args)
        void start_migration()
        void commit_graph()
        void exit_weaver()

cdef class Client:
    cdef client *thisptr
    def __cinit__(self, uint64_t my_id, uint64_t vt_id):
        self.thisptr = new client(my_id, vt_id)
    def __dealloc__(self):
        del self.thisptr
    def begin_tx(self):
        return self.thisptr.begin_tx()
    def create_node(self, tx_id):
        return self.thisptr.create_node(tx_id)
    def create_edge(self, tx_id, node1, node2):
        return self.thisptr.create_edge(tx_id, node1, node2)
    def delete_node(self, tx_id, node):
        self.thisptr.delete_node(tx_id, node)
    def delete_edge(self, tx_id, edge):
        self.thisptr.delete_edge(tx_id, edge)
    def end_tx(self, tx_id):
        self.thisptr.end_tx(tx_id)
    def run_reach_program(self, init_args):
        cdef vector[pair[uint64_t, reach_params]] c_args
        cdef pair[uint64_t, reach_params] arg_pair
        for rp in init_args:
            arg_pair.first = rp[0]
            arg_pair.second.mode = rp[1].mode
            arg_pair.second.dest = rp[1].dest
            arg_pair.second.reachable = rp[1].reachable
            arg_pair.second.prev_node.loc = rp[1].prev_node.loc
            arg_pair.second.prev_node.handle = rp[1].prev_node.handle
            c_args.push_back(arg_pair)
        c_rp = self.thisptr.run_reach_program(c_args)
        response = ReachParams(hops=c_rp.hops, reachable=c_rp.reachable)
        return response
    def start_migration(self):
        self.thisptr.start_migration()
    def commit_graph(self):
        self.thisptr.commit_graph()
    def exit_weaver(self):
        self.thisptr.exit_weaver()
