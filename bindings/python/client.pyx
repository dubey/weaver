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

cdef extern from "stdint.h":

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t

# end <stolen from Hyperdex/bindings/client.pyx>

cdef extern from "client.h" namespace "client":
    cdef cppclass client:
        client(uint64_t my_id, uint64_t vt_id)

        uint64_t begin_tx()
        uint64_t create_node(uint64_t tx_id)
        uint64_t create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2)
        void delete_node(uint64_t tx_id, uint64_t node)
        void delete_edge(uint64_t tx_id, uint64_t edge)
        void end_tx(uint64_t tx_id)

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
