#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Testing Weaver serializability and transaction
#                  correctness.
# 
#        Created:  2014-01-24 16:18:27
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
sys.path.append('../../bindings/python')

import client
import time
import random
import threading
from test_base import test_graph
from sets import Set

num_clients = 10
num_vts = 2
num_nodes = 10000
edge_factor = 1
edge_cnt = 0
init_tx_sz = 100
assert (num_nodes % init_tx_sz == 0)
assert (int(edge_factor * num_nodes) % init_tx_sz == 0)
wr_loops = 100
rd_loops = 100
clients = []
n_hndls = []
n_hndls_cpy = []
created_edges = Set([])
deleted_edges = Set([])
graph = {}
gmutex = threading.Lock() # graph DS mutex
pmutex = threading.Lock() # printing mutex

# if odd_even = -1, then don't care
# if odd_even = 0, then even
# if odd_even = 1, then odd
def get_random_idx(odd_even=-1):
    if odd_even == -1:
        return random.randrange(0, num_nodes)
    elif odd_even == 1:
        return random.randrange(1, num_nodes, 2)
    elif odd_even == 0:
        return random.randrange(0, num_nodes, 2)
    else:
        return -1 # error

# check if n2 already a nbr of n1
def check_repeat(n1, n2, edges):
    for e in graph[n1]:
        if n2 == e[0]:
            return True
    return (n1, n2) in edges

# create graph edges
# after transaction has finished:
# there is no edge (n1, n2) such that n1 != n2 mod 2
# there are no self-loops
# there maybe parallel edges
def create_edges(c, n_edges):
    global edge_cnt
    tx_id = c.begin_tx()
    edges = []
    with gmutex:
        while n_edges > 0:
            n1 = get_random_idx()
            n2 = get_random_idx()
            while n2 == n1:
                n2 = get_random_idx()
            e_hndl = c.create_edge(tx_id, n_hndls[n1], n_hndls[n2])
            created_edges.add(e_hndl)
            edges.append((n1, n2, e_hndl))
            if n1 % 2 == n2 % 2:
                n_edges -= 1
        for e in edges:
            if e[0] % 2 == e[1] % 2:
                edge_cnt += 1
            else:
                assert e[2] not in deleted_edges
                assert e[2] in created_edges
                c.delete_edge(tx_id, e[2], n_hndls[e[0]])
                deleted_edges.add(e[2])
    assert c.end_tx(tx_id)
    with gmutex:
        for e in edges:
            if e[0] % 2 == e[1] % 2:
                graph[e[0]].append((e[1], e[2]))

# delete n_edges randomly chosen edges
def delete_edges(c, n_edges):
    global edge_cnt
    tx_id = c.begin_tx()
    with gmutex:
        for i in range(n_edges):
            num_nbrs = 0
            while num_nbrs == 0:
                n = get_random_idx()
                num_nbrs = len(graph[n])
            e_idx = random.randrange(0, num_nbrs)
            e_hndl = ((graph[n])[e_idx])[1]
            assert e_hndl not in deleted_edges
            assert e_hndl in created_edges
            c.delete_edge(tx_id, e_hndl, n_hndls[n])
            deleted_edges.add(e_hndl)
            del (graph[n])[e_idx]
            edge_cnt -= 1
            assert len(graph[n]) == (num_nbrs-1)
    assert c.end_tx(tx_id)

# if n1 != n2 mod 2, then !reachable(n1, n2)
def reach_requests(c, n_reqs):
    pos_reqs = []
    neg_reqs = []
    for i in range(n_reqs):
            n1 = get_random_idx()
            n2 = get_random_idx(n1 % 2)
            while n2 == n1:
                n2 = get_random_idx(n1 % 2)
            pos_reqs.append((n_hndls_cpy[n1], n_hndls_cpy[n2]))
            n1 = get_random_idx()
            n2 = get_random_idx(1 if n1 % 2 == 0 else 0)
            neg_reqs.append((n_hndls_cpy[n1], n_hndls_cpy[n2]))
    rp = client.ReachParams()
    for r in pos_reqs:
        rp.dest = r[1]
        prog_args = [(r[0], rp)]
        response = c.run_reach_program(prog_args)
    for r in neg_reqs:
        rp.dest = r[1]
        prog_args = [(r[0], rp)]
        response = c.run_reach_program(prog_args)
        assert (not response.reachable)


# create initial graph
def init_graph(c):
    global edge_cnt
    global n_hndls_cpy
    with gmutex:
        for i in range(num_nodes):
            if i % init_tx_sz == 0:
                tx_id = c.begin_tx()
            n = c.create_node(tx_id)
            n_hndls.append(n)
            graph[i] = []
            if i % init_tx_sz == (init_tx_sz-1):
                assert c.end_tx(tx_id)
        n_hndls_cpy = n_hndls[:]
        for i in range(int(edge_factor * num_nodes)):
            if i % init_tx_sz == 0:
                tx_id = c.begin_tx()
            n1 = get_random_idx()
            n2 = get_random_idx(n1 % 2)
            while n2 == n1: # no self-loops
                n2 = get_random_idx(n1 % 2)
            assert n1 < len(n_hndls)
            assert n2 < len(n_hndls)
            e = c.create_edge(tx_id, n_hndls[n1], n_hndls[n2])
            created_edges.add(e)
            graph[n1].append((n2, e))
            edge_cnt += 1
            if i % init_tx_sz == (init_tx_sz-1):
                assert c.end_tx(tx_id)

def write_loop(c, tid):
    for i in range(wr_loops):
        create_edges(c, 100)
        delete_edges(c, 100)
        with pmutex:
            print str(tid) + ': Done write loop ' + str(i)

def read_loop(c, tid):
    for i in range(rd_loops):
        reach_requests(c, 20)
        with pmutex:
            print str(tid) + ': Read loop ' + str(i)

for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))
init_graph(clients[0])
print 'Created graph'
threads = []
for i in range(num_clients/2):
    t = threading.Thread(target=write_loop, args=(clients[i], i))
    t.start()
    threads.append(t)
for i in range(num_clients/2, num_clients):
    t = threading.Thread(target=read_loop, args=(clients[i], i))
    t.start()
    threads.append(t)
for t in threads:
    t.join()
