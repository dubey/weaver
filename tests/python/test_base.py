# 
# ===============================================================
#    Description:  Base Python client code for graph creation.
# 
#        Created:  11/12/2013 01:38:59 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
sys.path.append('../bindings/python')

import client
import time
import threading
import copy
import sys
import random as rand
import networkx as nx

TX_SIZE = 300

class test_graph:

    """
    Generate a random graph with n nodes and m edges
    Graph generation algorithm could be
        'gnm': G_{n,m} graph
        'gnp': G_{n,p} graph
        'sf': scale-free graph with n nodes
        'ba': Barabasi-Albert graph
    """
    def __init__(self, clients, n, m, num_vts=1, gen_alg='gnm', seed=int(time.time())):
        self.nodes = [0 for i in range(n)]
        self.edges = {}
        self.edge_handles = {}
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.startThread = False
        self.node_map = {} # Networkx node handle -> Weaver node handle

        # create Networkx graph
        if gen_alg == 'gnm':
            self.nxgraph = nx.gnm_random_graph(n, m, seed, True)
        elif gen_alg == 'gnp':
            p = float(m)/n
            if p < 0.2:
                self.nxgraph = nx.fast_gnp_random_graph(n, p, seed, True)
            else:
                self.nxgraph = nx.gnp_random_graph(n, p, seed, True)
        elif gen_alg == 'sf':
            self.nxgraph = nx.scale_free_graph(n=n, seed=seed)
        elif gen_alg == 'ba':
            self.nxgraph = nx.barabasi_albert_graph(n, m)
        else:
            print 'Unexpected graph generation algorithm'
            exit()

        # create Weaver graph
        num_threads = len(clients)

        # adding nodes
        nx_nodes = self.nxgraph.nodes()
        nodes_per_thread = n/num_threads
        node_sets = []
        threads = []
        for i in range(num_threads - 1):
            node_sets.append(nx_nodes[(nodes_per_thread)*i : (nodes_per_thread)*(i+1)])
        node_sets.append(nx_nodes[(nodes_per_thread)*(num_threads-1) :])
        for i in range(num_threads):
            t = threading.Thread(target=self.create_nodes, args=(node_sets[i], clients[i], i))
            t.start()
            threads.append(t)
            print 'Started thread ' + str(i) + ' for create nodes'
        self.lock.acquire()
        self.startThread = True
        self.cv.notifyAll()
        self.lock.release()
        for t in threads:
            t.join()
        del node_sets[:]
        del nx_nodes[:]
        print 'Added nodes'
        print 'Node map:'

        # adding edges
        nx_edges = self.nxgraph.edges()
        edges_per_thread = len(nx_edges)/num_threads
        edge_sets = []
        threads = []
        for i in range(num_threads - 1):
            edge_sets.append(nx_edges[(edges_per_thread)*i : (edges_per_thread)*(i+1)])
        edge_sets.append(nx_edges[(edges_per_thread)*(num_threads-1) :])
        self.startThread = False
        for i in range(num_threads):
            t = threading.Thread(target=self.create_edges, args=(edge_sets[i], clients[i], i, copy.deepcopy(self.node_map)))
            t.start()
            threads.append(t)
            print 'Started thread ' + str(i) + ' for create edges'
        self.lock.acquire()
        self.startThread = True
        self.cv.notifyAll()
        self.lock.release()
        for t in threads:
            t.join()
        del edge_sets[:]
        del nx_edges[:]
        print 'Added edges'

        # write graph to file
        g_out = open('graph.rec', 'w')
        for node in self.edges:
            line = str(node)
            for nbr in self.edges[node]:
                line += ' ' + str(nbr)
            g_out.write(line + '\n')
        g_out.close()

    def create_nodes(self, nx_nodes, client, client_id):
        with self.lock:
            while not self.startThread:
                self.cv.wait()
        tx_id = 0
        cntr = 0
        w_nodes = []
        for node in nx_nodes:
            if cntr % TX_SIZE == 0:
                tx_id = client.begin_tx()
            wnode = client.create_node(tx_id)
            w_nodes.append(wnode)
            if cntr % TX_SIZE == (TX_SIZE-1) or TX_SIZE == 1 or cntr == (len(nx_nodes)-1):
                client.end_tx(tx_id)
            if cntr % 1000 == 0:
                print 'Client ' + str(client_id) + ' created ' + str(cntr) + ' nodes'
            cntr += 1
        with self.lock:
            for i in range(len(nx_nodes)):
                self.nodes[i] = w_nodes[i]
                self.node_map[nx_nodes[i]] = w_nodes[i]
                self.edges[w_nodes[i]] = []
                self.edge_handles[w_nodes[i]] = []

    def create_edges(self, nx_edges, client, client_id, node_map):
        with self.lock:
            while not self.startThread:
                self.cv.wait()
        tx_id = 0
        cntr = 0
        edges = {}
        for edge in nx_edges:
            if cntr % TX_SIZE == 0:
                tx_id = client.begin_tx()
            self.edge_handles[node_map[edge[0]]].append(client.create_edge(tx_id, node_map[edge[0]], node_map[edge[1]]))
            if node_map[edge[0]] not in edges:
                edges[node_map[edge[0]]] = []
            edges[node_map[edge[0]]].append(node_map[edge[1]])
            if cntr % TX_SIZE == (TX_SIZE-1) or TX_SIZE == 1 or cntr == (len(nx_edges)-1):
                client.end_tx(tx_id)
            if cntr % 1000 == 0:
                print 'Client ' + str(client_id) + ' created ' + str(cntr) + ' edges'
            cntr += 1
        with self.lock:
            for n in edges:
                self.edges[n].extend(edges[n])

