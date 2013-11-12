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
import random as rand
import networkx as nx

TX_SIZE = 100

class test_graph:
    """
    Generate a random graph with n nodes and m edges
    Graph generation algorithm could be
        'gnm': G_{n,m} graph
        'gnp': G_{n,p} graph
        'sf': scale-free graph with n nodes
        'ba': Barabasi-Albert graph
    """
    def __init__(self, client, n, m, num_vts=1, gen_alg='gnm', seed=int(time.time())):
        self.nodes = [0 for i in range(n)]
        self.edges = {}
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
        node_map = {}
        cntr = 0
        tx_id = 0
        for node in self.nxgraph.nodes():
            if cntr % TX_SIZE == 0:
                tx_id = client.begin_tx()
            wnode = client.create_node(tx_id)
            self.nodes.append(wnode)
            node_map[node] = wnode
            self.edges[wnode] = []
            if cntr % TX_SIZE == (TX_SIZE-1) or TX_SIZE == 1:
                client.end_tx(tx_id)
            cntr += 1
        print 'Added ' + str(cntr) + ' nodes'
        cntr = 0
        for edge in self.nxgraph.edges():
            if cntr % TX_SIZE == 0:
                tx_id = client.begin_tx()
            client.create_edge(tx_id, node_map[edge[0]], node_map[edge[1]])
            self.edges[node_map[edge[0]]].append(node_map[edge[1]])
            if cntr % TX_SIZE == (TX_SIZE-1) or TX_SIZE == 1:
                client.end_tx(tx_id)
            cntr += 1
        print 'Added ' + str(cntr) + ' edges'
        g_out = open('graph.rec', 'w')
        for node in self.edges:
            line = str(node)
            for nbr in self.edges[node]:
                line += ' ' + str(nbr)
            g_out.write(line + '\n')
        g_out.close()
