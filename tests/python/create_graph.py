# 
# ===============================================================
#    Description:  Simple graph creation test for Python API.
# 
#        Created:  11/11/2013 12:10:09 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys

import weaver.client as client
import time
from test_base import test_graph

# creating line graph
nodes = []
num_nodes = 2000
num_clients = 1
clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID+i, i % client._NUM_VTS))
print 'Created client'
start = time.time()
tg = test_graph(clients, num_nodes, 2*num_nodes)
print 'Node = ' + str(tg.nodes[0])
print 'Edge to del = ' + str(tg.edge_handles[tg.nodes[0]][0])
tx_id = clients[0].begin_tx()
clients[0].delete_edge(tx_id, tg.edge_handles[tg.nodes[0]][0], tg.nodes[0])
clients[0].end_tx(tx_id)
end = time.time()
print 'Created graph in ' + str(end-start) + ' seconds'
