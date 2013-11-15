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
sys.path.append('../bindings/python')

import client
import time
from test_base import test_graph

# creating line graph
nodes = []
num_nodes = 100000
num_clients = 2
clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID+i, i % client._NUM_VTS))
print 'Created client'
start = time.time()
tg = test_graph(clients, num_nodes, 2*num_nodes)
end = time.time()
print 'Created graph in ' + str(end-start) + ' seconds'
