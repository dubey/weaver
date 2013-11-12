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

# creating line graph
nodes = []
num_nodes = 100
print client._CLIENT_ID
c = client.Client(client._CLIENT_ID+1, 0)
print 'Created client'
for i in range(num_nodes):
    tx_id = c.begin_tx()
    nodes.append(c.create_node(tx_id))
    c.end_tx(tx_id)
    print 'Created node ' + str(i)
for i in range(num_nodes-1):
    tx_id = c.begin_tx()
    c.create_edge(tx_id, nodes[i], nodes[i+1])
    c.end_tx(tx_id)
    print 'Created edge ' + str(i)
print 'Created graph'
