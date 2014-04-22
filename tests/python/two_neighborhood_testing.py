import sys
sys.path.append('../../.libs')

import time
import libclient as client
import simple_client

# creating graph
nodes = [None] * 5
coord_id = 0
c = client.Client(client._CLIENT_ID+1, coord_id)
sc = simple_client.simple_client(c)

tx_id = c.begin_tx()
center = c.create_node(tx_id)
nodes[0] = c.create_node(tx_id)
nodes[1] = c.create_node(tx_id)
nodes[2] = c.create_node(tx_id)
c.create_edge(tx_id, center, nodes[0])
c.create_edge(tx_id, center, nodes[1])
c.create_edge(tx_id, center, nodes[2])
nodes[3] = c.create_node(tx_id)
c.create_edge(tx_id, nodes[2], nodes[3])
nodes[4] = c.create_node(tx_id)
c.create_edge(tx_id, nodes[0], nodes[4])
c.set_node_property(tx_id, nodes[2], 'name', 'Scarlet')
c.set_node_property(tx_id, nodes[3], 'name', 'Greg')
c.set_node_property(tx_id, nodes[4], 'name', 'Ayush')
c.create_edge(tx_id, nodes[0], nodes[1])
c.create_edge(tx_id, nodes[1], nodes[2])
c.end_tx(tx_id)

print nodes
print sc.two_neighborhood(center, "name")
print sc.two_neighborhood(center, "nope")
print sc.two_neighborhood(nodes[0], "name")
print "done!"
