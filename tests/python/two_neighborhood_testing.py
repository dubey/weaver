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
c.set_node_property(tx_id, center, 'name', 'BAD1')
c.set_node_property(tx_id, nodes[0], 'name', 'BAD2')
c.set_node_property(tx_id, nodes[2], 'name', 'Scarlet')
c.set_node_property(tx_id, nodes[3], 'name', 'Greg')
c.set_node_property(tx_id, nodes[4], 'name', 'Ayush')
c.create_edge(tx_id, nodes[0], nodes[1])
c.create_edge(tx_id, nodes[1], nodes[2])
c.end_tx(tx_id)

print nodes
print sc.two_neighborhood(center, "name", caching=True)
print sc.two_neighborhood(center, "nope", caching=True)
tx_id = c.begin_tx()
new_2_hop = c.create_node(tx_id) 
c.create_edge(tx_id, nodes[1], new_2_hop)
c.set_node_property(tx_id, new_2_hop, 'name', 'Sam')
assert(c.end_tx(tx_id))
print "adding two hop neighbor"
print sc.two_neighborhood(center, "name", caching=True)

print "adding 1 and two hop neighbors"
tx_id = c.begin_tx()
new_1_hop = c.create_node(tx_id) 
to_del = c.create_edge(tx_id, center, new_1_hop)
c.set_node_property(tx_id, new_1_hop, 'name', 'Robert')

new_2_hops1 = c.create_node(tx_id) 
new_2_hops2 = c.create_node(tx_id) 
c.create_edge(tx_id, new_1_hop, new_2_hops1)
c.create_edge(tx_id, new_1_hop, new_2_hops2)
c.set_node_property(tx_id, new_2_hops1, 'name', 'Jianeng')
c.set_node_property(tx_id, new_2_hops2, 'name', 'Sarah')
assert(c.end_tx(tx_id))
print sc.two_neighborhood(center, "name", caching=True)
print "deleting node"

tx_id = c.begin_tx()
c.delete_edge(tx_id, to_del, center)
c.delete_node(tx_id, new_1_hop)
assert(c.end_tx(tx_id))
print sc.two_neighborhood(center, "name", caching=True)
print "done!"
