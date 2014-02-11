#! /usr/bin/env python
# 
# ===============================================================
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time
import code
sys.path.append('../../bindings/python')

print "interactive demo, start with -i flag"

import client
print "import client"
import simple_client
print "import simple_client"

coord_id = 0
print "coord_id = 0"
c = client.Client(client._CLIENT_ID, coord_id)
print "c = client.Client(client._CLIENT_ID, coord_id)"

tx_id = c.begin_tx()
nodes = dict()
nodes['saturn'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['saturn'], 'name', 'saturn')
c.set_node_property(tx_id, nodes['saturn'], 'age', '10000')
c.set_node_property(tx_id, nodes['saturn'], 'type', 'titan')

nodes['sky'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['sky'], 'name', 'sky')
c.set_node_property(tx_id, nodes['sky'], 'type', 'location')

nodes['sea'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['sea'], 'name', 'sea')
c.set_node_property(tx_id, nodes['sea'], 'type', 'location')

nodes['jupiter'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['jupiter'], 'name', 'jupiter')
c.set_node_property(tx_id, nodes['jupiter'], 'age', '5000')
c.set_node_property(tx_id, nodes['jupiter'], 'type', 'god')

nodes['neptune'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['neptune'], 'name', 'neptune')
c.set_node_property(tx_id, nodes['neptune'], 'age', '4500')
c.set_node_property(tx_id, nodes['neptune'], 'type', 'god')

nodes['hercules'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['hercules'], 'name', 'hercules')
c.set_node_property(tx_id, nodes['hercules'], 'age', '30')
c.set_node_property(tx_id, nodes['hercules'], 'type', 'demigod')

nodes['alcmene'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['alcmene'], 'name', 'alcmene')
c.set_node_property(tx_id, nodes['alcmene'], 'age', '45')
c.set_node_property(tx_id, nodes['alcmene'], 'type', 'human')

nodes['pluto'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['pluto'], 'name', 'pluto')
c.set_node_property(tx_id, nodes['pluto'], 'age', '4000')
c.set_node_property(tx_id, nodes['pluto'], 'type', 'god')

nodes['nemean'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['nemean'], 'name', 'nemean')
c.set_node_property(tx_id, nodes['nemean'], 'type', 'monster')

nodes['hydra'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['hydra'], 'name', 'hydra')
c.set_node_property(tx_id, nodes['hydra'], 'type', 'monster')

nodes['cerberus'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['cerberus'], 'name', 'cerberus')
c.set_node_property(tx_id, nodes['cerberus'], 'type', 'monster')

nodes['tartarus'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['tartarus'], 'name', 'tartarus')
c.set_node_property(tx_id, nodes['tartarus'], 'type', 'location')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['saturn'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'father')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['sky'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'lives', 'reason: loves fresh breezes')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['neptune'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['pluto'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['neptune'], nodes['sea'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'lives', 'reason: loves waves')
edge = c.create_edge(tx_id, nodes['neptune'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['neptune'], nodes['pluto'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['hercules'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'relationship', 'father')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['alcmene'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'relationship', 'mother')

edge = c.create_edge(tx_id, nodes['hercules'], nodes['alcmene'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '1')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['hydra'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '2')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['cerberus'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '12')

edge = c.create_edge(tx_id, nodes['pluto'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['neptune'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['tartarus'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'lives', 'reason: no fear of death')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['cerberus'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'pet', '')

edge = c.create_edge(tx_id, nodes['cerberus'], nodes['tartarus'])
c.set_edge_property(tx_id, nodes['cerberus'], edge, 'lives', '')

c.end_tx(tx_id)

print '''
tx_id = c.begin_tx()
nodes = dict()
nodes['saturn'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['saturn'], 'name', 'saturn')
c.set_node_property(tx_id, nodes['saturn'], 'age', '10000')
c.set_node_property(tx_id, nodes['saturn'], 'type', 'titan')

nodes['sky'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['sky'], 'name', 'sky')
c.set_node_property(tx_id, nodes['sky'], 'type', 'location')

nodes['sea'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['sea'], 'name', 'sea')
c.set_node_property(tx_id, nodes['sea'], 'type', 'location')

nodes['jupiter'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['jupiter'], 'name', 'jupiter')
c.set_node_property(tx_id, nodes['jupiter'], 'age', '5000')
c.set_node_property(tx_id, nodes['jupiter'], 'type', 'god')

nodes['neptune'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['neptune'], 'name', 'neptune')
c.set_node_property(tx_id, nodes['neptune'], 'age', '4500')
c.set_node_property(tx_id, nodes['neptune'], 'type', 'god')

nodes['hercules'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['hercules'], 'name', 'hercules')
c.set_node_property(tx_id, nodes['hercules'], 'age', '30')
c.set_node_property(tx_id, nodes['hercules'], 'type', 'demigod')

nodes['alcmene'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['alcmene'], 'name', 'alcmene')
c.set_node_property(tx_id, nodes['alcmene'], 'age', '45')
c.set_node_property(tx_id, nodes['alcmene'], 'type', 'human')

nodes['pluto'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['pluto'], 'name', 'pluto')
c.set_node_property(tx_id, nodes['pluto'], 'age', '4000')
c.set_node_property(tx_id, nodes['pluto'], 'type', 'god')

nodes['nemean'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['nemean'], 'name', 'nemean')
c.set_node_property(tx_id, nodes['nemean'], 'type', 'monster')

nodes['hydra'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['hydra'], 'name', 'hydra')
c.set_node_property(tx_id, nodes['hydra'], 'type', 'monster')

nodes['cerberus'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['cerberus'], 'name', 'cerberus')
c.set_node_property(tx_id, nodes['cerberus'], 'type', 'monster')

nodes['tartarus'] = c.create_node(tx_id)
c.set_node_property(tx_id, nodes['tartarus'], 'name', 'tartarus')
c.set_node_property(tx_id, nodes['tartarus'], 'type', 'location')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['saturn'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'father')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['sky'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'lives', 'reason: loves fresh breezes')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['neptune'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['jupiter'], nodes['pluto'])
c.set_edge_property(tx_id, nodes['jupiter'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['neptune'], nodes['sea'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'lives', 'reason: loves waves')
edge = c.create_edge(tx_id, nodes['neptune'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['neptune'], nodes['pluto'])
c.set_edge_property(tx_id, nodes['neptune'], edge, 'relationship', 'brother')

edge = c.create_edge(tx_id, nodes['hercules'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'relationship', 'father')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['alcmene'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'relationship', 'mother')

edge = c.create_edge(tx_id, nodes['hercules'], nodes['alcmene'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '1')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['hydra'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '2')
edge = c.create_edge(tx_id, nodes['hercules'], nodes['cerberus'])
c.set_edge_property(tx_id, nodes['hercules'], edge, 'battled', '')
c.set_edge_property(tx_id, nodes['hercules'], edge, 'time', '12')

edge = c.create_edge(tx_id, nodes['pluto'], nodes['jupiter'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['neptune'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'relationship', 'brother')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['tartarus'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'lives', 'reason: no fear of death')
edge = c.create_edge(tx_id, nodes['pluto'], nodes['cerberus'])
c.set_edge_property(tx_id, nodes['pluto'], edge, 'pet', '')

edge = c.create_edge(tx_id, nodes['cerberus'], nodes['tartarus'])
c.set_edge_property(tx_id, nodes['cerberus'], edge, 'lives', '')

c.end_tx(tx_id)
'''

sc = simple_client.simple_client(c)

print ""
print "demo time!"
print "reachability:"
print "clustering:"
print "read node properties:"
print '''read edge properties:
rp = client.ReadEdgesPropsParams(keys = ['relationship'])
prog_args = [(node_id, rp)]
response = c.read_edges_props(prog_args)
print response.edges_props

sc.reachability(nodes['pluto'], nodes['hercules'])
'''
