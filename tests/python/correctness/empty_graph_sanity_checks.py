#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Sanity checks -- transactions on an empty
#                  graph should all fail. 
# 
#        Created:  2013-12-17 16:56:37
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys

import weaver.client as client

coord_id = 0
c = client.Client('127.0.0.1', 2002)

c.begin_tx()
c.create_edge('ayush', '42')
assert not c.end_tx(), 'create edge'

c.begin_tx()
c.delete_node('298437')
assert not c.end_tx(), 'delete node'

c.begin_tx()
c.delete_edge('42', '298437')
assert not c.end_tx(), 'delete edge'

c.begin_tx()
c.set_node_property('egs', 'type', 'user')
assert not c.end_tx(), 'set node property'

c.begin_tx()
c.set_edge_property('84', '42', 'color', 'blue')
assert not c.end_tx(), 'set edge property'

print 'Pass empty_graph_sanity_checks.'
