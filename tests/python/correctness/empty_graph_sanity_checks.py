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

try:
    import weaver.client as client
except ImportError:
    import client

config_file=''
if len(sys.argv) > 1:
    config_file = sys.argv[1]

coord_id = 0
c = client.Client('127.0.0.1', 2002, config_file)

def try_commit(c, msg):
    try:
        c.end_tx()
        assert False, msg
    except client.WeaverError:
        pass

c.begin_tx()
c.create_edge('ayush', '42')
try_commit(c, 'create edge')

c.begin_tx()
c.delete_node('298437')
try_commit(c, 'delete node')

c.begin_tx()
c.delete_edge('42', '298437')
try_commit(c, 'delete edge')

c.begin_tx()
c.set_node_property('egs', 'type', 'user')
try_commit(c, 'set node property')

c.begin_tx()
c.set_edge_property('84', '42', 'color', 'blue')
try_commit(c, 'set edge property')

print 'Pass empty_graph_sanity_checks.'
