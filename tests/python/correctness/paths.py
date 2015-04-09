#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Test correctness of discover paths 
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2014, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys

try:
    from weaver import client
except ImportError:
    import client

config_file=''

if len(sys.argv) > 1:
    config_file = sys.argv[1]

# create client object
c = client.Client('127.0.0.1', 2002, config_file)

# create graph
c.begin_tx()

# src, dst
c.create_node('src')
c.create_node('dst')

# layer 1
c.create_node('1a')
c.create_node('1b')
c.create_edge('src', '1a')
c.create_edge('src', '1b')

# layer 2
c.create_node('2a')
c.create_node('2b')
c.create_node('2c')
c.create_node('2d')
c.create_node('2e')
c.create_edge('1a', '2a')
c.create_edge('1a', '2b')
c.create_edge('1a', '2c')
c.create_edge('1b', '2d')
c.create_edge('1b', '2e')

# layer 3
c.create_node('3')
c.create_edge('2c', '3')
c.create_edge('2d', '3')
c.create_edge('src', '3') # src to layer3
c.create_edge('3', '1a') # layer3 to layer1 for cycle

# edges to dst
c.create_edge('2a', 'dst')
c.create_edge('2b', 'dst')
c.create_edge('3', 'dst')
c.create_edge('2e', 'dst')

c.end_tx()

# discover paths between src and dst
#paths = c.discover_paths('src', 'dst')
#print paths
