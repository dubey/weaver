#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Create and delete multiple nodes and edges
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2015, Cornell University, see the LICENSE file
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

# create client object
c = client.Client('127.0.0.1', 2002, config_file)

assert c.aux_index()

c.begin_tx()
c.create_node('ayush')
c.create_node('egs')
c.create_edge('ayush', 'egs', 'e1')
c.create_edge('ayush', 'egs', 'e2')
c.end_tx()

c.begin_tx()
c.delete_edge('e1')
c.end_tx()

c.begin_tx()
c.delete_edge('e2')
c.end_tx()

c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
c.create_edge('ayush', 'egs', 'e2')
c.end_tx()

c.begin_tx()
c.delete_edge('e1')
c.delete_edge('e2')
c.end_tx()

c.begin_tx()
c.delete_node('ayush')
c.delete_node('egs')
c.end_tx()
