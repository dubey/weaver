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
sys.path.append('../../.libs')

import libclient as client

coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

tx_id = c.begin_tx()
c.create_edge(tx_id, 42, 84)
assert(not c.end_tx(tx_id))

tx_id = c.begin_tx()
c.delete_node(tx_id, 29837429)
assert(not c.end_tx(tx_id))

tx_id = c.begin_tx()
c.delete_edge(tx_id, 42, 84)
assert(not c.end_tx(tx_id))

tx_id = c.begin_tx()
c.set_node_property(tx_id, 42, 'color', 'blue')
assert(not c.end_tx(tx_id))

tx_id = c.begin_tx()
c.set_edge_property(tx_id, 42, 84, 'color', 'blue')
assert(not c.end_tx(tx_id))
