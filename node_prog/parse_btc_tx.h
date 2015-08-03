/*
 * ===============================================================
 *    Description:  Parse a node as a btc tx and return as client
 *                  node
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_parse_btc_tx_h_
#define weaver_node_prog_parse_btc_tx_h_

#include "node_prog/node.h"

namespace node_prog
{
    void parse_btc_tx(cl::node &tx, node_prog::node &n);
}

#endif
