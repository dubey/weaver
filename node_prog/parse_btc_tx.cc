/*
 * ===============================================================
 *    Description:  Implement parse_btc_tx
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "node_prog/parse_btc_tx.h"

void
node_prog::parse_btc_tx(cl::node &tx, node_prog::node &n)
{
    std::string txout_str = "TXOUT_";

    n.get_client_node(tx, true, true, true);
    for (auto &p: tx.out_edges) {
        if (p.first.compare(0, txout_str.size(), txout_str) == 0) {
            std::string consm_tx = "CTX_" + p.first.substr(txout_str.size());
            if (n.edge_exists(consm_tx)) {
                node_prog::edge &ctx_edge = n.get_edge(consm_tx);
                cl::edge &e = p.second;

                std::shared_ptr<cl::property> consumed_prop(new cl::property());
                consumed_prop->key = "consumed";
                consumed_prop->value = ctx_edge.get_neighbor().handle;

                e.properties.emplace_back(consumed_prop);
            }
        }
    }
}
