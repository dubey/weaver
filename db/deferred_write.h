/*
 * ===============================================================
 *    Description:  DS for deferred write for a migrated node.
 *
 *        Created:  09/16/2013 03:55:25 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_deferred_write_h_
#define weaver_db_deferred_write_h_

namespace db
{
    // state for a deferred write for a migrated node
    struct deferred_write
    {
        transaction::update_type type;
        vc::vclock vclk;
        node_handle_t remote_node;
        uint64_t remote_loc;
        edge_handle_t edge_handle;
        std::unique_ptr<std::string> key, value;

        inline
        deferred_write(transaction::update_type t, vc::vclock &vc) : type(t), vclk(vc) { }
    };
    
    typedef std::vector<deferred_write> def_write_lst;
}

#endif
