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

#ifndef __DEFERRED_WRITE__
#define __DEFERRED_WRITE__

namespace db
{
    // state for a deferred write for a migrated node
    struct deferred_write
    {
        enum message::msg_type type;
        vc::vclock vclk;
        uint64_t edge, remote_node, remote_loc;
        std::unique_ptr<std::string> key, value;

        inline
        deferred_write(enum message::msg_type t, vc::vclock &vc) : type(t), vclk(vc) { }
    };
    
    typedef std::vector<deferred_write> def_write_lst;
}

#endif
