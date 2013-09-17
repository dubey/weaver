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

namespace db
{
    // state for a deferred write for a migrated node
    struct deferred_write
    {
        enum message::msg_type type;
        vc::vclock vclk;
        union {
            struct {
                uint64_t node;
            } del_node;
            struct {
                uint64_t edge, n1, n2, loc2;
            } cr_edge;
            struct {
                uint64_t edge;
                uint64_t node;
            } del_edge;
        } request;

        inline
        deferred_write(enum message::msg_type t, vc::vclock &vc, uint64_t n)
            : type(t)
            , vclk(vc)
        {
            request.del_node.node = n;
        }

        inline
        deferred_write(enum message::msg_type t, vc::vclock &vc, uint64_t e,
            uint64_t nd1, uint64_t nd2, uint64_t l2)
            : type(t)
            , vclk(vc)
        {
            request.cr_edge.edge = e;
            request.cr_edge.n1 = nd1;
            request.cr_edge.n2 = nd2;
            request.cr_edge.loc2 = l2;
        }
        
        inline
        deferred_write(enum message::msg_type t, vc::vclock &vc, uint64_t e, uint64_t n)
            : type(t)
            , vclk(vc)
        {
            request.del_edge.edge = e;
            request.del_edge.node = n;
        }
    };
    
    typedef std::vector<deferred_write> def_write_lst;
}
