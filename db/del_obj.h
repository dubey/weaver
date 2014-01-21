/*
 * ===============================================================
 *    Description:  DS for storing a deleted object to be
 *                  permanently removed later on. 
 *
 *        Created:  2014-01-20 15:10:53
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

namespace db
{
    struct del_obj
    {
        enum message::msg_type type;
        vc::vclock vclk;
        uint64_t node, edge;

        inline
        del_obj(enum message::msg_type t, vc::vclock &vc, uint64_t n, uint64_t e=0)
            : type(t)
            , vclk(vc)
            , node(n)
            , edge(e)
        { }
    };
}
