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

#ifndef weaver_db_del_obj_h_
#define weaver_db_del_obj_h_

namespace db
{
    struct del_obj
    {
        enum message::msg_type type;
        vc::vclock vclk;
        uint64_t node, edge;
        std::bitset<NUM_VTS> no_outstanding_progs;

        inline
        del_obj(enum message::msg_type t, vc::vclock &vc, uint64_t n, uint64_t e=0)
            : type(t)
            , vclk(vc)
            , node(n)
            , edge(e)
        { }
    };

    // for permanent deletion priority queue
    struct perm_del_compare
        : std::binary_function<del_obj*, del_obj*, bool>
    {
        bool operator()(const del_obj* const &dw1, const del_obj* const &dw2)
        {
            assert(dw1->vclk.clock.size() == NUM_VTS);
            assert(dw2->vclk.clock.size() == NUM_VTS);
            for (uint64_t i = 0; i < NUM_VTS; i++) {
                if (dw1->vclk.clock[i] <= dw2->vclk.clock[i]) {
                    return false;
                }
            }
            return true;
        }
    };
}

#endif
