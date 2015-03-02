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
        transaction::update_type type;
        vc::vclock vclk;
        node_handle_t node;
        edge *e;
        std::vector<bool> no_outstanding_progs;

        inline
        del_obj(transaction::update_type t, vc::vclock &vc, const node_handle_t &n, edge *del_edge)
            : type(t)
            , vclk(vc)
            , node(n)
            , e(del_edge)
            , no_outstanding_progs(NumVts, false)
        { }
    };

    // for permanent deletion priority queue
    struct perm_del_compare
        : std::binary_function<del_obj*, del_obj*, bool>
    {
        bool operator()(const del_obj* const &dw1, const del_obj* const &dw2)
        {
            assert(dw1->vclk.clock.size() == ClkSz);
            assert(dw2->vclk.clock.size() == ClkSz);
            for (uint64_t i = 0; i < ClkSz; i++) {
                if (dw1->vclk.clock[i] <= dw2->vclk.clock[i]) {
                    return false;
                }
            }
            return true;
        }
    };
}

#endif
