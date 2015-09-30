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
        vclock_ptr_t tdel;
        vclock_ptr_t version; // creat time of the node
        node_handle_t node;
        edge_handle_t edge;
        std::vector<bool> no_outstanding_progs;

        inline
        del_obj(transaction::update_type t, const vclock_ptr_t &td, const vclock_ptr_t &ver, const node_handle_t &n, const edge_handle_t &e)
            : type(t)
            , tdel(td)
            , version(ver)
            , node(n)
            , edge(e)
            , no_outstanding_progs(NumVts, false)
        { }
    };

    // for permanent deletion priority queue
    struct perm_del_compare
        : std::binary_function<del_obj*, del_obj*, bool>
    {
        bool operator()(const del_obj* const &dw1, const del_obj* const &dw2)
        {
            PASSERT(dw1->tdel->clock.size() == ClkSz);
            PASSERT(dw2->tdel->clock.size() == ClkSz);
            for (uint64_t i = 0; i < ClkSz; i++) {
                if (dw1->tdel->clock[i] <= dw2->tdel->clock[i]) {
                    return false;
                }
            }
            return true;
        }
    };
}

#endif
