/*
 * ===============================================================
 *    Description:  State corresponding to a node program.
 *
 *        Created:  04/23/2013 10:44:00 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PROG_STATE__
#define __PROG_STATE__

#include <unordered_map>

#include "node_prog_type.h"

namespace state
{
    typedef std::unordered_map<uint64_t, db::Deletable*> node_map;
    typedef std::unordered_map<uint64_t, node_map> req_map;
    typedef std::unordered_map<db::prog_type, req_map> prog_map;

    class program_state
    {
        prog_map prog_state;

        public:
            program_state();

        public:
            bool state_exists(db::prog_type t, uint64_t req_id, uint64_t node_handle);
            db::Deletable* get_state(db::prog_type t, uint64_t req_id, uint64_t node_handle);
            void put_state(db::prog_type t, uint64_t req_id, uint64_t node_handle, db::Deletable *new_state);
    };

    program_state :: program_state()
    {
        req_map new_req_map;
        prog_state.emplace(db::REACHABILITY, new_req_map);
        prog_state.emplace(db::DIJKSTRA, new_req_map);
        prog_state.emplace(db::CLUSTERING, new_req_map);
    }

    inline bool
    program_state :: state_exists(db::prog_type t, uint64_t req_id, uint64_t node_handle)
    {
        req_map &rmap = prog_state.at(t);
        req_map::iterator rmap_iter = rmap.find(req_id);
        if (rmap_iter == rmap.end()) {
            return false;
        }
        node_map &nmap = rmap.at(req_id);
        node_map::iterator nmap_iter = nmap.find(node_handle);
        if (nmap_iter == nmap.end()) {
            return false;
        } else {
            return true;
        }
    }

    inline db::Deletable* 
    program_state :: get_state(db::prog_type t, uint64_t req_id, uint64_t node_handle)
    {
        db::Deletable *state = NULL;
        if (state_exists(t, req_id, node_handle)) {
            state = prog_state.at(t).at(req_id).at(node_handle);
        }
        return state;
    }

    inline void
    program_state :: put_state(db::prog_type t, uint64_t req_id, uint64_t node_handle, db::Deletable *new_state)
    {
        if (state_exists(t, req_id, node_handle)) {
            db::Deletable *old_state = get_state(t, req_id, node_handle);
            delete old_state;
        }
        prog_state[t][req_id][node_handle] = new_state;
    }
}

#endif
