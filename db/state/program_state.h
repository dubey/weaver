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
#include <po6/threads/mutex.h>

#include "node_prog/node_prog_type.h"

namespace state
{
    typedef std::unordered_map<uint64_t, node_prog::Deletable*> node_map;
    typedef std::unordered_map<uint64_t, node_map> req_map;
    typedef std::unordered_map<node_prog::prog_type, req_map> prog_map;

    class program_state
    {
        prog_map prog_state;
        po6::threads::mutex mutex;

        public:
            program_state();

        public:
            bool state_exists(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle);
            node_prog::Deletable* get_state(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle);
            void put_state(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle, node_prog::Deletable *new_state);
    };

    program_state :: program_state()
    {
        req_map new_req_map;
        prog_state.emplace(node_prog::REACHABILITY, new_req_map);
        prog_state.emplace(node_prog::DIJKSTRA, new_req_map);
        prog_state.emplace(node_prog::CLUSTERING, new_req_map);
    }

    inline bool
    program_state :: state_exists(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle)
    {
        mutex.lock();
        req_map &rmap = prog_state.at(t);
        req_map::iterator rmap_iter = rmap.find(req_id);
        if (rmap_iter == rmap.end()) {
            mutex.unlock();
            return false;
        }
        node_map &nmap = rmap.at(req_id);
        node_map::iterator nmap_iter = nmap.find(node_handle);
        if (nmap_iter == nmap.end()) {
            mutex.unlock();
            return false;
        } else {
            mutex.unlock();
            return true;
        }
    }

    inline node_prog::Deletable* 
    program_state :: get_state(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle)
    {
        node_prog::Deletable *state = NULL;
        mutex.lock();
        if (state_exists(t, req_id, node_handle)) {
            state = prog_state.at(t).at(req_id).at(node_handle);
        }
        mutex.unlock();
        return state;
    }

    inline void
    program_state :: put_state(node_prog::prog_type t, uint64_t req_id, uint64_t node_handle, node_prog::Deletable *new_state)
    {
        mutex.lock();
        if (state_exists(t, req_id, node_handle)) {
            node_prog::Deletable *old_state = get_state(t, req_id, node_handle);
            delete old_state;
        }
        prog_state[t][req_id][node_handle] = new_state;
        mutex.unlock();
    }
}

#endif
