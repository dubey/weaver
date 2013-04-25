/*
 * ===============================================================
 *    Description:  Cache for user query results
 *
 *        Created:  12/04/2012 09:58:54 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CACHE__
#define __CACHE__

#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>

#include "common/property.h"

namespace cache
{
    typedef std::unordered_map<uint64_t, node_prog::Deletable*> node_map;
    typedef std::unordered_map<node_prog::prog_type, node_map> prog_map;
    typedef std::unordered_map<uint64_t, std::pair<node_prog::prog_type, std::vector<uint64_t>>> invalid_map;

    class program_cache
    {
        prog_map prog_cache, transient_prog_cache;
        invalid_map itable, transient_itable;
        po6::threads::mutex mutex;

        public:
            program_cache();

        public:
            bool entry_exists(node_prog::prog_type t, uint64_t node_handle);
            node_prog::Deletable* get_cache(node_prog::prog_type t, uint64_t node_handle);
            void put_cache(uint64_t req_id, node_prog::prog_type t, uint64_t node_handle, node_prog::Deletable *new_cache);
            void delete_cache(uint64_t req_id);

        private:
            bool entry_exists(node_prog::prog_type t, uint64_t node_handle, prog_map &pc);
            node_prog::Deletable* get_cache(node_prog::prog_type t, uint64_t node_handle, prog_map &pc);
            void put_cache(uint64_t req_id, node_prog::prog_type t, uint64_t node_handle, node_prog::Deletable *new_cache, prog_map &pc, invalid_map &it);
            void delete_cache(uint64_t req_id, prog_map &pc, invalid_map &it);
    };

    inline
    program_cache :: program_cache()
    {
        node_map new_node_map;
        prog_cache.emplace(node_prog::REACHABILITY, new_node_map);
        prog_cache.emplace(node_prog::DIJKSTRA, new_node_map);
        prog_cache.emplace(node_prog::CLUSTERING, new_node_map);
        transient_prog_cache.emplace(node_prog::REACHABILITY, new_node_map);
        transient_prog_cache.emplace(node_prog::DIJKSTRA, new_node_map);
        transient_prog_cache.emplace(node_prog::CLUSTERING, new_node_map);
    }

    inline bool
    program_cache :: entry_exists(node_prog::prog_type t, uint64_t node_handle, prog_map &pc)
    {
        node_map &nmap = pc.at(t);
        node_map::iterator nmap_iter = nmap.find(node_handle);
        return (nmap_iter != nmap.end());
    }

    inline node_prog::Deletable*
    program_cache :: get_cache(node_prog::prog_type t, uint64_t node_handle, prog_map &pc)
    {
        node_prog::Deletable *cache = NULL;
        if (entry_exists(t, node_handle, pc)) {
            cache = pc.at(t).at(node_handle);
        }
        return cache;
    }

    inline void
    program_cache :: put_cache(uint64_t req_id, node_prog::prog_type t, uint64_t node_handle, node_prog::Deletable *new_cache, prog_map &pc, invalid_map &it)
    {
        if (entry_exists(t, node_handle, pc)) {
            node_prog::Deletable *old_cache = get_cache(t, node_handle, pc);
            delete old_cache;
            pc[t][node_handle] = new_cache;
        } else {
            pc[t][node_handle] = new_cache;
            if (it.find(req_id) == it.end()) {
                it.emplace(req_id, std::make_pair(t, std::vector<uint64_t>()));
            }
            it.at(req_id).second.emplace_back(node_handle);
        }
    }

    inline void
    program_cache :: delete_cache(uint64_t req_id, prog_map &pc, invalid_map &it)
    {
        // Deleting the cache corresponding to this req_id still leaves
        // the req_id in the list of cached_ids at the other nodes.
        // If and when those nodes are deleted, trying to delete this req_id
        // again would be a no-op.
        if (it.find(req_id) != it.end()) {
            std::pair<node_prog::prog_type, std::vector<uint64_t>> &inv = it.at(req_id);
            for (auto &node: inv.second) {
                node_map &nmap = pc.at(inv.first);
                delete nmap.at(node);
                nmap.erase(node);
                if (nmap.size() == 0) {
                    pc.erase(inv.first);
                }
            }
            it.erase(req_id);
        }
    }

    /*
    inline void
    program_cache :: commit(uint64_t id)
    {
        itable::iterator it;
        cache_mutex.lock();
        it = transient_invalidation_table.find(id);
        if (it != transient_invalidation_table.end()) {
            // inserting into cache
            uint64_t dest_node = it->second.second;
            int dest_loc = it->second.first;
            cached_object &transient_cobj = transient_cache_table[dest_loc]->at(dest_node);
            cached_object &cobj = (*cache_table[dest_loc])[dest_node];
            std::vector<uint64_t> to_delete;
            for (auto &node_iter: transient_cobj.nodes) {
                if (node_iter.second == id) {
                    cobj.nodes.emplace(node_iter.first, node_iter.second);
                }
                to_delete.emplace_back(node_iter.first);
            }
            if (transient_cobj.edge_props.find(id) != transient_cobj.edge_props.end()) {
                cobj.edge_props[id] = transient_cobj.edge_props[id];
            }
            invalidation_table.emplace(id, std::make_pair(dest_loc, dest_node));
            // removing from transient cache
            if (to_delete.size() == transient_cobj.nodes.size()) {
                transient_cache_table[dest_loc]->erase(dest_node);
            } else {
                for (auto del_node: to_delete) {
                    transient_cobj.nodes.erase(del_node);
                }
                transient_cobj.edge_props.erase(id);
            }
            transient_invalidation_table.erase(id);
        }
        cache_mutex.unlock();
    }
    */
}

#endif //__CACHE__
