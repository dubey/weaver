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

namespace std
{
    template <>
    struct hash<po6::net::location> 
    {
        public:
            size_t operator()(po6::net::location x) const throw() 
            {
                size_t h = (size_t)po6::net::location::hash(x);
                return h;
            }
    };
}

namespace cache
{
    class reach_cache
    {
        public:
            // positive traversal information is stored in a hash map:
            // destination node -> set of local nodes which can reach it
            // there is one such hash map for each shard
            std::unordered_map<size_t, std::unordered_set<size_t>> cache_table;
            // invalidation table is used to store a mapping from request id to
            // destination node(s), so that appropriate entries can be removed
            // from the cache_table on cache invalidation
            std::unordered_map<size_t, size_t> invalidation_table;

        public:
            void insert_entry(size_t dest_node, size_t local_node, size_t req_id);
            bool entry_exists(size_t dest_node, size_t local_node);
            void remove_entry(size_t req_id);
        
        private:
            int get_pointer_to_value(size_t dest_node, size_t local_node);
    };

    inline int
    reach_cache :: get_pointer_to_value(size_t dest_node, size_t local_node)
    {
        std::unordered_map<size_t, std::unordered_set<size_t>>::iterator iter1;
        iter1 = cache_table.find(dest_node);
        if (iter1 == cache_table.end()) {
            return -1;
        }
        std::unordered_set<size_t>::iterator iter2;
        iter2 = iter1->second.find(local_node);
        if (iter2 == iter1->second.end()) {
            return -1;
        } else {
            return 0;
        }
    }

    inline void
    reach_cache :: insert_entry(size_t dest_node, size_t local_node, size_t req_id)
    {
        cache_table[dest_node].insert(local_node);
        invalidation_table[req_id] = dest_node;
    }

    inline bool
    reach_cache :: entry_exists(size_t dest_node, size_t local_node)
    {
        int val = get_pointer_to_value(dest_node, local_node);
        if (val == -1) {
            return false;
        } else {
            return true;
        }
    }

    inline void
    reach_cache :: remove_entry(size_t req_id)
    {
        size_t dest_node = invalidation_table[req_id];
        cache_table.erase(dest_node);
        invalidation_table.erase(req_id);
    }
}

#endif //__CACHE__
