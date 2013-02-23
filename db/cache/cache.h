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
            reach_cache(int num_shards);

        public:
            // positive traversal information is stored in a hash map:
            // destination node -> set of local nodes which can reach it, along with req_id
            // there is one such hash map for each shard
            std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>> **cache_table;
            // invalidation table is used to store a mapping from request id to
            // destination (loc, node(s)), so that appropriate entries can be removed
            // from the cache_table on cache invalidation
            std::unordered_map<size_t, std::pair<size_t, size_t>> invalidation_table;

        public:
            bool insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id);
            size_t get_req_id(size_t dest_loc, size_t dest_node, size_t local_node);
            std::unique_ptr<std::unordered_set<size_t>> remove_entry(size_t req_id);

        private:
            po6::threads::mutex cache_mutex;
                
        private:
            bool entry_exists(size_t dest_loc, size_t dest_node, size_t local_node);
            bool mapping_exists(size_t dest_loc, size_t dest_node);
    };

    inline
    reach_cache :: reach_cache(int num_shards)
    {
        int i;
        cache_table = (std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>> **)
            malloc(num_shards * sizeof(std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>> *));
        for (i = 0; i < num_shards; i++)
        {
            cache_table[i] = new std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>>();
        }
    }

    // caution: not protected by mutex
    inline bool
    reach_cache :: entry_exists(size_t dest_loc, size_t dest_node, size_t local_node)
    {
        std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>>::iterator iter1;
        iter1 = cache_table[dest_loc]->find(dest_node);
        if (iter1 == cache_table[dest_loc]->end()) {
            return false;
        }
        std::unordered_set<size_t>::iterator iter2;
        iter2 = iter1->second.first->find(local_node); // iter1->second.first is the unordered set
        if (iter2 == iter1->second.first->end()) {
            return false;
        } else {
            return true;
        }
    }

    // caution: not protected by mutex
    inline bool
    reach_cache :: mapping_exists(size_t dest_loc, size_t dest_node)
    {
        std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>>::iterator iter1;
        iter1 = cache_table[dest_loc]->find(dest_node);
        return (iter1 != cache_table[dest_loc]->end());
    }

    // return 0 if the entry is not in cache
    // otherwise return the req_id which caused it to be cached
    inline size_t
    reach_cache :: get_req_id(size_t dest_loc, size_t dest_node, size_t local_node)
    {
        size_t ret;
        cache_mutex.lock();
        if (!entry_exists(dest_loc, dest_node, local_node)) {
            cache_mutex.unlock();
            return 0;
        } else {
            ret = (*cache_table[dest_loc])[dest_node].second;
            cache_mutex.unlock();
            return ret; 
        }
    }

    inline bool
    reach_cache :: insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id)
    {
        cache_mutex.lock();
        if (!entry_exists(dest_loc, dest_node, local_node))
        {
            if (!mapping_exists(dest_loc, dest_node)) {
                (*cache_table[dest_loc])[dest_node].first.reset(new std::unordered_set<size_t>());
            }
            (*cache_table[dest_loc])[dest_node].first->insert(local_node);
            (*cache_table[dest_loc])[dest_node].second = req_id;
            invalidation_table[req_id] = std::make_pair(dest_loc, dest_node);
            cache_mutex.unlock();
            return true;
        } else {
            cache_mutex.unlock();
            return false;
        }
    }

    // return a set of local nodes that have cached this entry
    inline std::unique_ptr<std::unordered_set<size_t>>
    reach_cache :: remove_entry(size_t req_id)
    {
        std::unordered_map<size_t, std::pair<size_t, size_t>>::iterator iter;
        std::unique_ptr<std::unordered_set<size_t>> ret;
        cache_mutex.lock();
        iter = invalidation_table.find(req_id);
        // checking if the entry has not already been deleted
        if (iter != invalidation_table.end()) 
        {
            size_t dest_node = iter->second.second;
            size_t dest_loc = iter->second.first;
            ret = std::move((*cache_table[dest_loc])[dest_node].first);
            (*cache_table[dest_loc]).erase(dest_node);
            invalidation_table.erase(req_id);
        }
        cache_mutex.unlock();
        return ret;
    }
}

#endif //__CACHE__
