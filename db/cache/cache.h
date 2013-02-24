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
    typedef std::unordered_map<size_t, std::pair<std::unique_ptr<std::unordered_set<size_t>>, size_t>> ctable;
    typedef std::unordered_map<size_t, std::pair<size_t, size_t>> itable;
    class reach_cache
    {
        public:
            reach_cache(int num_shards);

        public:
            // positive traversal information is stored in a hash map:
            // destination node -> set of local nodes which can reach it, along with req_id
            // there is one such hash map for each shard
            ctable **cache_table;
            ctable **transient_cache_table;
            // invalidation table is used to store a mapping from request id to
            // destination (loc, node(s)), so that appropriate entries can be removed
            // from the cache_table on cache invalidation
            itable invalidation_table;
            itable transient_invalidation_table;

        public:
            bool insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id);
            bool transient_insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id);
            size_t get_req_id(size_t dest_loc, size_t dest_node, size_t local_node);
            std::unique_ptr<std::unordered_set<size_t>> remove_entry(size_t req_id);
            std::unique_ptr<std::unordered_set<size_t>> remove_transient_entry(size_t req_id);
            void commit(size_t id);

        private:
            po6::threads::mutex cache_mutex;
                
        private:
            bool entry_exists(size_t dest_loc, size_t dest_node, size_t local_node, ctable **table);
            bool mapping_exists(size_t dest_loc, size_t dest_node, ctable **table);
            bool insert_into_table(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id, ctable **c_table, itable *i_table);
            std::unique_ptr<std::unordered_set<size_t>> remove_from_table(size_t req_id, ctable **c_table, itable *i_table);
    };

    inline
    reach_cache :: reach_cache(int num_shards)
    {
        int i;
        cache_table = (ctable **) malloc(num_shards * sizeof(ctable *));
        transient_cache_table = (ctable **)malloc(num_shards * sizeof(ctable *));
        for (i = 0; i < num_shards; i++)
        {
            cache_table[i] = new ctable();
            transient_cache_table[i] = new ctable();
        }
    }

    // caution: not protected by mutex
    inline bool
    reach_cache :: entry_exists(size_t dest_loc, size_t dest_node, size_t local_node, ctable **table)
    {
        ctable::iterator iter1;
        iter1 = table[dest_loc]->find(dest_node);
        if (iter1 == table[dest_loc]->end()) {
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
    reach_cache :: mapping_exists(size_t dest_loc, size_t dest_node, ctable **table)
    {
        ctable::iterator iter1;
        iter1 = table[dest_loc]->find(dest_node);
        return (iter1 != table[dest_loc]->end());
    }

    // return 0 if the entry is not in cache
    // otherwise return the req_id which caused it to be cached
    inline size_t
    reach_cache :: get_req_id(size_t dest_loc, size_t dest_node, size_t local_node)
    {
        size_t ret;
        cache_mutex.lock();
        if (!entry_exists(dest_loc, dest_node, local_node, cache_table)) {
            cache_mutex.unlock();
            return 0;
        } else {
            ret = (*cache_table[dest_loc])[dest_node].second;
            cache_mutex.unlock();
            return ret; 
        }
    }

    inline bool
    reach_cache :: insert_into_table(size_t dest_loc, size_t dest_node,
        size_t local_node, size_t req_id, ctable **c_table, itable *i_table)
    {
        cache_mutex.lock();
        if (!entry_exists(dest_loc, dest_node, local_node, c_table))
        {
            if (!mapping_exists(dest_loc, dest_node, c_table)) {
                (*c_table[dest_loc])[dest_node].first.reset(new std::unordered_set<size_t>());
            }
            (*c_table[dest_loc])[dest_node].first->insert(local_node);
            (*c_table[dest_loc])[dest_node].second = req_id;
            (*i_table)[req_id] = std::make_pair(dest_loc, dest_node);
            cache_mutex.unlock();
            return true;
        } else {
            cache_mutex.unlock();
            return false;
        }
    }

    inline bool
    reach_cache :: insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id)
    {
        return insert_into_table(dest_loc, dest_node, local_node, req_id, 
            cache_table, &invalidation_table);
    }

    inline bool
    reach_cache :: transient_insert_entry(size_t dest_loc, size_t dest_node, size_t local_node, size_t req_id)
    {
        return insert_into_table(dest_loc, dest_node, local_node, req_id,
            transient_cache_table, &transient_invalidation_table);
    }

    // return a set of local nodes that have cached this entry
    inline std::unique_ptr<std::unordered_set<size_t>>
    reach_cache :: remove_from_table(size_t req_id, ctable **c_table, itable *i_table)
    {
        itable::iterator iter;
        std::unique_ptr<std::unordered_set<size_t>> ret;
        cache_mutex.lock();
        iter = i_table->find(req_id);
        // checking if the entry has not already been deleted
        if (iter != i_table->end()) 
        {
            size_t dest_node = iter->second.second;
            size_t dest_loc = iter->second.first;
            ret = std::move((*c_table[dest_loc])[dest_node].first);
            (*c_table[dest_loc]).erase(dest_node);
            i_table->erase(req_id);
        }
        cache_mutex.unlock();
        return ret;
    }

    inline std::unique_ptr<std::unordered_set<size_t>>
    reach_cache :: remove_entry(size_t req_id)
    {
        return remove_from_table(req_id, cache_table, &invalidation_table);
    }

    inline std::unique_ptr<std::unordered_set<size_t>>
    reach_cache :: remove_transient_entry(size_t req_id)
    {
        return remove_from_table(req_id, transient_cache_table, &transient_invalidation_table);
    }

    inline void
    reach_cache :: commit(size_t id)
    {
        itable::iterator it;
        cache_mutex.lock();
        it = transient_invalidation_table.find(id);
        if (it != transient_invalidation_table.end()) {
            // removing from transient cache
            size_t dest_node = it->second.second;
            size_t dest_loc = it->second.first;
            std::unique_ptr<std::unordered_set<size_t>> lnodes =
            std::move((*transient_cache_table[dest_loc])[dest_node].first);
            (*transient_cache_table[dest_loc]).erase(dest_node);
            transient_invalidation_table.erase(id);
            // inserting into cache
            (*cache_table[dest_loc])[dest_node].first = std::move(lnodes);
            (*cache_table[dest_loc])[dest_node].second = id;
            invalidation_table[id] = std::make_pair(dest_loc, dest_node);
        }
        cache_mutex.unlock();
    }
}

#endif //__CACHE__
