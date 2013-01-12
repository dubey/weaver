/*
 * =====================================================================================
 *
 *       Filename:  cache.h
 *
 *    Description:  Cache for user query results
 *
 *        Version:  1.0
 *        Created:  12/04/2012 09:58:54 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

#ifndef __CACHE__
#define __CACHE__

#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <unordered_map>

namespace cache
{
    class reach_cache
    {
        public:
            std::unordered_map<uint16_t, std::unordered_map<void*, bool>> table;

        public:
            void insert_entry (uint16_t port, void *mem_addr, bool val);
            bool entry_exists (uint16_t port, void *mem_addr);
            bool get_cached_value (uint16_t port, void *mem_addr);
        
        private:
            int get_pointer_to_value (uint16_t port, void *mem_addr);
    };

    inline int
    reach_cache :: get_pointer_to_value (uint16_t port, void *mem_addr)
    {
        std::unordered_map<uint16_t, std::unordered_map<void*, bool>>::iterator iter1;
        iter1 = table.find (port);
        if (iter1 == table.end())
        {
            return -1;
        }
        std::unordered_map<void*, bool>::iterator iter2;
        iter2 = iter1->second.find (mem_addr);
        if (iter2 == iter1->second.end())
        {
            return -1;
        } else
        {
            return (iter2->second ? 1:0);
        }
    }

    /*
     * Insert val into reach cache
     * Will overwrite any old value associated with port, mem_addr
     */
    inline void
    reach_cache :: insert_entry (uint16_t port, void *mem_addr, bool val)
    {
        table[port][mem_addr] = val;
    }

    inline bool
    reach_cache :: entry_exists (uint16_t port, void *mem_addr)
    {
        int val = get_pointer_to_value (port, mem_addr);
        if (val == -1)
        {
            return false;
        } else
        {
            return true;
        }
    }

    inline bool
    reach_cache :: get_cached_value (uint16_t port, void *mem_addr)
    {
        int val = get_pointer_to_value (port, mem_addr);
        assert (val != -1);
        return (val==1 ? true:false);
    }
} 

#endif //__CACHE__
