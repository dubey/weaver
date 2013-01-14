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
            std::unordered_map<po6::net::location, std::unordered_map<void*, bool>> table;

        public:
            void insert_entry(po6::net::location loc, void *mem_addr, bool val);
            bool entry_exists(po6::net::location loc, void *mem_addr);
            bool get_cached_value(po6::net::location loc, void *mem_addr);
        
        private:
            int get_pointer_to_value(po6::net::location loc, void *mem_addr);
    };

    inline int
    reach_cache :: get_pointer_to_value(po6::net::location loc, void *mem_addr)
    {
        std::unordered_map<po6::net::location, std::unordered_map<void*, bool>>::iterator iter1;
        iter1 = table.find(loc);
        if (iter1 == table.end())
        {
            return -1;
        }
        std::unordered_map<void*, bool>::iterator iter2;
        iter2 = iter1->second.find(mem_addr);
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
    reach_cache :: insert_entry(po6::net::location loc, void *mem_addr, bool val)
    {
        table[loc][mem_addr] = val;
    }

    inline bool
    reach_cache :: entry_exists(po6::net::location loc, void *mem_addr)
    {
        int val = get_pointer_to_value(loc, mem_addr);
        if (val == -1)
        {
            return false;
        } else {
            return true;
        }
    }

    inline bool
    reach_cache :: get_cached_value(po6::net::location loc, void *mem_addr)
    {
        int val = get_pointer_to_value(loc, mem_addr);
        assert(val != -1);
        return (val==1 ? true:false);
    }
}

#endif //__CACHE__
