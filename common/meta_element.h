/*
 * ===============================================================
 *    Description:  Contains 'meta-information' about every graph 
 *                  element
 *
 *        Created:  10/29/2012 10:21:58 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __META_ELEMENT__
#define __META_ELEMENT__

#include <stdint.h>
#include <limits.h>
#include <po6/net/location.h>

#include "common/weaver_constants.h"

namespace common
{
    class meta_element
    {
        public:
            meta_element(std::shared_ptr<po6::net::location> server, 
                uint64_t t_creat, uint64_t t_delete, void *mem_addr);
        
        protected:
            std::shared_ptr<po6::net::location> myloc;
            uint64_t creat_time;
            uint64_t del_time;
            void *elem_addr; //memory address of this element on shard server
        
        public:
            uint64_t get_creat_time();
            uint64_t get_del_time();
            void update_del_time(uint64_t _del_time);
            void* get_addr();
            po6::net::location get_loc();
            std::shared_ptr<po6::net::location> get_loc_ptr();
            int get_shard_id();
    };

    inline
    meta_element :: meta_element(std::shared_ptr<po6::net::location> server, 
        uint64_t t_creat, uint64_t t_delete, void *mem_addr)
        : myloc(server)
        , creat_time(t_creat)
        , del_time(t_delete)
        , elem_addr(mem_addr)
    {
    }

    inline uint64_t
    meta_element :: get_creat_time()
    {
        return creat_time;
    }

    inline uint64_t
    meta_element :: get_del_time()
    {
        return del_time;
    }

    inline void
    meta_element :: update_del_time(uint64_t _del_time)
    {
        del_time = _del_time;
    }

    inline void*
    meta_element :: get_addr()
    {
        return elem_addr;
    }

    inline po6::net::location
    meta_element :: get_loc()
    {
        return *myloc;
    }

    inline std::shared_ptr<po6::net::location>
    meta_element :: get_loc_ptr()
    {
        return myloc;
    }

    // TODO this is a hack, until the system is running on a single maching
    // id is decided by which port the shard is running on
    inline int
    meta_element :: get_shard_id()
    {
        return (myloc->port - COORD_PORT - 1);
    }
}
#endif //__META_ELEMENT__
