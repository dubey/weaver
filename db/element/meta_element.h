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

namespace db
{
namespace element
{
    class meta_element
    {
        public:
            meta_element(po6::net::location server, uint32_t t_update, 
                uint32_t t_delete, void *mem_addr);
        
        protected:
            po6::net::location myloc;
            uint32_t t_u;
            uint32_t t_d;
            void *elem_addr; //memory address of this element on shard server
        
        public:
            uint32_t get_t_u();
            void* get_addr();
            po6::net::location get_loc();
    };

    inline
    meta_element :: meta_element(po6::net::location server, uint32_t t_update,
        uint32_t t_delete, void *mem_addr)
        : myloc(server)
        , t_u(t_update)
        , t_d(t_delete)
        , elem_addr(mem_addr)
    {
    }

    inline uint32_t
    meta_element :: get_t_u()
    {
        return t_u;
    }

    inline void*
    meta_element :: get_addr()
    {
        return elem_addr;
    }

    inline po6::net::location
    meta_element :: get_loc()
    {
        return myloc;
    }
}
}

#endif //__META_ELEMENT__
