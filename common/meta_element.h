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
            meta_element(int loc, uint64_t t_creat, uint64_t t_delete, uint64_t elem_handle);
            meta_element(int loc);
        
        protected:
            int myloc;
            uint64_t creat_time;
            uint64_t del_time;
            //unique handle of this element on shard server
            uint64_t handle;
        
        public:
            uint64_t get_creat_time();
            uint64_t get_del_time();
            void update_del_time(uint64_t _del_time);
            void update_creat_time(uint64_t _creat_time);
            uint64_t get_handle();
            void update_handle(uint64_t newhandle);
            int get_loc();
            void update_loc(int newloc);
    };

    inline
    meta_element :: meta_element(int loc)
        : myloc(loc)
    {
    }

    inline
    meta_element :: meta_element(int loc, uint64_t t_creat, uint64_t t_delete, uint64_t elem_handle)
        : myloc(loc)
        , creat_time(t_creat)
        , del_time(t_delete)
        , handle(elem_handle)
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

    inline void
    meta_element :: update_creat_time(uint64_t _creat_time)
    {
        creat_time = _creat_time;
    }

    inline uint64_t
    meta_element :: get_handle()
    {
        return handle;
    }

    inline void
    meta_element :: update_handle(uint64_t newhandle)
    {
        handle = newhandle;
    }

    inline int
    meta_element :: get_loc()
    {
        return myloc;
    }
    
    inline void
    meta_element :: update_loc(int newloc)
    {
        myloc = newloc;
    }
}
#endif //__META_ELEMENT__
