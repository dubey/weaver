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
            meta_element(int loc, uint64_t t_creat, uint64_t t_delete, void *node_handle);
            meta_element(int loc, uint64_t t_creat, uint64_t t_delete, uint64_t edge_handle);
        
        protected:
            int myloc;
            uint64_t creat_time;
            uint64_t del_time;
            //unique handle of this element on shard server
            union {
                size_t node;
                uint64_t edge;
            } handle;
        
        public:
            uint64_t get_creat_time();
            uint64_t get_del_time();
            void update_del_time(uint64_t _del_time);
            void update_creat_time(uint64_t _creat_time);
            size_t get_node_handle();
            uint64_t get_edge_handle();
            void update_node_handle(size_t addr);
            void update_edge_handle(uint64_t handle);
            int get_loc();
            void update_loc(int newloc);
    };

    inline
    meta_element :: meta_element(int loc, uint64_t t_creat, uint64_t t_delete, void *node_handle)
        : myloc(loc)
        , creat_time(t_creat)
        , del_time(t_delete)
    {
        handle.node = (size_t)node_handle;
    }

    inline
    meta_element :: meta_element(int loc, uint64_t t_creat, uint64_t t_delete, uint64_t edge_handle)
        : myloc(loc)
        , creat_time(t_creat)
        , del_time(t_delete)
    {
        handle.edge = edge_handle;
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

    inline size_t
    meta_element :: get_node_handle()
    {
        return handle.node;
    }

    inline uint64_t
    meta_element :: get_edge_handle()
    {
        return handle.edge;
    }

    inline void
    meta_element :: update_node_handle(size_t addr)
    {
        handle.node = addr;
    }

    inline void
    meta_element :: update_edge_handle(uint64_t ehandle)
    {
        handle.edge = ehandle;
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
