/*
 * ===============================================================
 *    Description:  Graph element (edges and vertices)
 *
 *        Created:  Thursday 11 October 2012 11:15:20  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PUB_NODE__
#define __PUB_NODE__

#include <limits.h>
#include <stdint.h>
#include <vector>
#include <algorithm>
#include <string.h>

#include "common/weaver_constants.h"
#include "common/property.h"

namespace db
{
namespace element
{
    class pub_node : pub_element
    {
        public:
            pub_node();
            pub_node(uint64_t uid);
            pub_edge* get_next_edge();

        protected:
            uint32_t cur_edge_idx;
            bool valid_at_cur_time(pub_edge &e);
    };

    inline
    pub_node :: pub_node()
    {
    }

    inline
    pub_node :: pub_node(uint64_t time)
        : super(time)
    {
    }

    inline
    pub_node :: get_id()
    {
        return creat_time;
    }

    inline common::property*
    pub_node :: get_next_prop(common::property prop)
    {
        for (; cur_idx < properties.size(); cur_idx++){
            if (valid_at_cur_time(properties.at(cur_idx))){
                return &properties.at(cur_idx);
            }
        }
        return NULL;
    }


    inline bool 
    pub_node :: valid_at_cur_time(pub_edge &e)
        {
            return (e.get_creat_time() <= cur_request_time) && (p.get_del_time > cur_request_time);
        }
}

#endif //__PUB_NODE__
