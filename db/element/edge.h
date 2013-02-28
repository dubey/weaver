/*
 * ===============================================================
 *    Description:  Graph edge class 
 *
 *        Created:  Tuesday 16 October 2012 02:28:29  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __EDGE__
#define __EDGE__

#include <stdint.h>
#include <vector>
#include <po6/net/location.h>

#include "common/meta_element.h"
#include "element.h"

namespace db
{
namespace element
{
    class edge : public element
    {
        public:
            edge(uint64_t time, std::unique_ptr<common::meta_element> _nbr);
            edge(std::shared_ptr<po6::net::location> server, uint64_t time, 
                std::unique_ptr<common::meta_element> _nbr);
        
        public:
            std::unique_ptr<common::meta_element> nbr; // out-neighbor for this edge
    };

    inline
    edge :: edge(std::shared_ptr<po6::net::location> server, uint64_t time,
        std::unique_ptr<common::meta_element> _nbr)
        : element(server, time, (void*)this)
        , nbr(std::move(_nbr))
    {
    }

    inline
    edge :: edge(uint64_t time, std::unique_ptr<common::meta_element> _nbr)
        : element(time, (void*)this)
        , nbr(std::move(_nbr))
    {
    }
}
}

#endif //__NODE__
