/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_h_
#define weaver_node_prog_edge_h_

#include <stdint.h>
#include <vector>
#include <po6/net/location.h>

#include "db/element/remote_node.h"
#include "property.h"

namespace node_prog
{
    class prop_iter;
    class prop_list;

    class edge
    {
        public:
            virtual ~edge() { }
            virtual uint64_t get_id() const = 0;
            virtual void traverse() = 0;
            virtual db::element::remote_node& get_neighbor() = 0;
            virtual prop_list get_properties() = 0;
            virtual bool has_property(std::pair<std::string, std::string>& p) = 0;
            virtual bool has_all_properties(std::vector<std::pair<std::string, std::string>>& props) = 0;
   };
}

#endif
