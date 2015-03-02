/*
 * ===============================================================
 *    Description:  Node prog edge list declaration.
 *
 *        Created:  2014-05-29 18:48:08
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_list_h_
#define weaver_node_prog_edge_list_h_

#include <stdint.h>
#include <iterator>
#include <unordered_map>

#include "db/edge.h"
#include "common/event_order.h"
#include "node_prog/edge.h"

namespace node_prog
{
    typedef std::unordered_map<edge_handle_t, std::vector<db::edge*>> edge_map_t;
    class edge_map_iter : public std::iterator<std::input_iterator_tag, edge>
    {
        db::edge *cur_edge;
        edge_map_t::iterator internal_cur;
        edge_map_t::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;
        order::oracle *time_oracle;

        public:
            edge_map_iter& operator++();
            edge_map_iter(edge_map_t::iterator begin,
                edge_map_t::iterator end,
                std::shared_ptr<vc::vclock> &req_time,
                order::oracle *time_oracle);
            bool operator==(const edge_map_iter& rhs);
            bool operator!=(const edge_map_iter& rhs);
            edge& operator*();
    };

    class edge_list
    {
        private:
            edge_map_t &wrapped;
            std::shared_ptr<vc::vclock> &req_time;
            order::oracle *time_oracle;

        public:
            edge_list(edge_map_t &edge_list,
                std::shared_ptr<vc::vclock> &req_time,
                order::oracle *time_oracle);
            edge_map_iter begin();
            edge_map_iter end();
            uint64_t count();
    };
}

#endif
