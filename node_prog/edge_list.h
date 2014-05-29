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
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "node_prog/edge.h"
#include "db/element/edge.h"

namespace node_prog
{
    typedef std::unordered_map<uint64_t, db::element::edge*> edge_map_t;
    class edge_map_iter : public std::iterator<std::input_iterator_tag, edge>
    {
        edge_map_t::iterator internal_cur;
        edge_map_t::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;

        public:
            edge_map_iter& operator++();
            edge_map_iter(edge_map_t::iterator begin, edge_map_t::iterator end,
                std::shared_ptr<vc::vclock> &req_time);
            bool operator==(const edge_map_iter& rhs);
            bool operator!=(const edge_map_iter& rhs);
            edge& operator*();
    };

    class edge_list
    {
        private:
            edge_map_t &wrapped;
            std::shared_ptr<vc::vclock> &req_time;

        public:
            edge_list(edge_map_t &edge_list, std::shared_ptr<vc::vclock> &req_time)
                : wrapped(edge_list), req_time(req_time) { }

            edge_map_iter begin()
            {
                return edge_map_iter(wrapped.begin(), wrapped.end(), req_time);
            }

            edge_map_iter end()
            {
                return edge_map_iter(wrapped.end(), wrapped.end(), req_time);
            }

            uint64_t count() { return wrapped.size(); }
    };
}

#endif
