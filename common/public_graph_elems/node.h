/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PUB_NODE__
#define __PUB_NODE__

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "edge.h"
#include "property.h"
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/property.h"

namespace common
{
    class edge_iter : public std::iterator<std::input_iterator_tag, edge>
    {
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_cur;
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;

        public:
        edge_iter& operator++() {
            while (internal_cur != internal_end) {
                internal_cur++;
                if (internal_cur != internal_end && order::clock_creat_before_del_after(*req_time,
                            internal_cur->second->get_creat_time(), internal_cur->second->get_del_time())) {
                    break;
                }
            }
            return *this;
        }

        edge_iter(std::unordered_map<uint64_t, db::element::edge*>::iterator begin,
                std::unordered_map<uint64_t, db::element::edge*>::iterator end, std::shared_ptr<vc::vclock>& req_time)
            : internal_cur(begin), internal_end(end), req_time(req_time)
        {
            if (internal_cur != internal_end && order::clock_creat_before_del_after(*req_time,
                        internal_cur->second->get_creat_time(), internal_cur->second->get_del_time())) {
                ++(*this);
            }
        }

        //edge_iter operator++(int) {edge_iter tmp(*this); operator++(); return tmp;}
        bool operator==(const edge_iter& rhs) {return internal_cur == rhs.internal_cur && req_time == rhs.req_time;} // TODO == for req time?
        bool operator!=(const edge_iter& rhs) {return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time);} // TODO == for req time?
        edge& operator*()
        {
            db::element::edge& toRet = *internal_cur->second;
            toRet.view_time = req_time;
            return (edge &) toRet;
        }
    };

    class edge_list
    {
        private:
            std::unordered_map<uint64_t, db::element::edge*>& wrapped;
        std::shared_ptr<vc::vclock> req_time;

        public:
        edge_list(std::unordered_map<uint64_t, db::element::edge*>& edge_list, std::shared_ptr<vc::vclock>& req_time)
            : wrapped(edge_list), req_time(req_time) {}

        edge_iter begin () //const
        {
            return edge_iter(wrapped.begin(), wrapped.end(), req_time);
        }

        edge_iter end () //const
        {
            return edge_iter(wrapped.end(), wrapped.end(), req_time);
        }
    };

    class node : private db::element::node
    {
        private:
            using db::element::node::out_edges;
            using db::element::node::properties;
            using db::element::node::view_time;
        public:
            using db::element::node::get_handle;
            edge_list get_edges(){assert(view_time != NULL); return edge_list(out_edges, view_time);};
            prop_list get_properties(){assert(view_time != NULL); return prop_list(properties, *view_time);};
    };
}

#endif
