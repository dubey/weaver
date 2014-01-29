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
        std::unordered_map<uint64_t, db::element::edge*>::iterator cur;
        std::unordered_map<uint64_t, db::element::edge*>::iterator end;
        vc::vclock& req_time;

        public:
        edge_iter(std::unordered_map<uint64_t, db::element::edge*>& edges, vc::vclock& req_time) : cur(edges.begin()), end(edges.end()), req_time(req_time) {}
        //edge_iter(const MyIterator& mit) : p(mit.p) {}
        
        edge_iter& operator++() {
            while (cur != end) {
                cur++;
                /* XXX
                if (cur != end && !order::clock_creat_before_del_after(req_time,
                            cur->second->get_creat_time(), cur->second->get_del_time())) {
                    break;
                }
                */
            }
            return *this;
        }

        //edge_iter operator++(int) {edge_iter tmp(*this); operator++(); return tmp;}
        bool operator==(const edge_iter& rhs) {return cur==rhs.cur && req_time == rhs.req_time;} // TODO == for req time?
        bool operator!=(const edge_iter& rhs) {return cur!=rhs.cur || !(req_time == rhs.req_time);} // TODO == for req time?
        edge&& operator*() {return edge(*cur->second, req_time);}
    };

    class node
    {
        private:
        db::element::node& base;
        vc::vclock& req_time;

        public:
        node(db::element::node& base, vc::vclock& time);

        uint64_t get_handle() { return base.get_handle();};
        edge_iter get_edge_iter(){ return edge_iter(base.out_edges, req_time);};
        prop_iter get_prop_iter(){ return prop_iter(base.get_props(), req_time);};
    };
}

#endif
