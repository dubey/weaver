/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PUB_EDGE__
#define __PUB_EDGE__

#include <stdint.h>
#include <vector>
#include <po6/net/location.h>

#include "property.h"
#include "node_ptr.h"
#include "common/vclock.h"
#include "db/element/property.h"
#include "db/element/edge.h"

namespace common
{
    class prop_iter : public std::iterator<std::input_iterator_tag, const property>
    {
        private:
            std::vector<db::element::property>::const_iterator cur;
            std::vector<db::element::property>::const_iterator end;
            vc::vclock& req_time;

        public:
        prop_iter(const std::vector<db::element::property>* prop_list, vc::vclock& req_time)
            : cur(prop_list->begin()), end(prop_list->end()), req_time(req_time) {}
        
        prop_iter& operator++() {
            while (cur != end) {
                cur++;
                if (cur != end && !order::clock_creat_before_del_after(req_time,
                            cur->get_creat_time(), cur->get_del_time())) {

                    break;
                }
            }
            return *this;
        }

        bool operator==(const prop_iter& rhs) {return cur==rhs.cur && req_time == rhs.req_time;} // TODO == for req time?
        bool operator!=(const prop_iter& rhs) {return cur!=rhs.cur || !(req_time == rhs.req_time);} // TODO == for req time?
        property& operator*() {return (property &) *cur;}
    };

    class edge 
    {
        private:
        db::element::edge& base; 
        vc::vclock& req_time;

        public:
        edge(db::element::edge& base, vc::vclock& time);

        node_ptr& get_neighbor(){ return (node_ptr&) base.nbr;};
        prop_iter get_prop_iter(){ return prop_iter(base.get_props(), req_time);};
    };
}

#endif
