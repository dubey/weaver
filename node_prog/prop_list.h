/*
 * ===============================================================
 *    Description:  Node prog prop list implementation.
 *
 *        Created:  2014-05-29 19:08:19
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Greg Hill, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_prop_list_h_
#define weaver_node_prog_prop_list_h_

#include <iterator>
#include <unordered_map>

#include "common/event_order.h"
#include "db/property.h"

namespace node_prog
{
    typedef std::unordered_map<std::string, db::element::property> prop_map_t;
    class prop_iter : public std::iterator<std::input_iterator_tag, property>
    {
        private:
            prop_map_t::iterator internal_cur;
            prop_map_t::iterator internal_end;
            vc::vclock &req_time;
            order::oracle *time_oracle;

        public:
            prop_iter& operator++();
            prop_iter(prop_map_t::iterator begin, prop_map_t::iterator end, vc::vclock& req_time, order::oracle *time_oracle);
            bool operator!=(const prop_iter& rhs) const;
            property& operator*();
    };

    class prop_list
    {
        private:
            prop_map_t &wrapped;
            vc::vclock &req_time;
            order::oracle *time_oracle;

        public:
            prop_list(prop_map_t &prop_list, vc::vclock &req_time, order::oracle *to)
                : wrapped(prop_list), req_time(req_time), time_oracle(to) { }

            prop_iter begin()
            {
                return prop_iter(wrapped.begin(), wrapped.end(), req_time, time_oracle);
            }

            prop_iter end()
            {
                return prop_iter(wrapped.end(), wrapped.end(), req_time, time_oracle);
            }
    };
}

#endif
