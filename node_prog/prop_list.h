/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PROP_LIST__
#define __PROP_LIST__

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "node_prog/property.h"
#include "db/element/property.h"

namespace node_prog
{
    // TODO take off node_prog:: tags
    class prop_iter : public std::iterator<std::input_iterator_tag, node_prog::property>
    {
        private:
            std::vector<db::element::property>::iterator internal_cur;
            std::vector<db::element::property>::iterator internal_end;
            vc::vclock& req_time;

        public:
            prop_iter& operator++() {
                while (internal_cur != internal_end) {
                    internal_cur++;
                    if (internal_cur != internal_end && order::clock_creat_before_del_after(req_time,
                                internal_cur->get_creat_time(), internal_cur->get_del_time())) {
                        break;
                    }
                }
                return *this;
            }

            prop_iter(std::vector<db::element::property>::iterator begin,
                    std::vector<db::element::property>::iterator end, vc::vclock& req_time)
                : internal_cur(begin), internal_end(end), req_time(req_time)
            {

                if (internal_cur != internal_end && !order::clock_creat_before_del_after(req_time,
                            internal_cur->get_creat_time(), internal_cur->get_del_time())) {
                    ++(*this);
                }
            }

            bool operator!=(const prop_iter& rhs) const {return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time);} // TODO == for req time?

            node_prog::property& operator*() {return (node_prog::property &) *internal_cur;} // XXX change to static cast?
    };

    class prop_list
    {
        private:
            std::vector<db::element::property>& wrapped;
            vc::vclock &req_time;

        public:
            prop_list(std::vector<db::element::property>& prop_list, vc::vclock& req_time)
                : wrapped(prop_list), req_time(req_time) {}

            prop_iter begin () //const
            {
                return prop_iter(wrapped.begin(), wrapped.end(), req_time);
            }

            prop_iter end () //const
            {
                return prop_iter(wrapped.end(), wrapped.end(), req_time);
            }
    };
}

#endif
