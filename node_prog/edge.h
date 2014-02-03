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
#include "node_handle.h"
#include "common/vclock.h"
#include "db/element/property.h"
#include "db/element/edge.h"

namespace node_prog
{
    class prop_iter : public std::iterator<std::input_iterator_tag, property>
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

            property& operator*() {return (property &) *internal_cur;} // XXX change to static cast?
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


    class edge : private db::element::edge
    {
        public:
            uint64_t get_id()
            {
                return db::element::edge::get_id();
            };

            node_handle& get_neighbor()
            {
                return (node_handle&) nbr;
            };

            prop_list get_properties()
            {
                assert(view_time != NULL);
                return prop_list(properties, *view_time);
            };

            bool has_property(property& p)
            {
                assert(view_time != NULL);
                return db::element::edge::has_property((db::element::property &) p, *view_time);
            };

            bool has_all_properties(std::vector<property>& props)
            {
                assert(view_time != NULL);
                for (auto &p : props) {
                    if (!db::element::edge::has_property((db::element::property &) p, *view_time)) {
                        return false;
                    }
                }
                return true;
            };
    };
}

#endif
