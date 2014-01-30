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

                    if (internal_cur != internal_end)    WDEBUG << "checking property " << internal_cur->key << " in filtering iter2" << std::endl;

                    if (internal_cur != internal_end && order::clock_creat_before_del_after(req_time,
                                internal_cur->get_creat_time(), internal_cur->get_del_time())) {
                        WDEBUG << "ready to pass property " << internal_cur->key << " in filtering iter2" << std::endl;

                        break;
                    }
                }
                return *this;
            }

            prop_iter(std::vector<db::element::property>::iterator begin,
                    std::vector<db::element::property>::iterator end, vc::vclock& req_time)
                : internal_cur(begin), internal_end(end), req_time(req_time)
            {
               if (internal_cur != internal_end)  WDEBUG << "checking property " << internal_cur->key << " in filtering iter1" << std::endl;

               if (internal_cur != internal_end && !order::clock_creat_before_del_after(req_time,
                            internal_cur->get_creat_time(), internal_cur->get_del_time())) {
                    ++(*this);
                }
            }

            //bool operator==(const prop_iter& rhs) {return internal_cur == rhs.internal_cur && req_time == rhs.req_time;} // TODO == for req time?
            bool operator!=(const prop_iter& rhs) const {return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time);} // TODO == for req time?

            property& operator*() {return (property &) *internal_cur;} // XXX change to static cast?
    };

    class prop_list
    {
        private:
            std::vector<db::element::property>& wrapped;
            vc::vclock& req_time;

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
        private:
            using db::element::edge::nbr;
            using db::element::edge::properties;
            using db::element::edge::view_time;
            using db::element::edge::get_handle;

        public:
            uint64_t get_handle(){ return db::element::edge::get_handle();};
            node_ptr& get_neighbor(){ return (node_ptr&) nbr;};
            prop_list get_properties(){assert(view_time != NULL); return prop_list(properties, *view_time);};
            //prop_iter get_prop_iter(){assert(view_time != NULL); return prop_iter(properties, *view_time);};
    };
}

#endif
