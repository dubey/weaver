/*
 * ===============================================================
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __EDGE_LIST__
#define __EDGE_LIST__

#include <stdint.h>
#include <iostream>
#include <iterator>
#include <vector>
#include <unordered_map>

#include "node_prog/edge.h"
#include "db/element/edge.h"

namespace node_prog
{
    class map_edge_iter : public std::iterator<std::input_iterator_tag, edge>
    {
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_cur;
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;

        public:
        map_edge_iter& operator++()
        {
            while (internal_cur != internal_end) {
                internal_cur++;
                if (internal_cur != internal_end && order::clock_creat_before_del_after(*req_time,
                            internal_cur->second->base.get_creat_time(), internal_cur->second->base.get_del_time())) {
                    break;
                }
            }
            return *this;
        }

        map_edge_iter(std::unordered_map<uint64_t, db::element::edge*>::iterator begin,
                std::unordered_map<uint64_t, db::element::edge*>::iterator end, std::shared_ptr<vc::vclock> &req_time)
            : internal_cur(begin), internal_end(end), req_time(req_time)
        {
            if (internal_cur != internal_end && !order::clock_creat_before_del_after(*req_time,
                        internal_cur->second->base.get_creat_time(), internal_cur->second->base.get_del_time())) {
                ++(*this);
            }
        }

        bool operator==(const map_edge_iter& rhs) { return internal_cur == rhs.internal_cur && req_time == rhs.req_time; } // TODO == for req time?
        bool operator!=(const map_edge_iter& rhs) { return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time); } // TODO == for req time?

        edge& operator*()
        {
            db::element::edge& toRet = *internal_cur->second;
            toRet.base.view_time = req_time;
            return (edge &) toRet; // XXX problem here?
        }
    };

    // TODO take vclocks off this one
    class vector_edge_iter : public std::iterator<std::input_iterator_tag, edge>
    {
        std::vector<db::element::edge>::iterator internal_cur;
        std::vector<db::element::edge>::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;

        public:
        vector_edge_iter& operator++()
        {
            while (internal_cur != internal_end) {
                internal_cur++;
                if (internal_cur != internal_end && order::clock_creat_before_del_after(*req_time,
                            internal_cur->base.get_creat_time(), internal_cur->base.get_del_time())) {
                    break;
                }
            }
            return *this;
        }

        vector_edge_iter(std::vector<db::element::edge>::iterator begin,
                std::vector<db::element::edge>::iterator end, std::shared_ptr<vc::vclock> req_time)
            : internal_cur(begin), internal_end(end), req_time(req_time)
        {
            if (internal_cur != internal_end && !order::clock_creat_before_del_after(*req_time,
                        internal_cur->base.get_creat_time(), internal_cur->base.get_del_time())) {
                ++(*this);
            }
        }

        bool operator==(const vector_edge_iter& rhs) { return internal_cur == rhs.internal_cur && req_time == rhs.req_time; } // TODO == for req time?
        bool operator!=(const vector_edge_iter& rhs) { return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time); } // TODO == for req time?

        edge& operator*()
        {
            return (edge &) *internal_cur;
        }
    };

    template <typename T, typename iter_T>
        class edge_list
        {
            private:
                T& wrapped;
                std::shared_ptr<vc::vclock>& req_time;

            public:
                edge_list(T &edge_list, std::shared_ptr<vc::vclock> &req_time)
                    : wrapped(edge_list), req_time(req_time) {}

                iter_T begin () //const
                {
                    return iter_T(wrapped.begin(), wrapped.end(), req_time);
                }

                iter_T end () //const
                {
                    return iter_T(wrapped.end(), wrapped.end(), req_time);
                }

                // TODO could make an empty() function
        };
}

#endif
