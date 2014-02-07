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
    // TODO take off node_prog:: tags
    class edge_iter : public std::iterator<std::input_iterator_tag, node_prog::edge>
    {
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_cur;
        std::unordered_map<uint64_t, db::element::edge*>::iterator internal_end;
        std::shared_ptr<vc::vclock> req_time;

        public:
        edge_iter& operator++()
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

        edge_iter(std::unordered_map<uint64_t, db::element::edge*>::iterator begin,
                std::unordered_map<uint64_t, db::element::edge*>::iterator end, std::shared_ptr<vc::vclock>& req_time)
            : internal_cur(begin), internal_end(end), req_time(req_time)
        {
            if (internal_cur != internal_end && !order::clock_creat_before_del_after(*req_time,
                        internal_cur->second->base.get_creat_time(), internal_cur->second->base.get_del_time())) {
                ++(*this);
            }
        }

        bool operator==(const edge_iter& rhs) { return internal_cur == rhs.internal_cur && req_time == rhs.req_time; } // TODO == for req time?
        bool operator!=(const edge_iter& rhs) { return internal_cur != rhs.internal_cur || !(req_time == rhs.req_time); } // TODO == for req time?

        edge& operator*()
        {
            db::element::edge& toRet = *internal_cur->second;
            toRet.base.view_time = req_time;
            return (edge &) toRet; // XXX problem here?
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
}

#endif
