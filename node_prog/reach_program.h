#ifndef __DIJKSTRA_PROG__
#define __DIJKSTRA_PROG__

#include <vector>

#include "element/node.h"
#include "element/remote_node.h"
#include "common/message.h"
#include "node_prog_type.h"
#include "node_program.h"
#include "db/element/remote_node.h"

namespace db
{
    class reach_params : public virtual Packable 
    {
        public:
            db::element::remote_node prev_node;
            db::element::remote_node dest;
            std::vector<common::property> edge_props;

        public:
            virtual size_t size() const 
            {
                size_t toRet = message::size(prev_node) + message::size(dest) + message::size(edge_props);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, dest);
                message::pack_buffer(packer, edge_props);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, dest);
                message::unpack_buffer(unpacker, edge_props);
            }
    };

    struct reach_node_state : Deletable 
    {
        db::remote_node prev_node; // previous node
        uint64_t prev_id; // previous node's local request id
        uint32_t out_count; // number of requests propagated

        virtual ~reach_node_state()
        {
        }
    };

    struct reach_cache_value : Deletable 
    {
        int dummy;

        virtual ~reach_cache_value()
        {
        }
    };

    std::vector<std::pair<element::remote_node, reach_params>> 
    reach_node_program(element::node &n,
            reach_params &params,
            reach_node_state &state,
            reach_cache_value &cache)
    {
        std::cout << "Reachability program\n" << std::cout;
        std::vector<std::pair<element::remote_node, reach_params>> next;
        for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
            next.emplace_back(std::make_pair(possible_nbr.second->nbr, params));
        }
        return next;
    }
}

#endif //__DIKJSTRA_PROG__
