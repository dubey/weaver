#ifndef __DIJKSTRA_PROG__
#define __DIJKSTRA_PROG__

#include <vector>

#include "element/node.h"
#include "element/remote_node.h"
#include "common/message.h"
#include "db/node_prog_type.h"
#include "db/node_program.h"
#include "db/element/remote_node.h"

namespace db
{
    class dijkstra_params : public virtual Packable 
    {
        public:
            db::element::remote_node source;
            db::element::remote_node dest;
            uint32_t edge_weight_name; // the key of the property which holds the weight of an an edge
            std::vector<common::property> edge_props;
            bool is_widest_path;
            std::pair<uint64_t, uint64_t> tentative_map_value; // for returning from a prop

        public:
            virtual size_t size() const 
            {
                size_t toRet = message::size(source) + message::size(dest);
                toRet += message::size(edge_weight_name) + message::size(edge_props);
                toRet += message::size(is_widest_path) + message::size(tentative_map_value);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, source);
                message::pack_buffer(packer, dest);
                message::pack_buffer(packer, edge_weight_name);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, is_widest_path);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, source);
                message::unpack_buffer(unpacker, dest);
                message::unpack_buffer(unpacker, edge_weight_name);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, is_widest_path);
            }
    };

    struct dijkstra_node_state : Deletable 
    {
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::less<dijkstra_queue_elem>> next_nodes_shortest; 
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::greater<dijkstra_queue_elem>> next_nodes_widest; 
        // map from a node (by its create time) to its cost and the req_id of the node that came before it in the shortest path
        std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> visited_map; 

        virtual ~dijkstra_node_state()
        {
            /* implement me? XXX */
        }
    };

    struct dijkstra_cache_value : Deletable 
    {
        int dummy;

        virtual ~dijkstra_cache_value()
        {
            /* implement me? XXX */
        }
    };

    std::vector<std::pair<element::remote_node, dijkstra_params>> 
    dijkstra_node_program(element::node &n,
            dijkstra_params &params,
            dijkstra_node_state &state,
            dijkstra_cache_value &cache)
    {
        std::cout << "OMG ITS RUNNING THE NODE PROGRAM" << std::cout;
        std::vector<std::pair<element::remote_node, dijkstra_params>> next;
        for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
            next.emplace_back(std::make_pair(possible_nbr.second->nbr, params));
        }
        return next;
    }
}

#endif //__DIKJSTRA_PROG__
