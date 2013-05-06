/*
 * ===============================================================
 *    Description:  Dijkstra shortest path program.
 *
 *        Created:  Sunday 21 April 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __DIJKSTRA_PROG__
#define __DIJKSTRA_PROG__

#include <vector>

#include "common/message.h"
#include "db/element/node.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    class dijkstra_params : public virtual Packable 
    {
        public:
            uint64_t source_handle;
            uint64_t dest_handle;
            uint32_t edge_weight_name; // the key of the property which holds the weight of an an edge
            std::vector<common::property> edge_props;
            bool is_widest_path;
            std::pair<uint64_t, uint64_t> tentative_map_value; // for returning from a prop

        public:
            virtual size_t size() const 
            {
                size_t toRet = message::size(source_handle) + message::size(dest_handle);
                toRet += message::size(edge_weight_name) + message::size(edge_props);
                toRet += message::size(is_widest_path) + message::size(tentative_map_value);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, source_handle);
                message::pack_buffer(packer, dest_handle);
                message::pack_buffer(packer, edge_weight_name);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, is_widest_path);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, source_handle);
                message::unpack_buffer(unpacker, dest_handle);
                message::unpack_buffer(unpacker, edge_weight_name);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, is_widest_path);
            }
    };

    struct dijkstra_node_state : Deletable 
    {
        //std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::less<dijkstra_queue_elem>> next_nodes_shortest; 
        //std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::greater<dijkstra_queue_elem>> next_nodes_widest; 
        // map from a node (by its create time) to its cost and the req_id of the node that came before it in the shortest path
        std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> visited_map; 

        virtual ~dijkstra_node_state()
        {
            /* implement me? XXX */
        }
    };

    struct dijkstra_cache_value : CacheValueBase 
    {
        int dummy;

        virtual ~dijkstra_cache_value()
        {
            /* implement me? XXX */
        }
    };

    std::vector<std::pair<db::element::remote_node, dijkstra_params>> 
    dijkstra_node_program(uint64_t req_id,
            db::element::node &n,
            db::element::remote_node &rn,
            dijkstra_params &params,
            std::function<dijkstra_node_state&()> state_getter,
            std::function<dijkstra_cache_value&()> cache_value_putter,
            std::function<std::vector<dijkstra_cache_value *>()> cached_values_getter)
    {
        std::vector<std::pair<db::element::remote_node, dijkstra_params>> next;
        if (params.edge_weight_name == 88){
            std::cout << "YERRRRRRRRRRRRRr" << std::endl;
        }
        if (n.get_creat_time() == params.dest_handle){
            params.edge_weight_name = 42;
            std::cout << "FOUND DEST, RETURNING" << std::endl;
            next.emplace_back(std::make_pair(db::element::remote_node(-1, 1337), params));
            return next;
        }
        for (std::pair<const uint64_t, db::element::edge*> &possible_nbr : n.out_edges) {
            next.emplace_back(std::make_pair(possible_nbr.second->nbr, params));
        }
        std::cout << "OMG ITS RUNNING THE NODE PROGRAM " << next.size()<< std::endl;
        return next;
    }

    std::vector<std::pair<db::element::remote_node, dijkstra_params>> 
    dijkstra_node_deleted_program(uint64_t req_id,
                db::element::node &n, // node who asked to go to deleted node
                uint64_t deleted_handle, // handle of node that didn't exist
            dijkstra_params &params_given, // params we had sent to deleted node
            std::function<dijkstra_node_state&()> state_getter){
        std::cout << "DELETED PROGRAM " <<  std::endl;
        return std::vector<std::pair<db::element::remote_node, dijkstra_params>>(); 
    }
}

#endif //__DIKJSTRA_PROG__
