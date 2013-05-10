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
    class dijkstra_queue_elem
    {
            public:
            size_t cost;
            db::element::remote_node node;
            uint64_t prev_node_req_id; // used for reconstructing path in coordinator

            int operator<(const dijkstra_queue_elem& other) const
            { 
                return other.cost < cost; 
            }

            int operator>(const dijkstra_queue_elem& other) const
            { 
                return other.cost > cost; 
            }

            dijkstra_queue_elem()
            {
            }

            dijkstra_queue_elem(size_t c, db::element::remote_node n, size_t prev)
            {
                cost = c;
                node = n;
                prev_node_req_id = prev;
            }
    };

    class dijkstra_params : public virtual Packable 
    {
        public:
            uint64_t source_handle;
            db::element::remote_node source_node;
            uint64_t dest_handle;
            uint32_t edge_weight_name; // the key of the property which holds the weight of an an edge
            std::vector<common::property> edge_props;
            bool is_widest_path;
            bool adding_nodes;
            uint64_t prev_node_id;
            std::vector<std::pair<uint64_t, db::element::remote_node>> entries_to_add;
            uint64_t next_node_id;
            std::vector<std::pair<uint64_t, uint64_t>> final_path;
            uint64_t cost;


        public:
            virtual size_t size() const 
            {
                size_t toRet = 0;
                toRet += message::size(source_handle);
                toRet += message::size(source_node);
                toRet += message::size(dest_handle);
                toRet += message::size(edge_weight_name);
                toRet += message::size(edge_props);
                toRet += message::size(is_widest_path);
                toRet += message::size(adding_nodes);
                toRet += message::size(prev_node_id);
                toRet += message::size(entries_to_add);
                toRet += message::size(next_node_id);
                toRet += message::size(final_path);
                toRet += message::size(cost);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, source_handle);
                message::pack_buffer(packer, source_node);
                message::pack_buffer(packer, dest_handle);
                message::pack_buffer(packer, edge_weight_name);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, is_widest_path);
                message::pack_buffer(packer, adding_nodes);
                message::pack_buffer(packer, prev_node_id);
                message::pack_buffer(packer, entries_to_add);
                message::pack_buffer(packer, next_node_id);
                message::pack_buffer(packer, final_path);
                message::pack_buffer(packer, cost);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, source_handle);
                message::unpack_buffer(unpacker, source_node);
                message::unpack_buffer(unpacker, dest_handle);
                message::unpack_buffer(unpacker, edge_weight_name);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, is_widest_path);
                message::unpack_buffer(unpacker, adding_nodes);
                message::unpack_buffer(unpacker, prev_node_id);
                message::unpack_buffer(unpacker, entries_to_add);
                message::unpack_buffer(unpacker, next_node_id);
                message::unpack_buffer(unpacker, final_path);
                message::unpack_buffer(unpacker, cost);
            }
    };

    struct dijkstra_node_state : Deletable 
    {
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::less<dijkstra_queue_elem>> next_nodes_shortest; 
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::greater<dijkstra_queue_elem>> next_nodes_widest; 
        // map from a node (by its create time) to the req_id of the node that came before it in the shortest path and its cost
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

    // caution: assuming we hold n->update_mutex
    template<typename Func>
    void apply_to_valid_edges(db::element::node* n, std::vector<common::property>& edge_props, size_t req_id, Func func){
            // check the properties of each out-edge, assumes lock for node is held
            for (const std::pair<size_t, db::element::edge*> &e : n->out_edges)
            {
                bool use_edge = e.second->get_creat_time() <= req_id  
                    && e.second->get_del_time() > req_id; // edge created and deleted in acceptable timeframe

                for (size_t i = 0; i < edge_props.size() && use_edge; i++) // checking edge properties
                {
                    if (!e.second->has_property(edge_props[i])) {
                        use_edge = false;
                        break;
                    }
                }
                if (use_edge) {
                    func(e.second);
                }
            }
        }

    inline size_t
        calculate_priority(size_t current_cost, size_t edge_cost, bool is_widest_path){
            size_t priority;
            if (is_widest_path){
                priority = current_cost < edge_cost ? current_cost : edge_cost;
            } else {
                priority = edge_cost + current_cost;
            }
            return priority;
        }

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

        if (n.get_creat_time() == params.source_handle){
            dijkstra_node_state& node_state = state_getter();
            std::cout << "at source! " <<  std::endl;
            if (params.adding_nodes == true){ // response from a propagation, add nodes it could potentially reach to priority queue
                if (params.is_widest_path) {
                    for (auto &elem : params.entries_to_add) {
                        node_state.next_nodes_widest.emplace(elem.first, elem.second, params.next_node_id);
                    }
                } else {
                    for (auto &elem : params.entries_to_add) {
                        node_state.next_nodes_shortest.emplace(elem.first, elem.second, params.next_node_id);
                    }
                }
                params.entries_to_add.clear();
                node_state.visited_map.emplace(params.next_node_id, std::make_pair(params.prev_node_id, params.cost));
                std::cout << "adding node " << params.next_node_id << " to visited map with previous nbr " << params.prev_node_id << " and cost " << params.cost << std::endl;
            } else { 
                if (node_state.visited_map.count(params.source_handle) > 0){ // response from a deleted node
                    params.entries_to_add.clear();
                } else { // starting the request, add source neighbors to priority queue
                    params.source_node = rn;
                    std::cout << "first time adding source neighbors to PQ source node handle: "<< params.source_node.handle <<  std::endl;
                    params.cost = params.is_widest_path ? MAX_TIME : 0; // don't want source node to be bottleneck in path
                    node_state.visited_map.emplace(params.source_handle, std::make_pair(params.source_handle, params.cost));
                    for (const std::pair<size_t, db::element::edge*> &e : n.out_edges)
                    {
                        bool use_edge = e.second->get_creat_time() <= req_id  
                            && e.second->get_del_time() > req_id; // edge created and deleted in acceptable timeframe

                        for (size_t i = 0; i < params.edge_props.size() && use_edge; i++) // checking edge properties
                        {
                            if (!e.second->has_property(params.edge_props[i])) {
                                use_edge = false;
                                break; }
                        }
                        if (use_edge) {
                            // first is whether key exists, second is value
                            std::pair<bool, size_t> weightpair = e.second->get_property_value(params.edge_weight_name, req_id);
                            if (weightpair.first) {
                                size_t priority = calculate_priority(params.cost, weightpair.second, params.is_widest_path);
                                if (params.is_widest_path){
                                    node_state.next_nodes_widest.emplace(priority, e.second->nbr, params.source_handle); 
                                } else {
                                    node_state.next_nodes_shortest.emplace(priority, e.second->nbr, params.source_handle);
                                }
                            }
                        }
                    }
                }
                params.adding_nodes = true;
            }
            // select which node to visit next based on priority queue
            dijkstra_queue_elem next_to_add; //XXX change to reference, maybe need const

            while (!node_state.next_nodes_shortest.empty() || !node_state.next_nodes_widest.empty()) {
                //std::cout << "in while looop " <<  std::endl;
                if (params.is_widest_path) {
                    next_to_add = node_state.next_nodes_widest.top();
                    node_state.next_nodes_widest.pop();
                } else {
                    //std::cout << "popped a node " <<  std::endl;
                    next_to_add = node_state.next_nodes_shortest.top();
                    node_state.next_nodes_shortest.pop();
                }
                params.cost = next_to_add.cost;
                params.next_node_id = next_to_add.node.handle;
                params.prev_node_id = next_to_add.prev_node_req_id;
                // we have found destination! We know it was not deleted as coord checked
                if (params.next_node_id == params.dest_handle) {
                    std::cout << "found dest from prev " << params.prev_node_id<<  std::endl;
                    // rebuild path based on req_id's in visited_map
                    std::pair<uint64_t, uint64_t> visited_entry;

                    if (params.is_widest_path){
                        uint64_t cur_node = params.dest_handle;
                        uint64_t cur_cost = params.cost;
                        params.final_path.push_back(std::make_pair(cur_node, cur_cost));
                        cur_node = params.prev_node_id;
                        visited_entry = node_state.visited_map[params.prev_node_id];
                        while (cur_node != params.source_handle){
                            cur_cost = visited_entry.second;
                            params.final_path.push_back(std::make_pair(cur_node, cur_cost));
                            cur_node = visited_entry.first;
                            visited_entry = node_state.visited_map[cur_node];
                            }
                        } else { // shortest path, have to calculate edge weight based on cumulative cost to node before
                        uint64_t old_cost = params.cost;
                        uint64_t old_node = params.dest_handle; // the node father from sourc
                        uint64_t cur_node = params.prev_node_id;
                        while (old_node != params.source_handle){
                            visited_entry = node_state.visited_map[cur_node];
                            params.final_path.push_back(std::make_pair(old_node, old_cost-visited_entry.second));
                            old_node = cur_node;
                            old_cost = visited_entry.second;
                            cur_node = visited_entry.first;
                        }
                    }
                    next.emplace_back(std::make_pair(db::element::remote_node(-1, 1337), params));
                    return next;
                } else { // we need to send a prop
                    bool get_neighbors = true;
                    if (node_state.visited_map.count(params.next_node_id) > 0) {
                        size_t old_cost = node_state.visited_map[params.next_node_id].second;
                        // keep searching if better path exists to that node
                        if (params.is_widest_path ? old_cost >= params.cost : old_cost <= params.cost){
                            get_neighbors = false;
                        }
                    }
                    if (get_neighbors){
                        next.emplace_back(std::make_pair(next_to_add.node, params));
                        std::cout << "sending prop to a neighbor " <<  std::endl;
                        return next;
                    }
                    else {

                        std::cout << "get neighbors failed " <<  std::endl;
                    }
                }
            }
            // dest couldn't be reached, send failure to coord
            std::vector<std::pair<uint64_t, uint64_t>> emptyPath;
            params.final_path = emptyPath;
            params.cost = 0;
            next.emplace_back(std::make_pair(db::element::remote_node(-1, 1337), params));
        } else { // it is a request to add neighbors
            std::cout << "not at source! " <<  std::endl;
            // check the properties of each out-edge, assumes lock for node is held
            for (const std::pair<size_t, db::element::edge*> &e : n.out_edges)
            {
                bool use_edge = e.second->get_creat_time() <= req_id  
                    && e.second->get_del_time() > req_id; // edge created and deleted in acceptable timeframe

                for (size_t i = 0; i < params.edge_props.size() && use_edge; i++) // checking edge properties
                {
                    if (!e.second->has_property(params.edge_props[i])) {
                        use_edge = false;
                        break;
                    }
                }
                if (use_edge) {
                    // first is whether key exists, second is value
                    std::pair<bool, size_t> weightpair = e.second->get_property_value(params.edge_weight_name, req_id);
                    if (weightpair.first) {
                        size_t priority = calculate_priority(params.cost, weightpair.second, params.is_widest_path);
                        params.entries_to_add.emplace_back(std::make_pair(priority, e.second->nbr));
                    }
                }
            }
            std::cout << "prop added " << params.entries_to_add.size() << " of its neighbors sending to: " << params.source_node.handle<< std::endl;
            params.adding_nodes = true;
            next.emplace_back(std::make_pair(params.source_node, params));
        }
        return next;
    }

std::vector<std::pair<db::element::remote_node, dijkstra_params>> 
dijkstra_node_deleted_program(uint64_t req_id,
        db::element::node &n, // node who asked to go to deleted node
        uint64_t deleted_handle, // handle of node that didn't exist
        dijkstra_params &params_given, // params we had sent to deleted node
        std::function<dijkstra_node_state&()> state_getter){
    std::cout << "DELETED PROGRAM " <<  std::endl;
    params_given.adding_nodes = false;
    std::vector<std::pair<db::element::remote_node, dijkstra_params>> next;
    next.emplace_back(std::make_pair(params_given.source_node, params_given));
    return next;
}
}

#endif //__DIKJSTRA_PROG__
