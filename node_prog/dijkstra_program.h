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
            if (params.adding_nodes == true){ // response from a propagation, add nodes it could potentially reach to priority queue
                if (params.is_widest_path) {
                    for (auto &elem : params.entries_to_add) {
                        node_statenext_nodes_widest.emplace(elem.first, elem.second, params.prev_node_id);
                    }
                } else {
                    for (auto &elem : params.entries_to_add) {
                        node_state.next_nodes_shortest.emplace(elem.first, elem.second, params.prev_node_id);
                    }
                }
                // XXX add to visited map here?
                node_state.visited_map.emplace(next_req_id, std::make_pair(current_cost, next_to_add.prev_node_req_id)); // mark the cost to get here
            } else { // starting the request, add source to priority queue
                if (params.is_widest_path){
                    params.next_nodes_widest.emplace(MAX_TIME, source_node, node.get_creat_id()); // don't want source node to be bottleneck in path
                } else {
                    params.next_nodes_shortest.emplace(0, source_node, node.get_creat_id());
                }
                params.adding_nodes = true;
            }
            // select which node to visit next based on priority queue
            size_t current_cost = 0;
            db::dijkstra_queue_elem next_to_add; //XXX change to reference, maybe need const
            uint64_t next_req_id;

            while (!request->next_nodes_shortest.empty() || !request->next_nodes_widest.empty()) {
                if (params.is_widest_path) {
                    next_to_add = node_state.next_nodes_widest.top();
                    node_state.next_nodes_widest.pop();
                } else {
                    next_to_add = node_state.next_nodes_shortest.top();
                    node_state.next_nodes_shortest.pop();
                }
                size_t current_cost = next_to_add.cost;
                uint64_t next_node_id = next_to_add.node.handle;
                // we have found destination! We know it was not deleted as coord checked
                if (next_node_id == params.dest_handle) {
                    // rebuild path based on req_id's in visited_map
                    size_t cur = next_to_add.prev_node_req_id;
                    size_t curc = next_to_add.cost;
                    size_t next = node_state.visited_map[cur].second; // prev_node_creat_id for that node
                    size_t nextc = node_state.visited_map[cur].first; 
                    while (cur != next){
                        params.final_path.push_back(std::make_pair(cur, curc));
                        std::swap(cur, next);
                        std::swap(curc, nextc);
                        next = node_state.visited_map[cur].second;
                        nextc = node_state.visited_map[cur].first;
                    }
                    params.final_cost = current_cost;
                    next.emplace_back(std::make_pair(db::element::remote_node(-1, 1337), params));
                } else { // we need to send a prop
                    bool get_neighbors = true;
                    if (node_state.visited_map.count(next_node_id) > 0) {
                        size_t old_cost = node_state.visited_map[next_req_id].first;
                        if (params.is_widest_path ? old_cost >= current_cost : old_cost <= current_cost){
                            get_neighbors = false;
                        }
                        if (get_neighbors){
                            params.prev_node_id = next_req_id;
                            next.emplace_back(std::make_pair(next_to_add.node, params));
                            return next;
                        }
                    }
                }
                // dest couldn't be reached, send failure to coord
                std::vector<uint64_t> emptyPath;
                params.final_path = emptypath;
                params.final_cost = 0;
                next.emplace_back(std::make_pair(db::element::remote_node(-1, 1337), params));
            } else { // it is a request to add neighbors
                auto list_add_fun = [&params] (db::element::edge * e) {
                    // first is whether key exists, second is value
                    std::pair<bool, size_t> weightpair = e->get_property_value(req->edge_weight_name, req->start_time); 
                    if (weightpair.first) {
                        size_t priority = calculate_priority(params.current_cost, weightpair.second, params.is_widest_path);
                        params.entries_to_add.emplace_back(std::make_pair(priority, e->nbr));
                    }
                };
                apply_to_valid_edges(next_node, params.edge_props, req_id, list_add_fun);
                params.adding_nodes = true;
                next.emplace_back(std::make_pair(params.source_node, params));
            }
            return next;
        }
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
