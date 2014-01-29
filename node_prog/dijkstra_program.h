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
#include <string.h>
#include <sstream>

#include "common/message.h"
#include "common/public_graph_elems/node.h"
#include "common/public_graph_elems/edge.h"
#include "common/public_graph_elems/node_ptr.h"

namespace node_prog
{
    class dijkstra_queue_elem : public virtual Node_State_Base // inherits this so its packable, inheriting packable directly might not work
    {
        public:
            uint64_t cost;
            common::node_ptr node;
            uint64_t prev_node_req_id; // used for reconstructing path in coordinator

            int operator<(const dijkstra_queue_elem& other) const
            { 
                return other.cost < cost; 
            }

            int operator>(const dijkstra_queue_elem& other) const
            { 
                return other.cost > cost; 
            }

            dijkstra_queue_elem() { }

            dijkstra_queue_elem(uint64_t c, common::node_ptr n, uint64_t prev)
                : cost(c)
                , node(n)
                , prev_node_req_id(prev)
            {
            }

        public:
            virtual uint64_t size() const
            {
                uint64_t sz = message::size(cost)
                    + message::size(node)
                    + message::size(prev_node_req_id);
                return sz;
            }

            virtual void pack(e::buffer::packer &packer) const
            {
                message::pack_buffer(packer, cost);
                message::pack_buffer(packer, node);
                message::pack_buffer(packer, prev_node_req_id);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, cost);
                message::unpack_buffer(unpacker, node);
                message::unpack_buffer(unpacker, prev_node_req_id);
            }
    };

    class dijkstra_params : public virtual Node_Parameters_Base  
    {
        public:
            uint64_t src_handle;
            common::node_ptr source_node;
            uint64_t dst_handle;
            std::string edge_weight_name; // the name of the property which holds the weight of an an edge
            std::vector<db::element::property> edge_props;
            bool is_widest_path;
            bool adding_nodes;
            uint64_t prev_node;
            std::vector<std::pair<uint64_t, common::node_ptr>> entries_to_add;
            uint64_t next_node;
            std::vector<std::pair<uint64_t, uint64_t>> final_path;
            uint64_t cost;
            uint64_t vt_id;

            virtual ~dijkstra_params() { }

        public:
            virtual bool search_cache() {
                return false;
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = 0;
                toRet += message::size(src_handle);
                toRet += message::size(source_node);
                toRet += message::size(dst_handle);
                toRet += message::size(edge_weight_name);
                toRet += message::size(edge_props);
                toRet += message::size(is_widest_path);
                toRet += message::size(adding_nodes);
                toRet += message::size(prev_node);
                toRet += message::size(entries_to_add);
                toRet += message::size(next_node);
                toRet += message::size(final_path);
                toRet += message::size(cost);
                toRet += message::size(vt_id);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, src_handle);
                message::pack_buffer(packer, source_node);
                message::pack_buffer(packer, dst_handle);
                message::pack_buffer(packer, edge_weight_name);
                message::pack_buffer(packer, edge_props);
                message::pack_buffer(packer, is_widest_path);
                message::pack_buffer(packer, adding_nodes);
                message::pack_buffer(packer, prev_node);
                message::pack_buffer(packer, entries_to_add);
                message::pack_buffer(packer, next_node);
                message::pack_buffer(packer, final_path);
                message::pack_buffer(packer, cost);
                message::pack_buffer(packer, vt_id);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, src_handle);
                message::unpack_buffer(unpacker, source_node);
                message::unpack_buffer(unpacker, dst_handle);
                message::unpack_buffer(unpacker, edge_weight_name);
                message::unpack_buffer(unpacker, edge_props);
                message::unpack_buffer(unpacker, is_widest_path);
                message::unpack_buffer(unpacker, adding_nodes);
                message::unpack_buffer(unpacker, prev_node);
                message::unpack_buffer(unpacker, entries_to_add);
                message::unpack_buffer(unpacker, next_node);
                message::unpack_buffer(unpacker, final_path);
                message::unpack_buffer(unpacker, cost);
                message::unpack_buffer(unpacker, vt_id);
            }
    };

    struct dijkstra_node_state : public virtual Node_State_Base 
    {
        std::priority_queue<dijkstra_queue_elem,
                            std::vector<dijkstra_queue_elem>,
                            std::less<dijkstra_queue_elem>> pq_shortest; 
        std::priority_queue<dijkstra_queue_elem,
                            std::vector<dijkstra_queue_elem>,
                            std::greater<dijkstra_queue_elem>> pq_widest; 
        // map from a node (by its create time) to the req_id of the node
        // that came before it in the shortest path and its cost
        std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> visited; 

        virtual ~dijkstra_node_state() { }

        virtual uint64_t size() const
        {
            uint64_t sz = message::size(pq_shortest)
                + message::size(pq_widest)
                + message::size(visited);
            return sz;
        }
        virtual void pack(e::buffer::packer& packer) const 
        {
            message::pack_buffer(packer, pq_shortest);
            message::pack_buffer(packer, pq_widest);
            message::pack_buffer(packer, visited);
        }
        virtual void unpack(e::unpacker& unpacker)
        {
            message::unpack_buffer(unpacker, pq_shortest);
            message::unpack_buffer(unpacker, pq_widest);
            message::unpack_buffer(unpacker, visited);
        }
    };

    struct dijkstra_cache_value : public virtual Cache_Value_Base 
    {
        /*
        uint32_t edge_key; // edge weight key
        uint64_t dst_node;
        uint64_t cost;
        bool is_widest;
        
        */
        virtual ~dijkstra_cache_value() { }
    };

    inline uint64_t
    calculate_priority(uint64_t current_cost, uint64_t edge_cost, bool is_widest_path)
    {
        uint64_t priority;
        if (is_widest_path) {
            priority = current_cost < edge_cost ? current_cost : edge_cost;
        } else {
            priority = edge_cost + current_cost;
        }
        return priority;
    }

    std::vector<std::pair<common::node_ptr, dijkstra_params>> 
    dijkstra_node_program(
            common::node &n,
            common::node_ptr &rn,
            dijkstra_params &params,
            std::function<dijkstra_node_state&()> state_getter,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>, std::shared_ptr<std::vector<common::node_ptr>>, uint64_t)>&,
            std::unique_ptr<db::caching::cache_response>)
    {
        WDEBUG << "DIJKSTRAAAAA" << std::endl;
        std::vector<std::pair<common::node_ptr, dijkstra_params>> next;
        /*
        if (n.get_handle() == params.src_handle) {
            dijkstra_node_state &node_state = state_getter();
            WDEBUG << "Dijkstra program: at source" <<  std::endl;
            if (params.adding_nodes == true) { 
                // response from a propagation, add nodes it could potentially reach to priority queue
                if (params.is_widest_path) {
                    for (auto &elem : params.entries_to_add) {
                        node_state.pq_widest.emplace(elem.first, elem.second, params.next_node);
                    }
                } else {
                    for (auto &elem : params.entries_to_add) {
                        node_state.pq_shortest.emplace(elem.first, elem.second, params.next_node);
                    }
                }
                params.entries_to_add.clear();
                node_state.visited.emplace(params.next_node, std::make_pair(params.prev_node, params.cost));
            } else { 
                if (node_state.visited.count(params.src_handle) > 0) { 
                    // response from a deleted node
                    params.entries_to_add.clear();
                } else { 
                    // starting the request, add source neighbors to priority queue
                    params.source_node = rn;
                    params.cost = params.is_widest_path ? MAX_TIME : 0; // don't want source node to be bottleneck in path
                    node_state.visited.emplace(params.src_handle, std::make_pair(params.src_handle, params.cost)); // handles same at source

                    common::edge *e;
                    for (auto &iter: n.out_edges) {
                        e = iter.second;
                        // edge created and deleted in acceptable timeframe
                        bool use_edge = order::clock_creat_before_del_after(*req_vclock, e->get_creat_time(), e->get_del_time());
                        /*
                        for (uint64_t i = 0; i < params.edge_props.size() && use_edge; i++) {
                            // checking edge properties
                            if (!e->has_property(params.edge_props[i])) {
                                use_edge = false;
                                break; 
                            }
                        }
                        */
                        /*
                        if (use_edge) {
                            // first is whether key exists, second is value
                            std::pair<bool, std::string> weightpair = 
                                    e->get_property_value(params.edge_weight_name, *req_vclock);
                            if (weightpair.first) {
                                uint64_t edge_weight;
                                std::stringstream(weightpair.second) >> edge_weight;
                                WDEBUG << "got edge weight " << edge_weight << " from string " << weightpair.second << std::endl;
                                uint64_t priority = calculate_priority(params.cost, edge_weight, params.is_widest_path);
                                if (params.is_widest_path) {
                                    node_state.pq_widest.emplace(priority, e->nbr, params.src_handle); 
                                } else {
                                    node_state.pq_shortest.emplace(priority, e->nbr, params.src_handle);
                                }
                            }
                        }
                    }
                }
                params.adding_nodes = true;
            }
            // select which node to visit next based on priority queue
            dijkstra_queue_elem next_to_add; //XXX change to reference, maybe need const

            WDEBUG << "DIJKSTRA SHORTEST PQ SIZE "<< node_state.pq_shortest.size() << " WIDEST PQ SIZE " << node_state.pq_widest.size() << std::endl;
            while (!node_state.pq_shortest.empty() || !node_state.pq_widest.empty()) {
                if (params.is_widest_path) {
                    next_to_add = node_state.pq_widest.top();
                    node_state.pq_widest.pop();
                } else {
                    next_to_add = node_state.pq_shortest.top();
                    node_state.pq_shortest.pop();
                }
                params.cost = next_to_add.cost;
                params.next_node = next_to_add.node.handle;
                params.prev_node = next_to_add.prev_node_req_id;
                if (params.next_node == params.dst_handle) {
                    WDEBUG << "DIJKSTRA found dest" << std::endl;
                    // we have found destination! We know it was not deleted as coord checked
                    std::pair<uint64_t, uint64_t> visited_entry;
                    // rebuild path based on req_id's in visited
                    if (params.is_widest_path) {
                        uint64_t cur_node = params.dst_handle;
                        uint64_t cur_cost = params.cost;
                        params.final_path.push_back(std::make_pair(cur_node, cur_cost));
                        cur_node = params.prev_node;
                        visited_entry = node_state.visited[params.prev_node];
                        while (cur_node != params.src_handle) {
                            cur_cost = visited_entry.second;
                            params.final_path.push_back(std::make_pair(cur_node, cur_cost));
                            cur_node = visited_entry.first;
                            visited_entry = node_state.visited[cur_node];
                        }
                    } else {
                        // shortest path, have to calculate edge weight based on cumulative cost to node before
                        uint64_t old_cost = params.cost;
                        uint64_t old_node = params.dst_handle; // the node father from sourc
                        uint64_t cur_node = params.prev_node;
                        while (old_node != params.src_handle) {
                            visited_entry = node_state.visited[cur_node];
                            params.final_path.push_back(std::make_pair(old_node, old_cost-visited_entry.second));
                            old_node = cur_node;
                            old_cost = visited_entry.second;
                            cur_node = visited_entry.first;
                        }
                    }
                    next.emplace_back(std::make_pair(common::node_ptr(params.vt_id, 1337), params));
                    return next;
                } else { // we need to send a prop
                    bool get_neighbors = true;
                    if (node_state.visited.count(params.next_node) > 0) {
                        uint64_t old_cost = node_state.visited[params.next_node].second;
                        // keep searching if better path exists to that node
                        if (params.is_widest_path ? old_cost >= params.cost : old_cost <= params.cost){
                            get_neighbors = false;
                        }
                    }
                    if (get_neighbors) {
                        WDEBUG << "DIJKSTRA sending to node "<< next_to_add.node.handle << " on shard " <<  next_to_add.node.loc << std::endl;
                        next.emplace_back(std::make_pair(next_to_add.node, params));
                        return next;
                    }
                }
            }
            // dest couldn't be reached, send failure to coord
            std::vector<std::pair<uint64_t, uint64_t>> emptyPath;
            params.final_path = emptyPath;
            params.cost = 0;
            next.emplace_back(std::make_pair(common::node_ptr(COORD_ID, 1337), params));

        } else { // it is a request to add neighbors
            // check the properties of each out-edge, assumes lock for node is held
            WDEBUG << "Dijkstra program: NOT source" <<  std::endl;
            common::edge *e;
            for (auto &iter: n.out_edges) {
                e = iter.second;
                // edge created and deleted in acceptable timeframe
                bool use_edge = order::clock_creat_before_del_after(*req_vclock, e->get_creat_time(), e->get_del_time());

                if (use_edge) {
                    // first is whether key exists, second is value
                    std::pair<bool, std::string> weightpair = e->get_property_value(params.edge_weight_name, *req_vclock);
                    if (weightpair.first) {
                        uint64_t edge_weight;
                        std::stringstream(weightpair.second) >> edge_weight;
                        WDEBUG << "got edge weight " << edge_weight << " from string " << weightpair.second << std::endl;
                        uint64_t priority = calculate_priority(params.cost, edge_weight, params.is_widest_path);
                        WDEBUG << " trying to add edge with nbr loc " << e->nbr.loc << " and nbr handle " << e->nbr.handle << std::endl;
                        assert(e->nbr.loc < NUM_SHARDS + SHARD_ID_INCR); //XXX
                        params.entries_to_add.emplace_back(std::make_pair(priority, e->nbr));
                    }
                }
            }
            params.adding_nodes = true;
            next.emplace_back(std::make_pair(params.source_node, params));
        }
        */
        return next;
    }

    /*
    std::vector<std::pair<common::node_ptr, dijkstra_params>> 
    dijkstra_node_deleted_program(uint64_t req_id,
        common::node &n, // node who asked to go to deleted node
        uint64_t deleted_handle, // handle of node that didn't exist
        dijkstra_params &params_given, // params we had sent to deleted node
        std::function<dijkstra_node_state&()> state_getter)
    {
        UNUSED(req_id);
        UNUSED(n);
        UNUSED(state_getter);

        WDEBUG << "DELETED PROGRAM " << deleted_handle << std::endl;
        params_given.adding_nodes = false;
        std::vector<std::pair<common::node_ptr, dijkstra_params>> next;
        next.emplace_back(std::make_pair(params_given.source_node, params_given));
        return next;
    }
    */
}

#endif //__DIKJSTRA_PROG__
