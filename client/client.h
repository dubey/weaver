/*
 * ===============================================================
 *    Description:  Client stub functions
 *
 *        Created:  01/23/2013 02:13:12 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_client_h_
#define weaver_client_client_h_

#include <fstream>
#include <unordered_map>
#include <po6/net/location.h>

#include "common/message_constants.h"
#include "common/comm_wrapper.h"
#include "transaction.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "node_prog/pathless_reach_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/two_neighborhood_program.h"
//#include "node_prog/dijkstra_program.h"
#include "node_prog/read_node_props_program.h"
#include "node_prog/read_edges_props_program.h"
#include "node_prog/read_n_edges_program.h"
#include "node_prog/edge_count_program.h"
#include "node_prog/edge_get_program.h"

namespace client
{
    class client
    {
        public:
            client(uint64_t my_id, uint64_t vt_id);

        private:
            uint64_t myid, shifted_id, vtid;
            common::comm_wrapper comm;
            tx_list_t cur_tx;
            uint64_t cur_tx_id, tx_id_ctr, temp_handle_ctr;

        public:
            void begin_tx();
            uint64_t create_node();
            uint64_t create_edge(uint64_t node1, uint64_t node2);
            void delete_node(uint64_t node); 
            void delete_edge(uint64_t edge, uint64_t node);
            void set_node_property(uint64_t node, std::string &key, std::string &value);
            void set_edge_property(uint64_t node, uint64_t edge, std::string &key, std::string &value);
            bool end_tx();

            template <typename ParamsType>
            std::unique_ptr<ParamsType> 
            run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args);
            node_prog::reach_params run_reach_program(std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args);
            node_prog::pathless_reach_params run_pathless_reach_program(std::vector<std::pair<uint64_t, node_prog::pathless_reach_params>> initial_args);
            node_prog::clustering_params run_clustering_program(std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args);
            node_prog::two_neighborhood_params run_two_neighborhood_program(std::vector<std::pair<uint64_t, node_prog::two_neighborhood_params>> initial_args);
            //node_prog::dijkstra_params run_dijkstra_program(std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args);
            node_prog::read_node_props_params read_node_props_program(std::vector<std::pair<uint64_t, node_prog::read_node_props_params>> initial_args);
            node_prog::read_edges_props_params read_edges_props_program(std::vector<std::pair<uint64_t, node_prog::read_edges_props_params>> initial_args);
            node_prog::read_n_edges_params read_n_edges_program(std::vector<std::pair<uint64_t, node_prog::read_n_edges_params>> initial_args);
            node_prog::edge_count_params edge_count_program(std::vector<std::pair<uint64_t, node_prog::edge_count_params>> initial_args);
            node_prog::edge_get_params edge_get_program(std::vector<std::pair<uint64_t, node_prog::edge_get_params>> initial_args);

            void start_migration();
            void single_stream_migration();
            void commit_graph();
            void exit_weaver();
            void print_msgcount();
            std::vector<uint64_t> get_node_count();

        private:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            void send_coord(std::auto_ptr<e::buffer> buf);
            busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
#pragma GCC diagnostic pop
            void reconfigure();
            uint64_t generate_handle();
    };
}

#endif
