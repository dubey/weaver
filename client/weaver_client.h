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
#include "common/server_manager_link_wrapper.h"
#include "common/transaction.h"
#include "client/comm_wrapper.h"
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
#include "node_prog/traverse_with_props.h"

namespace cl
{
    class client
    {
        public:
            client(const char *coordinator, uint16_t port, const char *config_file);

        private:
            uint64_t myid, vtid;
            std::string myid_str;
            std::unique_ptr<cl::comm_wrapper> comm;
            server_manager_link m_sm;
            transaction::tx_list_t cur_tx;
            uint64_t cur_tx_id, tx_id_ctr, handle_ctr;

        public:
            void begin_tx();
            std::string create_node(std::string &handle);
            std::string create_edge(std::string &handle, std::string &node1, std::string &node2);
            void delete_node(std::string &node); 
            void delete_edge(std::string &edge, std::string &node);
            void set_node_property(std::string &node, std::string key, std::string value);
            void set_edge_property(std::string &node, std::string &edge, std::string key, std::string value);
            bool end_tx();

            template <typename ParamsType>
            std::unique_ptr<ParamsType> run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<std::string, ParamsType>> &initial_args);
            node_prog::reach_params run_reach_program(std::vector<std::pair<std::string, node_prog::reach_params>> &initial_args);
            node_prog::pathless_reach_params run_pathless_reach_program(std::vector<std::pair<std::string, node_prog::pathless_reach_params>> &initial_args);
            node_prog::clustering_params run_clustering_program(std::vector<std::pair<std::string, node_prog::clustering_params>> &initial_args);
            node_prog::two_neighborhood_params run_two_neighborhood_program(std::vector<std::pair<std::string, node_prog::two_neighborhood_params>> &initial_args);
            //node_prog::dijkstra_params run_dijkstra_program(std::vector<std::pair<std::string, node_prog::dijkstra_params>> &initial_args);
            node_prog::read_node_props_params read_node_props_program(std::vector<std::pair<std::string, node_prog::read_node_props_params>> &initial_args);
            node_prog::read_edges_props_params read_edges_props_program(std::vector<std::pair<std::string, node_prog::read_edges_props_params>> &initial_args);
            node_prog::read_n_edges_params read_n_edges_program(std::vector<std::pair<std::string, node_prog::read_n_edges_params>> &initial_args);
            node_prog::edge_count_params edge_count_program(std::vector<std::pair<std::string, node_prog::edge_count_params>> &initial_args);
            node_prog::edge_get_params edge_get_program(std::vector<std::pair<std::string, node_prog::edge_get_params>> &initial_args);
            node_prog::traverse_props_params traverse_props_program(std::vector<std::pair<std::string, node_prog::traverse_props_params>> &initial_args);

            void start_migration();
            void single_stream_migration();
            void exit_weaver();
            uint64_t get_vt_id() { return vtid; }
            std::vector<uint64_t> get_node_count();

        private:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
#pragma GCC diagnostic pop
            std::string generate_handle();
            bool maintain_sm_connection();
            void reconfigure();
    };
}

#endif
