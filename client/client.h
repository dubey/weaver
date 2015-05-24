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
#include "client/weaver_returncode.h"
#include "client/comm_wrapper.h"
#include "client/datastructures.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "node_prog/pathless_reach_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/two_neighborhood_program.h"
#include "node_prog/read_node_props_program.h"
#include "node_prog/read_n_edges_program.h"
#include "node_prog/edge_count_program.h"
#include "node_prog/edge_get_program.h"
#include "node_prog/node_get_program.h"
#include "node_prog/traverse_with_props.h"
#include "node_prog/discover_paths.h"
#include "node_prog/get_btc_block.h"

namespace cl
{
    class client
    {
        public:
            client(const char *coordinator, uint16_t port, const char *config_file);
            void initialize_logging();

        private:
            uint64_t myid, vtid;
            std::string myid_str;
            std::unique_ptr<cl::comm_wrapper> comm;
            server_manager_link m_sm;
            transaction::tx_list_t cur_tx;
            uint64_t cur_tx_id, tx_id_ctr, handle_ctr;
            bool init;
            bool logging;
            weaver_client_returncode fail_tx(weaver_client_returncode);

        public:
            weaver_client_returncode begin_tx();
            weaver_client_returncode create_node(std::string &handle, const std::vector<std::string> &aliases);
            weaver_client_returncode create_edge(std::string &handle, const std::string &node1, const std::string &node1_alias, const std::string &node2, const std::string &node2_alias);
            weaver_client_returncode delete_node(const std::string &node, const std::string &alias);
            weaver_client_returncode delete_edge(const std::string &edge, const std::string &node, const std::string &node_alias);
            weaver_client_returncode set_node_property(const std::string &node, const std::string &alias, std::string key, std::string value);
            weaver_client_returncode set_edge_property(const std::string &node, const std::string &alias, const std::string &edge, std::string key, std::string value);
            weaver_client_returncode add_alias(const std::string &alias, const std::string &node);
            weaver_client_returncode end_tx();
            weaver_client_returncode abort_tx();

            template <typename ParamsType>
            weaver_client_returncode run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<std::string, ParamsType>> &initial_args, ParamsType &return_param);
            weaver_client_returncode run_reach_program(std::vector<std::pair<std::string, node_prog::reach_params>> &initial_args, node_prog::reach_params&);
            weaver_client_returncode run_pathless_reach_program(std::vector<std::pair<std::string, node_prog::pathless_reach_params>> &initial_args, node_prog::pathless_reach_params&);
            weaver_client_returncode run_clustering_program(std::vector<std::pair<std::string, node_prog::clustering_params>> &initial_args, node_prog::clustering_params&);
            weaver_client_returncode run_two_neighborhood_program(std::vector<std::pair<std::string, node_prog::two_neighborhood_params>> &initial_args, node_prog::two_neighborhood_params&);
            weaver_client_returncode read_node_props_program(std::vector<std::pair<std::string, node_prog::read_node_props_params>> &initial_args, node_prog::read_node_props_params&);
            weaver_client_returncode read_n_edges_program(std::vector<std::pair<std::string, node_prog::read_n_edges_params>> &initial_args, node_prog::read_n_edges_params&);
            weaver_client_returncode edge_count_program(std::vector<std::pair<std::string, node_prog::edge_count_params>> &initial_args, node_prog::edge_count_params&);
            weaver_client_returncode edge_get_program(std::vector<std::pair<std::string, node_prog::edge_get_params>> &initial_args, node_prog::edge_get_params&);
            weaver_client_returncode node_get_program(std::vector<std::pair<std::string, node_prog::node_get_params>> &initial_args, node_prog::node_get_params&);
            weaver_client_returncode traverse_props_program(std::vector<std::pair<std::string, node_prog::traverse_props_params>> &initial_args, node_prog::traverse_props_params&);
            weaver_client_returncode discover_paths_program(std::vector<std::pair<std::string, node_prog::discover_paths_params>> &initial_args, node_prog::discover_paths_params&);
            weaver_client_returncode get_btc_block_program(std::vector<std::pair<std::string, node_prog::get_btc_block_params>> &initial_args, node_prog::get_btc_block_params&);

            weaver_client_returncode start_migration();
            weaver_client_returncode single_stream_migration();
            weaver_client_returncode exit_weaver();
            uint64_t get_vt_id() { return vtid; }
            weaver_client_returncode get_node_count(std::vector<uint64_t>&);
            bool aux_index();
            void print_cur_tx();

        private:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
#pragma GCC diagnostic pop
            std::string generate_handle();
            bool maintain_sm_connection(replicant_returncode &rc);
            void reconfigure();
    };
}

#endif
