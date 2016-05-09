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

#include <exception>
#include <fstream>
#include <unordered_map>
#include <po6/net/location.h>

#include "common/message_constants.h"
#include "common/server_manager_link_wrapper.h"
#include "common/transaction.h"
#include "client/weaver/weaver_returncode.h"
#include "client/comm_wrapper.h"
#include "client/datastructures.h"
#include "node_prog/dynamic_prog_table.h"
#include "node_prog/base_classes.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/traverse_with_props.h"

namespace cl
{
    class weaver_client_exception : public std::exception
    {
        std::string message;

        public:
            weaver_client_exception(const std::string &msg)
                : std::exception()
                , message(msg)
            { }

            virtual const char* what() const throw()
            {
                return message.c_str();
            }
    };

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
            std::unordered_map<std::string, std::shared_ptr<dynamic_prog_table>> m_dyn_prog_map;
            std::unordered_map<std::string, std::string> m_built_in_progs;

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

            weaver_client_returncode run_node_prog(const std::string &prog_type,
                                                   std::vector<std::pair<std::string, std::shared_ptr<node_prog::Node_Parameters_Base>>> &args,
                                                   std::shared_ptr<node_prog::Node_Parameters_Base> &return_param);
            weaver_client_returncode traverse_props_program(std::vector<std::pair<std::string, node_prog::traverse_props_params>> &initial_args,
                                                            node_prog::traverse_props_params&);

            weaver_client_returncode register_node_prog(const std::string &so_file,
                                                        std::string &prog_handle);
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
