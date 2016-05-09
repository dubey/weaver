/*
 * ===============================================================
 *    Description:  Test program for dynamic linking
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"

using cl::client;
using node_prog::traverse_props_params;

int main()
{
    client c("127.0.0.1", 2002, "/usr/local/etc/weaver.yaml");

    std::string prog_handle;
    //c.register_node_prog("./.libs/libtestprog.so", prog_handle);
    //std::cout << "client got prog handle = " << prog_handle << std::endl;

    std::string empty;
    std::string ayush = "ayush";
    std::string egs   = "egs";
    std::vector<std::string> empty_str_vec;
    std::vector<std::pair<std::string, std::string>> empty_str_pair_vec;

    c.begin_tx();
    c.create_node(ayush, empty_str_vec);
    c.create_node(egs, empty_str_vec);
    std::string edge_handle;
    c.create_edge(edge_handle, ayush, empty, egs, empty);
    c.end_tx();

    db::remote_node coord(0, std::string());
    traverse_props_params arg_param, ret_param;
    arg_param.returning = false;
    arg_param.prev_node = coord;
    arg_param.node_aliases.emplace_back(empty_str_vec);
    arg_param.node_props.emplace_back(empty_str_pair_vec);
    arg_param.edge_props.emplace_back(empty_str_pair_vec);
    arg_param.node_aliases.emplace_back(empty_str_vec);
    arg_param.node_props.emplace_back(empty_str_pair_vec);
    arg_param.collect_nodes = false;
    arg_param.collect_edges = false;

    std::vector<std::pair<std::string, traverse_props_params>> prog_args;
    prog_args.emplace_back(std::make_pair(ayush, arg_param));
    c.traverse_props_program(prog_args, ret_param);

    bool found = false;
    for (const std::string &s: ret_param.return_nodes) {
        if ("egs" == s) {
            found = true;
        }
    }

    assert(found);

    return 0;
}
