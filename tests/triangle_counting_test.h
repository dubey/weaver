#include "client/client.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/triangle_program.h"
#define LENGTH 4000

void line_triangle_test(client::client &c) {
    // make line of length line_length
    std::cout << "line reachability makign graph" << std::endl;
    uint64_t tx_id = c.begin_tx();
    size_t nodes[LENGTH];
    for (int i = 0; i < LENGTH; i++) {
        nodes[i] = c.create_node(tx_id);
    }
    std::cout << "sending node transation" << std::endl;
    c.end_tx(tx_id);
    std::cout << "made nodes" << std::endl;

    int num_edges = 0;
    tx_id = c.begin_tx();
    for (int i = 0; i < LENGTH-1; i++) {
        c.create_edge(tx_id, nodes[i], nodes[i+1]);
        num_edges++;
        c.create_edge(tx_id, nodes[i+1], nodes[i]); // temp to see if things break
        num_edges++;
    }
    c.end_tx(tx_id);

    std::vector<std::pair<uint64_t, node_prog::triangle_params>> initial_args;
    node_prog::triangle_params params;
    initial_args.emplace_back(std::make_pair(-1, params));
     std::unique_ptr<node_prog::triangle_params>res = c.run_node_program(node_prog::TRIANGLE_COUNT, initial_args);
    std::cout << "on " << num_edges<< " graph ... was " << res->num_edges << std::endl << std::endl;
    assert(res->num_edges == (2 * LENGTH)-2);

    test_graph * g = new test_graph(&c, time(NULL), 10000, 100000, false, false);
    num_edges += 100000;

    node_prog::triangle_params params2;
    initial_args.clear();
    initial_args.emplace_back(std::make_pair(-1, params2));
     res = c.run_node_program(node_prog::TRIANGLE_COUNT, initial_args);
    std::cout << "... was " << res->num_edges << std::endl << std::endl;
    assert(res->num_edges == num_edges);
}

void
triangle_counting_test()
{
    client::client c(CLIENT_ID, 0);
    line_triangle_test(c);
}
