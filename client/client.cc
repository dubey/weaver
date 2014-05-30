/*
 * ===============================================================
 *    Description:  Client methods implementation.
 *
 *        Created:  03/19/2014 11:58:26 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/client.h"
#include "common/message.h"

namespace client
{

client :: client(uint64_t my_id, uint64_t vt_id)
    : myid(my_id)
    , shifted_id(myid << (64-ID_BITS))
    , vtid(vt_id)
    , comm(my_id, 1, CLIENT_MSGRECV_TIMEOUT, true)
    , tx_id_ctr(0)
    , temp_handle_ctr(0)
{
    comm.client_init();
}

uint64_t
client :: begin_tx()
{
    uint64_t tx_id = ++tx_id_ctr;
    tx_map.emplace(tx_id, std::vector<std::shared_ptr<pending_update>>());
    return tx_id;
}

uint64_t
client :: create_node(uint64_t tx_id)
{
    if (tx_map.find(tx_id) == tx_map.end()) {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
        return 0;
    } else {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_NODE_CREATE_REQ;
        upd->handle = generate_handle();
        tx_map[tx_id].emplace_back(upd);
        return upd->handle;
    }
}

uint64_t
client :: create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2)
{
    if (tx_map.find(tx_id) == tx_map.end()) {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
        return 0;
    } else {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_EDGE_CREATE_REQ;
        upd->handle = generate_handle();
        upd->elem1 = node1;
        upd->elem2 = node2;
        tx_map[tx_id].emplace_back(upd);
        return upd->handle;
    }
}

void
client :: delete_node(uint64_t tx_id, uint64_t node)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_NODE_DELETE_REQ;
        upd->elem1 = node;
        tx_map[tx_id].emplace_back(upd);
    } else {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
    }
}

void
client :: delete_edge(uint64_t tx_id, uint64_t edge, uint64_t node)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_EDGE_DELETE_REQ;
        upd->elem1 = edge;
        upd->elem2 = node;
        tx_map[tx_id].emplace_back(upd);
    } else {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
    }
}

void
client :: set_node_property(uint64_t tx_id, uint64_t node,
    std::string &key, std::string &value)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_NODE_SET_PROP;
        upd->elem1 = node;
        upd->key = std::move(key);
        upd->value = std::move(value);
        tx_map[tx_id].emplace_back(upd);
    } else {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
    }
}

void
client :: set_edge_property(uint64_t tx_id, uint64_t node, uint64_t edge,
    std::string &key, std::string &value)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<pending_update>();
        upd->type = message::CLIENT_EDGE_SET_PROP;
        upd->elem1 = edge;
        upd->elem2 = node;
        upd->key = std::move(key);
        upd->value = std::move(value);
        tx_map[tx_id].emplace_back(upd);
    } else {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
    }
}

bool
client :: end_tx(uint64_t tx_id)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        message::message msg;
        message::prepare_tx_message_client(msg, tx_map[tx_id]);
        send_coord(msg.buf);

        busybee_returncode recv_code = recv_coord(&msg.buf);
        if (recv_code == BUSYBEE_TIMEOUT) {
            // assume vt is dead, fail tx
            reconfigure();
            return false;
        }
        if (recv_code != BUSYBEE_SUCCESS) {
            WDEBUG << "tx msg recv fail" << std::endl;
            return false;
        }
        message::msg_type mtype = msg.unpack_message_type();
        assert(mtype == message::CLIENT_TX_DONE
            || mtype == message::CLIENT_TX_FAIL);
        if (mtype == message::CLIENT_TX_DONE) {
            return true;
        } else {
            return false;
        }
    } else {
        WDEBUG << "transaction id " << tx_id << " not found" << std::endl;
        return false;
    }
}

template <typename ParamsType>
std::unique_ptr<ParamsType>
client :: run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args)
{
    message::message msg;
    msg.prepare_message(message::CLIENT_NODE_PROG_REQ, prog_to_run, initial_args);
    send_coord(msg.buf);

    busybee_returncode recv_code = recv_coord(&msg.buf);
    if (recv_code == BUSYBEE_TIMEOUT) {
        // assume vt is dead
        reconfigure();
        return NULL;
    }
    if (recv_code != BUSYBEE_SUCCESS) {
        WDEBUG << "node prog return msg fail" << std::endl;
        return NULL;
    }

    uint64_t ignore_req_id;
    node_prog::prog_type ignore_type;
    std::unique_ptr<ParamsType> toRet(new ParamsType());
    msg.unpack_message(message::NODE_PROG_RETURN, ignore_type, ignore_req_id, *toRet);
    return std::move(toRet);
}

node_prog::reach_params
client :: run_reach_program(std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args)
{
    return *run_node_program(node_prog::REACHABILITY, initial_args);
}

node_prog::pathless_reach_params
client :: run_pathless_reach_program(std::vector<std::pair<uint64_t, node_prog::pathless_reach_params>> initial_args)
{
    return *run_node_program(node_prog::PATHLESS_REACHABILITY, initial_args);
}

node_prog::clustering_params
client :: run_clustering_program(std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args)
{
    return *run_node_program(node_prog::CLUSTERING, initial_args);
}

node_prog::two_neighborhood_params
client :: run_two_neighborhood_program(std::vector<std::pair<uint64_t, node_prog::two_neighborhood_params>> initial_args)
{
    return *run_node_program(node_prog::TWO_NEIGHBORHOOD, initial_args);
}

/*
node_prog::dijkstra_params
client :: run_dijkstra_program(std::vector<std::pair<uint64_t, node_prog::dijkstra_params>> initial_args)
{
    return *run_node_program(node_prog::DIJKSTRA, initial_args);
}
*/

node_prog::read_node_props_params
client :: read_node_props_program(std::vector<std::pair<uint64_t, node_prog::read_node_props_params>> initial_args)
{
    return *run_node_program(node_prog::READ_NODE_PROPS, initial_args);
}

node_prog::read_edges_props_params
client :: read_edges_props_program(std::vector<std::pair<uint64_t, node_prog::read_edges_props_params>> initial_args)
{
    return *run_node_program(node_prog::READ_EDGES_PROPS, initial_args);
}

node_prog::read_n_edges_params
client :: read_n_edges_program(std::vector<std::pair<uint64_t, node_prog::read_n_edges_params>> initial_args)
{
    return *run_node_program(node_prog::READ_N_EDGES, initial_args);
}

node_prog::edge_count_params
client :: edge_count_program(std::vector<std::pair<uint64_t, node_prog::edge_count_params>> initial_args)
{
    return *run_node_program(node_prog::EDGE_COUNT, initial_args);
}

node_prog::edge_get_params
client :: edge_get_program(std::vector<std::pair<uint64_t, node_prog::edge_get_params>> initial_args)
{
    return *run_node_program(node_prog::EDGE_GET, initial_args);
}

void
client :: start_migration()
{
    message::message msg;
    msg.prepare_message(message::START_MIGR);
    send_coord(msg.buf);
}

void
client :: single_stream_migration()
{
    message::message msg;
    msg.prepare_message(message::ONE_STREAM_MIGR);
    send_coord(msg.buf);

    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        WDEBUG << "single stream migration return msg fail" << std::endl;
    }
}

void
client :: commit_graph()
{
    message::message msg;
    msg.prepare_message(message::CLIENT_COMMIT_GRAPH);
    send_coord(msg.buf);
}


void
client :: exit_weaver()
{
    message::message msg;
    msg.prepare_message(message::EXIT_WEAVER);
    send_coord(msg.buf);
}

void
client :: print_msgcount()
{
    message::message msg;
    msg.prepare_message(message::CLIENT_MSG_COUNT);
    send_coord(msg.buf);
}

std::vector<uint64_t>
client :: get_node_count()
{
    message::message msg;
    msg.prepare_message(message::CLIENT_NODE_COUNT);
    send_coord(msg.buf);

    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        WDEBUG << "node count return msg fail" << std::endl;
    }
    std::vector<uint64_t> node_count;
    msg.unpack_message(message::NODE_COUNT_REPLY, node_count);
    return node_count;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
void
client :: send_coord(std::auto_ptr<e::buffer> buf)
{
    if (comm.send(vtid, buf) != BUSYBEE_SUCCESS) {
        return;
    }
}

busybee_returncode
client :: recv_coord(std::auto_ptr<e::buffer> *buf)
{
    busybee_returncode ret;
    uint64_t sender;
    while (true) {
        comm.quiesce_thread(0);
        ret = comm.recv(&sender, buf);
        switch (ret) {
            case BUSYBEE_SUCCESS:
            case BUSYBEE_TIMEOUT:
                return ret;

            case BUSYBEE_INTERRUPTED:
                continue;

            default:
                WDEBUG << "msg recv error: " << ret << std::endl;
                return ret;
        }
    }
}
#pragma GCC diagnostic pop

void
client :: reconfigure()
{
    // our vt is probably dead
    // set vtid to next backup, client has to retry
    vtid += (NUM_SHARDS + NUM_VTS);
}

// to generate 64 bit graph element handles
// assuming no more than 2^(ID_BITS) clients
// assuming no more than 2^(64-ID_BITS) graph nodes and edges created at this client
uint64_t
client :: generate_handle()
{
    uint64_t new_handle = (++temp_handle_ctr) & TOP_MASK;
    new_handle |= shifted_id;
    return new_handle;
}

}
