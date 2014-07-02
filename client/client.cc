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

#define weaver_debug_
#include "common/message.h"
#include "common/config_constants.h"
#include "client/client_constants.h"
#include "client/weaver_client.h"

// global extern variables
// ugly, defined only to get rid of undefined symbol error
// not used in client
uint64_t NumVts;
uint64_t NumShards;
uint64_t NumBackups;
uint64_t NumEffectiveServers;
uint64_t NumActualServers;
uint64_t ShardIdIncr;
char *ShardsFile;

using cl::client;
using transaction::pending_update;

//client :: client(const char *coordinator, uint16_t port)
client :: client()
    : m_sm("127.0.0.1", 2002)
    , cur_tx_id(UINT64_MAX)
    , tx_id_ctr(0)
    , temp_handle_ctr(0)
{
    init_config_constants("/usr/local/etc/weaver.yaml");

    vtid = rand() % NumVts;

    assert(m_sm.get_replid(myid) && "get repl_id");
    WDEBUG << "replicant id " << myid << std::endl;
    assert(myid > NumActualServers);
    shifted_id = myid << (64-ID_BITS);

    int try_sm = 0;
    while (!maintain_sm_connection()) {
        WDEBUG << "retry sm connection " << try_sm++ << std::endl;
    }

    //for (uint64_t i = 0; i < NumActualServers; i++) {
    //    server::state_t st = m_sm.config()->get_state(server_id(i));
    //    if (st != server::AVAILABLE) {
    //        WDEBUG << "Server " << i << " is in trouble, has state " << st << std::endl;
    //    } else {
    //        WDEBUG << "Server " << i << " is healthy, has state " << st << std::endl;
    //    }
    //}

    po6::net::location loc("127.0.0.1", 6200);
    comm.reset(new common::comm_wrapper(loc, 1, CLIENT_MSGRECV_TIMEOUT));
    WDEBUG << "here" << std::endl;
    comm->client_init(*m_sm.config());
    WDEBUG << "client created" << std::endl;
}

void
client :: begin_tx()
{
    assert(cur_tx_id == UINT64_MAX && "only one concurrent transaction per client");
    cur_tx_id = ++tx_id_ctr;
}

void
client :: create_node(std::string &handle)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_CREATE_REQ;
    upd->handle = handle;
    cur_tx.emplace_back(upd);
}

void
client :: create_edge(std::string &handle, std::string &node1, std::string &node2)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_CREATE_REQ;
    upd->handle = handle;
    upd->handle1 = node1;
    upd->handle2 = node2;
    cur_tx.emplace_back(upd);
}

void
client :: delete_node(std::string &node)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_DELETE_REQ;
    upd->handle1 = node;
    cur_tx.emplace_back(upd);
}

void
client :: delete_edge(std::string &edge, std::string &node)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_DELETE_REQ;
    upd->handle1 = edge;
    upd->handle2 = node;
    cur_tx.emplace_back(upd);
}

void
client :: set_node_property(std::string &node,
    std::string key, std::string value)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_SET_PROPERTY;
    upd->handle1 = node;
    upd->key.reset(new std::string(std::move(key)));
    upd->value.reset(new std::string(std::move(value)));
    cur_tx.emplace_back(upd);
}

void
client :: set_edge_property(std::string &node, std::string &edge,
    std::string key, std::string value)
{
    assert(cur_tx_id != UINT64_MAX);
    auto upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_SET_PROPERTY;
    upd->handle1 = edge;
    upd->handle2 = node;
    upd->key.reset(new std::string(std::move(key)));
    upd->value.reset(new std::string(std::move(value)));
    cur_tx.emplace_back(upd);
}

bool
client :: end_tx()
{
    if (cur_tx_id == UINT64_MAX) {
        WDEBUG << "no active tx to commit" << std::endl;
        return false;
    }

    bool success = false;
    message::message msg;
    msg.prepare_message(message::CLIENT_TX_INIT, cur_tx);
    send_coord(msg.buf);

    busybee_returncode recv_code = recv_coord(&msg.buf);
    if (recv_code == BUSYBEE_TIMEOUT) {
        // assume vt is dead, fail tx
        WDEBUG << "operation timeout, perhaps timestamper is dead?" << std::endl;
        success = false;
    } else if (recv_code != BUSYBEE_SUCCESS) {
        WDEBUG << "tx msg recv fail" << std::endl;
    } else {
        message::msg_type mtype = msg.unpack_message_type();
        assert(mtype == message::CLIENT_TX_SUCCESS
            || mtype == message::CLIENT_TX_ABORT);
        if (mtype == message::CLIENT_TX_SUCCESS) {
            success = true;
        }
    }

    cur_tx_id = UINT64_MAX;
    cur_tx.clear();

    return success;
}

template <typename ParamsType>
std::unique_ptr<ParamsType>
client :: run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<std::string, ParamsType>> &initial_args)
{
    message::message msg;
    msg.prepare_message(message::CLIENT_NODE_PROG_REQ, prog_to_run, initial_args);
    send_coord(msg.buf);

    busybee_returncode recv_code = recv_coord(&msg.buf);
    if (recv_code == BUSYBEE_TIMEOUT) {
        // assume vt is dead
        //reconfigure();
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
client :: run_reach_program(std::vector<std::pair<std::string, node_prog::reach_params>> &initial_args)
{
    return *run_node_program(node_prog::REACHABILITY, initial_args);
}

node_prog::pathless_reach_params
client :: run_pathless_reach_program(std::vector<std::pair<std::string, node_prog::pathless_reach_params>> &initial_args)
{
    return *run_node_program(node_prog::PATHLESS_REACHABILITY, initial_args);
}

node_prog::clustering_params
client :: run_clustering_program(std::vector<std::pair<std::string, node_prog::clustering_params>> &initial_args)
{
    return *run_node_program(node_prog::CLUSTERING, initial_args);
}

node_prog::two_neighborhood_params
client :: run_two_neighborhood_program(std::vector<std::pair<std::string, node_prog::two_neighborhood_params>> &initial_args)
{
    return *run_node_program(node_prog::TWO_NEIGHBORHOOD, initial_args);
}

/*
node_prog::dijkstra_params
client :: run_dijkstra_program(std::vector<std::pair<std::string, node_prog::dijkstra_params>> initial_args)
{
    return *run_node_program(node_prog::DIJKSTRA, initial_args);
}
*/

node_prog::read_node_props_params
client :: read_node_props_program(std::vector<std::pair<std::string, node_prog::read_node_props_params>> &initial_args)
{
    return *run_node_program(node_prog::READ_NODE_PROPS, initial_args);
}

node_prog::read_edges_props_params
client :: read_edges_props_program(std::vector<std::pair<std::string, node_prog::read_edges_props_params>> &initial_args)
{
    return *run_node_program(node_prog::READ_EDGES_PROPS, initial_args);
}

node_prog::read_n_edges_params
client :: read_n_edges_program(std::vector<std::pair<std::string, node_prog::read_n_edges_params>> &initial_args)
{
    return *run_node_program(node_prog::READ_N_EDGES, initial_args);
}

node_prog::edge_count_params
client :: edge_count_program(std::vector<std::pair<std::string, node_prog::edge_count_params>> &initial_args)
{
    return *run_node_program(node_prog::EDGE_COUNT, initial_args);
}

node_prog::edge_get_params
client :: edge_get_program(std::vector<std::pair<std::string, node_prog::edge_get_params>> &initial_args)
{
    return *run_node_program(node_prog::EDGE_GET, initial_args);
}

node_prog::traverse_props_params
client :: traverse_props_program(std::vector<std::pair<std::string, node_prog::traverse_props_params>> &initial_args)
{
    return *run_node_program(node_prog::TRAVERSE_PROPS, initial_args);
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
    if (comm->send(vtid, buf) != BUSYBEE_SUCCESS) {
        return;
    }
}

busybee_returncode
client :: recv_coord(std::auto_ptr<e::buffer> *buf)
{
    busybee_returncode ret;
    while (true) {
        comm->quiesce_thread(0);
        ret = comm->recv(buf);
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

bool
client :: maintain_sm_connection()
{
    replicant_returncode rc;

    if (!m_sm.ensure_configuration(&rc))
    {
        if (rc == REPLICANT_INTERRUPTED)
        {
            WDEBUG << "signal received";
        }
        else if (rc == REPLICANT_TIMEOUT)
        {
            WDEBUG << "operation timed out";
        }
        else
        {
            WDEBUG << "coordinator failure: " << m_sm.error().msg();
        }

        return false;
    }

    return true;
}
