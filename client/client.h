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

#ifndef __CLIENT__
#define __CLIENT__

#include <fstream>
#include <unordered_map>
#include <po6/net/location.h>

#include "common/weaver_constants.h"
#include "common/busybee_infra.h"
#include "common/message_tx_client.h"
#include "common/property.h"
#include "transaction.h"
#include "node_prog/node_prog_type.h"

class client
{
    public:
        client(uint64_t my_id);
        ~client();

    private:
        uint64_t myid;
        std::shared_ptr<po6::net::location> myloc;
        busybee_mta *client_bb;
        std::unordered_map<uint64_t, client::tx_list_t> tx_map;
        uint64_t tx_id_ctr, temp_handle_ctr;

    public:
        uint64_t begin_tx();
        uint64_t create_node(uint64_t tx_id);
        uint64_t create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2);
        void delete_node(uint64_t tx_id, uint64_t node); 
        void delete_edge(uint64_t tx_id, uint64_t node, uint64_t edge);
        std::vector<uint64_t> end_tx();
        void commit_graph();
        void exit_weaver();

    private:
        void send_coord(std::auto_ptr<e::buffer> buf);
        busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
};

inline
client :: client(uint64_t my_id)
    : myid(my_id)
    , tx_id_ctr(0)
    , temp_handle_ctr(0)
{
    initialize_busybee(client_bb, myid, myloc);
}

inline
client :: ~client()
{
    delete client_bb;
}

inline uint64_t
client :: begin_tx()
{
    uint64_t tx_id = ++tx_id_ctr;
    tx_map.insert(tx_id, client::tx_list_t());
}

inline uint64_t
client :: create_node(uint64_t tx_id)
{
    if (tx_map.find(tx_id) == tx_map.end()) {
        return 0;
    } else {
        uint64_t new_node = ++temp_handle_ctr;
        auto upd = std::make_shared<client::pending_update>();
        upd->type = message::CLIENT_NODE_CREATE_REQ;
        upd->temp_handle = new_node;
        tx_map.at(tx_id).push_back(upd);
        return new_node;
    }
}

inline uint64_t
client :: create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2)
{
    if (tx_map.find(tx_id) == tx_map.end()) {
        return 0;
    } else {
        uint64_t new_edge = ++temp_handle_ctr;
        auto upd = std::make_shared<client::pending_update>();
        upd->type = message::CLIENT_EDGE_CREATE_REQ;
        upd->elem1 = node1;
        upd->elem2 = node2;
        upd->temp_handle = new_edge;
        tx_map.at(tx_id).push_back(upd);
        return new_edge;
    }
}

inline void
client :: delete_node(uint64_t tx_id, uint64_t node)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<client::pending_update>();
        upd->type = message::CLIENT_NODE_DELETE_REQ;
        upd->elem1 = node;
        upd->temp_handle = 0;
        tx_map.at(tx_id).push_back(upd);
    }
}

inline void
client :: delete_edge(uint64_t tx_id, uint64_t edge)
{
    if (tx_map.find(tx_id) != tx_map.end()) {
        auto upd = std::make_shared<client::pending_update>();
        upd->type = message::CLIENT_EDGE_DELETE_REQ;
        upd->elem1 = edge;
        upd->temp_handle = 0;
        tx_map.at(tx_id).push_back(upd);
    }
}

inline std::vector<uint64_t>
client :: end_tx(uint64_t tx_id)
{
    std::vector<uint64_t> ret;
    if (tx_map.find(tx_id) != tx_map.end()) {
        message::message msg;
        message::prepare_tx_message_client(msg, tx_map.at(tx_id));
        send_coord(msg.buf);
        if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
            return ret;
        }
        message::unpack_message(msg, message::CLIENT_TX_DONE, ret);
    }
    return ret;
}

inline void
client :: commit_graph()
{
    message::message msg;
    message::prepare_message(msg, message::CLIENT_COMMIT_GRAPH);
    send_coord(msg.buf);
}


inline void
client :: exit_weaver()
{
    message::message msg;
    message::prepare_message(msg, message::EXIT_WEAVER);
    send_coord(msg.buf);
}

inline void
client :: send_coord(std::auto_ptr<e::buffer> buf)
{
    busybee_returncode ret;
    if ((ret = client_bb->send(COORD_ID, buf)) != BUSYBEE_SUCCESS) {
        DEBUG << "msg send error: " << ret << std::endl;
        return;
    }
}

inline busybee_returncode
client :: recv_coord(std::auto_ptr<e::buffer> *buf)
{
    busybee_returncode ret;
    uint64_t sender;
    while (true) {
        ret = client_bb->recv(&sender, buf);
        switch (ret) {
            case BUSYBEE_SUCCESS:
                return ret;

            case BUSYBEE_INTERRUPTED:
                continue;

            default:
                DEBUG << "msg recv error: " << ret << std::endl;
                return ret;
        }
    }
}

#endif // __CLIENT__
