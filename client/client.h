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
#include <po6/net/location.h>

#include "common/weaver_constants.h"
#include "common/busybee_infra.h"
#include "common/message.h"
#include "common/property.h"
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

    public:
        uint64_t create_node();
        uint64_t create_edge(uint64_t node1, uint64_t node2);
        void delete_node(uint64_t node); 
        void delete_edge(uint64_t node, uint64_t edge);
        void add_edge_prop(uint64_t node, uint64_t edge, uint32_t key, uint64_t value);
        void del_edge_prop(uint64_t node, uint64_t edge, uint32_t key);
        void commit_graph();
        template <typename ParamsType>
        std::unique_ptr<ParamsType> run_node_program(node_prog::prog_type prog_to_run,
            std::vector<std::pair<uint64_t, ParamsType>> initial_args);
        uint64_t get_node_loc(uint64_t node);
        void start_migration();
        void exit_weaver();

    private:
        void send_coord(std::auto_ptr<e::buffer> buf);
        busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
};

inline
client :: client(uint64_t my_id)
    : myid(my_id)
{
    initialize_busybee(client_bb, myid, myloc);
}

inline
client :: ~client()
{
    delete client_bb;
}

inline uint64_t
client :: create_node()
{
    uint64_t new_node;
    message::message msg(message::CLIENT_NODE_CREATE_REQ);
    message::prepare_message(msg, message::CLIENT_NODE_CREATE_REQ);
    send_coord(msg.buf);
    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        return 0;
    }
    message::unpack_message(msg, message::CLIENT_REPLY, new_node);
    return new_node;
}

inline uint64_t
client :: create_edge(uint64_t node1, uint64_t node2)
{
    uint64_t new_edge;
    message::message msg(message::CLIENT_EDGE_CREATE_REQ);
    message::prepare_message(msg, message::CLIENT_EDGE_CREATE_REQ, node1, node2);
    send_coord(msg.buf);
    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        return 0;
    }
    message::unpack_message(msg, message::CLIENT_REPLY, new_edge);
    return new_edge;
}

inline void
client :: delete_node(uint64_t node)
{
    message::message msg(message::CLIENT_NODE_DELETE_REQ);
    message::prepare_message(msg, message::CLIENT_NODE_DELETE_REQ, node);
    send_coord(msg.buf);
    recv_coord(&msg.buf);
}

inline void
client :: delete_edge(uint64_t node, uint64_t edge)
{
    message::message msg(message::CLIENT_EDGE_DELETE_REQ);
    message::prepare_message(msg, message::CLIENT_EDGE_DELETE_REQ, node, edge);
    send_coord(msg.buf);
    recv_coord(&msg.buf);
}

inline void
client :: add_edge_prop(uint64_t node, uint64_t edge, uint32_t key, uint64_t value)
{
    message::message msg(message::CLIENT_ADD_EDGE_PROP);
    message::prepare_message(msg, message::CLIENT_ADD_EDGE_PROP, node, edge, key, value);
    send_coord(msg.buf);
    recv_coord(&msg.buf);
}

inline void
client :: del_edge_prop(uint64_t node, uint64_t edge, uint32_t key)
{
    message::message msg(message::CLIENT_DEL_EDGE_PROP);
    message::prepare_message(msg, message::CLIENT_DEL_EDGE_PROP, node, edge, key);
    send_coord(msg.buf);
    recv_coord(&msg.buf);
}

inline void
client :: commit_graph()
{
    message::message msg;
    message::prepare_message(msg, message::CLIENT_COMMIT_GRAPH);
    send_coord(msg.buf);
}

// client needs to delete the pointer returned
template <typename ParamsType>
inline std::unique_ptr<ParamsType>
client :: run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args)
{
    message::message msg;
    message::prepare_message(msg, message::CLIENT_NODE_PROG_REQ, prog_to_run, initial_args);
    send_coord(msg.buf);
    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        return NULL;
    }
    uint64_t ignore_req_id;
    node_prog::prog_type ignore;
    std::vector<uint64_t> ignore_cache;
    std::pair<uint64_t, ParamsType> tempPair;
    message::unpack_message(msg, message::NODE_PROG, ignore, ignore_req_id, ignore_cache, tempPair);

    std::unique_ptr<ParamsType> toRet(new ParamsType(tempPair.second)); // make sure client frees
    return std::move(toRet);
}

// caution: node loc may change as time goes on, the location returned
// is only correct as of the time the request is processed at the coordinator
inline uint64_t
client :: get_node_loc(uint64_t node)
{
    message::message msg;
    uint64_t loc = (uint64_t)(-1);
    message::prepare_message(msg, message::CLIENT_NODE_LOC_REQ, node);
    send_coord(msg.buf);
    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        return loc;
    }
    message::unpack_message(msg, message::CLIENT_NODE_LOC_REPLY, loc);
    return loc;
}

inline void
client :: start_migration()
{
    message::message msg;
    message::prepare_message(msg, message::START_MIGR);
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
