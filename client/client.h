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

#include <po6/net/location.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/message.h"
#include "common/property.h"
#include "node_prog/node_prog_type.h"

class client
{
    public:
        client(uint16_t myport);

    private:
        po6::net::location myloc, myrecloc, coord_loc;
        busybee_sta client_bb;

    public:
        size_t create_node();
        size_t create_edge(size_t node1, size_t node2);
        void delete_node(size_t node); 
        void delete_edge(size_t node, size_t edge);
        void add_edge_prop(size_t node, size_t edge, uint32_t key, size_t value);
        void del_edge_prop(size_t node, size_t edge, uint32_t key);
        bool reachability_request(size_t node1, size_t node2, std::shared_ptr<std::vector<common::property>> edge_props);
        std::pair<size_t, std::vector<std::pair<size_t, size_t>>> shortest_path_request(size_t node1, size_t node2, uint32_t edge_weight_prop,
                std::shared_ptr<std::vector<common::property>> edge_props);
        std::pair<size_t, std::vector<std::pair<size_t, size_t>>> widest_path_request(size_t node1, size_t node2, uint32_t edge_weight_prop, 
                std::shared_ptr<std::vector<common::property>> edge_props);
        double local_clustering_coefficient(size_t node,
                std::shared_ptr<std::vector<common::property>> edge_props);

        template <typename ParamsType>
        ParamsType * run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args);

    private:
        std::pair<size_t, std::vector<std::pair<size_t, size_t>>> dijkstra_request(size_t node1, size_t node2, uint32_t edge_weight_prop, bool is_widest,
                std::shared_ptr<std::vector<common::property>> edge_props);
        void send_coord(std::auto_ptr<e::buffer> buf);
};

inline
client :: client(uint16_t myport)
    : myloc(CLIENT_IPADDR, myport)
    , myrecloc(CLIENT_IPADDR, myport)
    , coord_loc(COORD_IPADDR, COORD_CLIENT_REC_PORT)
    , client_bb(myloc.address, myloc.port, 0)
{
}

inline size_t
client :: create_node()
{
    busybee_returncode ret;
    size_t new_node;
    message::message msg(message::CLIENT_NODE_CREATE_REQ);
    message::prepare_message(msg, message::CLIENT_NODE_CREATE_REQ, myloc.port);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return 0;
    }
    message::unpack_message(msg, message::CLIENT_REPLY, new_node);
    return new_node;
}

inline size_t
client :: create_edge(size_t node1, size_t node2)
{
    busybee_returncode ret;
    size_t new_edge;
    message::message msg(message::CLIENT_EDGE_CREATE_REQ);
    message::prepare_message(msg, message::CLIENT_EDGE_CREATE_REQ, myloc.port,
            node1, node2);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return 0;
    }
    message::unpack_message(msg, message::CLIENT_REPLY, new_edge);
    return new_edge;
}

inline void
client :: delete_node(size_t node)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_NODE_DELETE_REQ);
    message::prepare_message(msg, message::CLIENT_NODE_DELETE_REQ, myloc.port, node);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
    }
}

inline void
client :: delete_edge(size_t node, size_t edge)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_EDGE_DELETE_REQ);
    message::prepare_message(msg, message::CLIENT_EDGE_DELETE_REQ, myloc.port,
            node, edge);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
    }
}

inline void
client :: add_edge_prop(size_t node, size_t edge, uint32_t key, size_t value)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_ADD_EDGE_PROP);
    message::prepare_message(msg, message::CLIENT_ADD_EDGE_PROP, myloc.port, node, edge, key, value);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
    }
}

inline void
client :: del_edge_prop(size_t node, size_t edge, uint32_t key)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_DEL_EDGE_PROP);
    message::prepare_message(msg, message::CLIENT_DEL_EDGE_PROP, myloc.port, node, edge, key);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
    }
}

inline bool
client :: reachability_request(size_t node1, size_t node2,
    std::shared_ptr<std::vector<common::property>> edge_props)
{
    busybee_returncode ret;
    bool reachable;
    message::message msg(message::CLIENT_REACHABLE_REQ);
    message::prepare_message(msg, message::CLIENT_REACHABLE_REQ, myloc.port, node1, node2, *edge_props);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return false;
    }
    message::unpack_message(msg, message::CLIENT_REPLY, reachable);
    return reachable;
}

inline std::pair<size_t, std::vector<std::pair<size_t, size_t>>>
client :: shortest_path_request(size_t node1, size_t node2, 
    uint32_t edge_weight_prop, std::shared_ptr<std::vector<common::property>> edge_props)
{
    return dijkstra_request(node1, node2, edge_weight_prop, false, edge_props);
}

inline std::pair<size_t, std::vector<std::pair<size_t, size_t>>>
client :: widest_path_request(size_t node1, size_t node2,
    uint32_t edge_weight_prop, std::shared_ptr<std::vector<common::property>> edge_props)
{
    return dijkstra_request(node1, node2, edge_weight_prop, true, edge_props);
}

inline std::pair<size_t, std::vector<std::pair<size_t, size_t>>>
client :: dijkstra_request(size_t node1, size_t node2, uint32_t edge_weight_prop, bool is_widest,
    std::shared_ptr<std::vector<common::property>> edge_props)
{
    busybee_returncode ret;
    std::pair<size_t, std::vector<std::pair<size_t, size_t>>> toRet; // pair of cost and path
    message::message msg(message::CLIENT_DIJKSTRA_REQ);
    message::prepare_message(msg, message::CLIENT_DIJKSTRA_REQ, myloc.port, node1, node2, edge_weight_prop, is_widest, *edge_props);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        toRet.first = 0;
        return toRet;
    }
    message::unpack_message(msg, message::CLIENT_DIJKSTRA_REPLY,
            toRet.first, toRet.second);
    return toRet;
}

inline double
client :: local_clustering_coefficient(size_t node, std::shared_ptr<std::vector<common::property>> edge_props)
{
    busybee_returncode ret;
    size_t numerator;
    size_t denominator;
    message::message msg(message::CLIENT_CLUSTERING_REQ);
    message::prepare_message(msg, message::CLIENT_CLUSTERING_REQ, myloc.port, node, *edge_props);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return false;
    }
    message::unpack_message(msg, message::CLIENT_CLUSTERING_REPLY, numerator, denominator);
    if (denominator == 0) {
        std::cerr << "not possible to compute clustering coefficient: less than two valid neighbors" << std::endl;
        return 0;
    }
    else {
        std::cerr << "Client got " << numerator << " over " << denominator << std::endl;
        return (double) numerator/ denominator;
    }
}

template <typename ParamsType>
inline ParamsType *
client :: run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_NODE_PROG_REQ);
    //std::cout << "client sent type " << prog_to_run << std::endl;
    message::prepare_message(msg, message::CLIENT_NODE_PROG_REQ, myloc.port, prog_to_run, initial_args);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return NULL;
    }
    uint64_t ignore_req_id;
    node_prog::prog_type ignore;
    std::vector<uint64_t> ignore_cache;
    std::pair<uint64_t, ParamsType> tempPair;
    message::unpack_message(msg, message::NODE_PROG, ignore, ignore_req_id, ignore_cache, tempPair);

    ParamsType *toRet = new ParamsType(tempPair.second); // make sure client frees
    return toRet;
}

inline void
client :: send_coord(std::auto_ptr<e::buffer> buf)
{
    busybee_returncode ret;
    if ((ret = client_bb.send(coord_loc, buf)) != BUSYBEE_SUCCESS) {
        std::cerr << "msg recv error: " << ret << std::endl;
        return;
    }
}

#endif // __CLIENT__
