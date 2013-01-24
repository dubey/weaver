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
        bool reachability_request(size_t node1, size_t node2);

    private:
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
    uint16_t dummy;
    message::message msg(message::CLIENT_NODE_CREATE_REQ);
    msg.prep_client0(myloc.port);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return 0;
    }
    msg.unpack_client1(&dummy, &new_node);
    return new_node;
}

inline size_t
client :: create_edge(size_t node1, size_t node2)
{
    busybee_returncode ret;
    size_t new_edge;
    uint16_t dummy;
    message::message msg(message::CLIENT_EDGE_CREATE_REQ);
    msg.prep_client2(myloc.port, node1, node2);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return 0;
    }
    msg.unpack_client1(&dummy, &new_edge);
    return new_edge;
}

inline void
client :: delete_node(size_t node)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_NODE_DELETE_REQ);
    msg.prep_client1(myloc.port, node);
    send_coord(msg.buf);
}

inline void
client :: delete_edge(size_t node, size_t edge)
{
    busybee_returncode ret;
    message::message msg(message::CLIENT_EDGE_DELETE_REQ);
    msg.prep_client2(myloc.port, node, edge);
    send_coord(msg.buf);
}

inline bool
client :: reachability_request(size_t node1, size_t node2)
{
    busybee_returncode ret;
    bool reachable;
    message::message msg(message::CLIENT_REACHABLE_REQ);
    msg.prep_client2(myloc.port, node1, node2);
    send_coord(msg.buf);
    if ((ret = client_bb.recv(&myrecloc, &msg.buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return false;
    }
    msg.unpack_client_rr_reply(&reachable);
    return reachable;
}

inline void
client :: send_coord(std::auto_ptr<e::buffer> buf)
{
    busybee_returncode ret;
    if ((ret = client_bb.send(coord_loc, buf)) != BUSYBEE_SUCCESS)
    {
        std::cerr << "msg recv error: " << ret << std::endl;
        return;
    }
}

#endif // __CLIENT__
