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
#include "common/message.h"
#include "common/message_tx_client.h"
#include "common/property.h"
#include "transaction.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"

namespace client
{
    class client
    {
        public:
            client(uint64_t my_id, uint64_t vt_id);
            ~client();

        private:
            uint64_t myid, shifted_id, vtid;
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *client_bb;
            std::unordered_map<uint64_t, tx_list_t> tx_map;
            uint64_t tx_id_ctr, temp_handle_ctr;

        public:
            uint64_t begin_tx();
            uint64_t create_node(uint64_t tx_id);
            uint64_t create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2);
            void delete_node(uint64_t tx_id, uint64_t node); 
            void delete_edge(uint64_t tx_id, uint64_t edge, uint64_t node);
            void end_tx(uint64_t tx_id);

            template <typename ParamsType>
            std::unique_ptr<ParamsType> 
            run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args);
            node_prog::reach_params run_reach_program(std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args);
            node_prog::clustering_params run_clustering_program(std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args);

            void start_migration();
            void single_stream_migration();
            void commit_graph();
            void exit_weaver();
            void print_msgcount();

        private:
            void send_coord(std::auto_ptr<e::buffer> buf);
            busybee_returncode recv_coord(std::auto_ptr<e::buffer> *buf);
            uint64_t generate_handle();
    };

    inline
    client :: client(uint64_t my_id, uint64_t vt_id)
        : myid(my_id)
        , shifted_id(myid << (64-ID_BITS))
        , vtid(vt_id)
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
        tx_map.emplace(tx_id, std::vector<std::shared_ptr<pending_update>>());
        return tx_id;
    }

    inline uint64_t
    client :: create_node(uint64_t tx_id)
    {
        if (tx_map.find(tx_id) == tx_map.end()) {
            return 0;
        } else {
            auto upd = std::make_shared<pending_update>();
            upd->type = message::CLIENT_NODE_CREATE_REQ;
            upd->handle = generate_handle();
            tx_map.at(tx_id).push_back(upd);
            return upd->handle;
        }
    }

    inline uint64_t
    client :: create_edge(uint64_t tx_id, uint64_t node1, uint64_t node2)
    {
        if (tx_map.find(tx_id) == tx_map.end()) {
            return 0;
        } else {
            auto upd = std::make_shared<pending_update>();
            upd->type = message::CLIENT_EDGE_CREATE_REQ;
            upd->handle = generate_handle();
            upd->elem1 = node1;
            upd->elem2 = node2;
            tx_map.at(tx_id).push_back(upd);
            return upd->handle;
        }
    }

    inline void
    client :: delete_node(uint64_t tx_id, uint64_t node)
    {
        if (tx_map.find(tx_id) != tx_map.end()) {
            auto upd = std::make_shared<pending_update>();
            upd->type = message::CLIENT_NODE_DELETE_REQ;
            upd->elem1 = node;
            tx_map.at(tx_id).push_back(upd);
        }
    }

    inline void
    client :: delete_edge(uint64_t tx_id, uint64_t edge, uint64_t node)
    {
        if (tx_map.find(tx_id) != tx_map.end()) {
            auto upd = std::make_shared<pending_update>();
            upd->type = message::CLIENT_EDGE_DELETE_REQ;
            upd->elem1 = edge;
            upd->elem2 = node;
            tx_map.at(tx_id).push_back(upd);
        }
    }

    inline void
    client :: end_tx(uint64_t tx_id)
    {
        if (tx_map.find(tx_id) != tx_map.end()) {
            message::message msg;
            message::prepare_tx_message_client(msg, tx_map.at(tx_id));
            send_coord(msg.buf);
            if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
                WDEBUG << "tx msg recv fail" << std::endl;
            }
            uint32_t mtype;
            msg.buf->unpack_from(BUSYBEE_HEADER_SIZE) >> mtype;
            assert(mtype == message::CLIENT_TX_DONE);
        }
    }

    template <typename ParamsType>
    inline std::unique_ptr<ParamsType>
    client :: run_node_program(node_prog::prog_type prog_to_run, std::vector<std::pair<uint64_t, ParamsType>> initial_args)
    {
        message::message msg;
        message::prepare_message(msg, message::CLIENT_NODE_PROG_REQ, prog_to_run, initial_args);
        send_coord(msg.buf);
        if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
            WDEBUG << "node prog return msg fail" << std::endl;
            return NULL;
        }

        uint64_t ignore_req_id;
        node_prog::prog_type ignore_type;
        std::pair<uint64_t, ParamsType> tempPair;
        message::unpack_message(msg, message::NODE_PROG_RETURN, ignore_type, ignore_req_id, tempPair);
// XXX fix this crap, why not unpack into unique_ptr?
        std::unique_ptr<ParamsType> toRet(new ParamsType(tempPair.second));
        return std::move(toRet);
    }

    inline node_prog::reach_params
    client :: run_reach_program(std::vector<std::pair<uint64_t, node_prog::reach_params>> initial_args)
    {
        return *run_node_program(node_prog::REACHABILITY, initial_args);
    }

    inline node_prog::clustering_params
    client :: run_clustering_program(std::vector<std::pair<uint64_t, node_prog::clustering_params>> initial_args)
    {
        return *run_node_program(node_prog::CLUSTERING, initial_args);
    }


    inline void
    client :: start_migration()
    {
        message::message msg;
        message::prepare_message(msg, message::START_MIGR);
        send_coord(msg.buf);
    }

    inline void
    client :: single_stream_migration()
    {
        message::message msg;
        message::prepare_message(msg, message::ONE_STREAM_MIGR);
        send_coord(msg.buf);

        if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
            WDEBUG << "single stream migration return msg fail" << std::endl;
        }
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
    client :: print_msgcount()
    {
        message::message msg;
        message::prepare_message(msg, message::CLIENT_MSG_COUNT);
        send_coord(msg.buf);
    }

    inline void
    client :: send_coord(std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = client_bb->send(vtid, buf)) != BUSYBEE_SUCCESS) {
            WDEBUG << "msg send error: " << ret << std::endl;
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
                    WDEBUG << "msg recv error: " << ret << std::endl;
                    return ret;
            }
        }
    }

    // to generate 64 bit graph element handles
    // assuming no more than 2^(ID_BITS) clients
    // assuming no more than 2^(64-ID_BITS) graph nodes and edges created at this client
    // TODO shift to 128 bit handles? or more client id bits
    inline uint64_t
    client :: generate_handle()
    {
        uint64_t new_handle = (++temp_handle_ctr) & TOP_MASK;
        new_handle |= shifted_id;
        return new_handle;
    }

}

#endif
