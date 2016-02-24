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

#include <random>

#include "common/message.h"
#include "common/config_constants.h"
#include "client/client_constants.h"
#include "client/client.h"

using node_prog::Node_Parameters_Base;

#define CLIENTLOG std::cerr << __FILE__ << ":" << __LINE__ << " "

// ugly, declared only to get rid of undefined symbol error
// not used in client
DECLARE_CONFIG_CONSTANTS;

using cl::client;
using transaction::pending_update;

client :: client(const char *coordinator="127.0.0.1", uint16_t port=5200, const char *config_file="/etc/weaver.yaml")
    : m_sm(coordinator, port)
    , cur_tx_id(UINT64_MAX)
    , tx_id_ctr(0)
    , handle_ctr(0)
    , init(true)
    , logging(false)
{
    if (!init_config_constants(config_file)) {
        std::cerr << "weaver_client: error in init_config_constants, config file=" << config_file << std::endl;
        init = false;
        std::string except_message = "could not initialize configuration constants from file ";
        except_message += config_file;
        throw weaver_client_exception(except_message);
    }

    std::random_device rd;
    std::mt19937_64 generator(rd());
    std::uniform_int_distribution<uint64_t> distribution(0, NumVts-1);
    vtid = distribution(generator);

    if (!m_sm.get_unique_number(myid)) {
        init = false;
        std::string except_message = "could not contact Weaver server manager";
        throw weaver_client_exception(except_message);
    }
    if (myid <= MaxNumServers) {
        init = false;
        std::string except_message = "internal error in initialization, myid=" + std::to_string(myid);
        throw weaver_client_exception(except_message);
    }
    myid_str = std::to_string(myid);

    int try_sm = 0;
    replicant_returncode rc;
    while (!maintain_sm_connection(rc)) {
        std::cerr << "weaver_client: retry server manager connection #" << try_sm++ << std::endl;
        init = false;
        if (try_sm == 10) {
            init = false;
            std::string except_message = "multiple server manager connection attempts failed";
            throw weaver_client_exception(except_message);
        }
    }

    comm.reset(new cl::comm_wrapper(myid, *m_sm.config()));
}

// call once per application, even with multiple clients
void
client :: initialize_logging()
{
    logging = true;
}

#define CHECK_INIT \
    if (!init) { \
        return WEAVER_CLIENT_INITERROR; \
    }

#define CHECK_ACTIVE_TX \
    if (cur_tx_id == UINT64_MAX) { \
        return WEAVER_CLIENT_NOACTIVETX; \
    }

#define CHECK_AUX_INDEX \
    if (!AuxIndex) { \
        return WEAVER_CLIENT_NOAUXINDEX; \
    }

weaver_client_returncode
client :: fail_tx(weaver_client_returncode code)
{
    cur_tx_id = UINT64_MAX;
    cur_tx.clear();
    return code;
}

weaver_client_returncode
client :: begin_tx()
{
    CHECK_INIT;

    if (cur_tx_id != UINT64_MAX) {
        return WEAVER_CLIENT_ACTIVETX;
    } else {
        cur_tx_id = ++tx_id_ctr;
        return WEAVER_CLIENT_SUCCESS;
    }
}

void
client :: print_cur_tx()
{
    if (!logging) {
        return;
    }

    CLIENTLOG << "Current transaction details:" << std::endl;
    for (auto upd: cur_tx) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                CLIENTLOG << "NODE CREATE" << std::endl;
                CLIENTLOG << "\thandle = " << upd->handle <<  std::endl;
                break;

            case transaction::EDGE_CREATE_REQ:
                CLIENTLOG << "EDGE CREATE" << std::endl;
                CLIENTLOG << "\thandle = " << upd->handle;
                CLIENTLOG << "\tstart node,alias = " << upd->handle1 << "," << upd->alias1
                       << " end node,alias = " << upd->handle2 << "," << upd->alias2 << std::endl;
                break;

            case transaction::NODE_DELETE_REQ:
                CLIENTLOG << "NODE DELETE" << std::endl;
                CLIENTLOG << "\tnode,alias = " << upd->handle1 << "," << upd->alias1 << std::endl;
                break;

            case transaction::EDGE_DELETE_REQ:
                CLIENTLOG << "EDGE DELETE" << std::endl;
                CLIENTLOG << "\tedge = " << upd->handle1 << std::endl;
                CLIENTLOG << "\tnode,alias = " << upd->handle2 << "," << upd->alias2 << std::endl;
                break;

            case transaction::NODE_SET_PROPERTY:
                CLIENTLOG << "NODE SET PROPERTY" << std::endl;
                CLIENTLOG << "\tnode,alias = " << upd->handle1 << "," << upd->alias1 << std::endl;
                CLIENTLOG << "\tkey,value = " << *upd->key << "," << *upd->value << std::endl;
                break;

            case transaction::EDGE_SET_PROPERTY:
                CLIENTLOG << "EDGE SET PROPERTY" << std::endl;
                CLIENTLOG << "\tedge = " << upd->handle1 << std::endl;
                CLIENTLOG << "\tnode,alias = " << upd->handle2 << "," << upd->alias2 << std::endl;
                CLIENTLOG << "\tkey,value = " << *upd->key << "," << *upd->value << std::endl;
                break;

            case transaction::ADD_AUX_INDEX:
                CLIENTLOG << "ADD ALIAS" << std::endl;
                CLIENTLOG << "\tnode,alias = " << upd->handle1 << "," << upd->handle << std::endl;
                break;
        }
    }
}

weaver_client_returncode
client :: create_node(std::string &handle, const std::vector<std::string> &aliases)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_CREATE_REQ;
    if (handle == "") {
        handle = generate_handle();
    }
    upd->handle = handle;
    cur_tx.emplace_back(upd);

    for (const std::string &a: aliases) {
        add_alias(a, upd->handle);
    }

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: create_edge(std::string &handle, const std::string &node1, const std::string &node1_alias, const std::string &node2, const std::string &node2_alias)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_CREATE_REQ;
    if (handle == "") {
        handle = generate_handle();
    }
    upd->handle = handle;
    upd->handle1 = node1;
    upd->handle2 = node2;
    upd->alias1 = node1_alias;
    upd->alias2 = node2_alias;
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: delete_node(const std::string &node, const std::string &alias)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_DELETE_REQ;
    upd->handle1 = node;
    upd->alias1 = alias;
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: delete_edge(const std::string &edge, const std::string &node, const std::string &alias)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_DELETE_REQ;
    upd->handle1 = edge;
    upd->handle2 = node;
    upd->alias2 = alias;
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: set_node_property(const std::string &node, const std::string &alias, std::string key, std::string value)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::NODE_SET_PROPERTY;
    upd->handle1 = node;
    upd->alias1 = alias;
    upd->key.reset(new std::string(std::move(key)));
    upd->value.reset(new std::string(std::move(value)));
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: set_edge_property(const std::string &node, const std::string &alias, const std::string &edge,
    std::string key, std::string value)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::EDGE_SET_PROPERTY;
    upd->handle1 = edge;
    upd->handle2 = node;
    upd->alias2 = alias;
    upd->key.reset(new std::string(std::move(key)));
    upd->value.reset(new std::string(std::move(value)));
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: add_alias(const std::string &alias, const std::string &node)
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;
    CHECK_AUX_INDEX;

    std::shared_ptr<pending_update> upd = std::make_shared<pending_update>();
    upd->type = transaction::ADD_AUX_INDEX;
    upd->handle = alias;
    upd->handle1 = node;
    cur_tx.emplace_back(upd);

    return WEAVER_CLIENT_SUCCESS;
}

#undef CHECK_AUX_INDEX

weaver_client_returncode
client :: end_tx()
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    bool retry;
    bool success;
    weaver_client_returncode tx_code = WEAVER_CLIENT_SUCCESS;
    message::message recv_msg;
    // currently no retry on timeout/disrupted, pass error to client
    // so it is responsibility of client to ensure that they do not reexec tx that was completed
    do {
        message::message msg;
        msg.prepare_message(message::CLIENT_TX_INIT, nullptr, cur_tx_id, cur_tx);
        busybee_returncode send_code = send_coord(msg.buf);

        if (send_code == BUSYBEE_DISRUPTED) {
            reconfigure();
            return fail_tx(WEAVER_CLIENT_DISRUPTED);
        } else if (send_code != BUSYBEE_SUCCESS) {
            return fail_tx(WEAVER_CLIENT_INTERNALMSGERROR);
        }

        busybee_returncode recv_code = recv_coord(&recv_msg.buf);

        switch (recv_code) {
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_DISRUPTED:
                reconfigure();
                return fail_tx(WEAVER_CLIENT_DISRUPTED);
            break;

            case BUSYBEE_SUCCESS:
                success = true;
                retry = false;
            break;

            default:
                success = false;
                retry = false;
        }

    } while (retry);

    if (success) {
        message::msg_type mtype = recv_msg.unpack_message_type();
        assert(mtype == message::CLIENT_TX_SUCCESS || mtype == message::CLIENT_TX_ABORT);
        if (mtype == message::CLIENT_TX_ABORT) {
            success = false;
            tx_code = WEAVER_CLIENT_ABORT;
        }
    }

    if (!success) {
        print_cur_tx();
    }

    cur_tx_id = UINT64_MAX;
    cur_tx.clear();

    return tx_code;
}

weaver_client_returncode
client :: abort_tx()
{
    CHECK_INIT;
    CHECK_ACTIVE_TX;

    cur_tx_id = UINT64_MAX;
    cur_tx.clear();

    return WEAVER_CLIENT_SUCCESS;
}

#undef CHECK_ACTIVE_TX

weaver_client_returncode
client :: run_node_prog(uint64_t prog_type,
                        std::vector<std::pair<std::string, std::shared_ptr<Node_Parameters_Base>>> &initial_args,
                        std::shared_ptr<Node_Parameters_Base> &return_param)
{
    CHECK_INIT;

    message::message msg;
    busybee_returncode send_code, recv_code;

#ifdef weaver_benchmark_

    // XXX fix nullptr
    msg.prepare_message(message::CLIENT_NODE_PROG_REQ, nullptr, prog_type, initial_args);
    send_code = send_coord(msg.buf);

    if (send_code != BUSYBEE_SUCCESS) {
        return WEAVER_CLIENT_INTERNALMSGERROR;
    }

    recv_code = recv_coord(&msg.buf);

    if (recv_code != BUSYBEE_SUCCESS) {
        return WEAVER_CLIENT_INTERNALMSGERROR;
    }

#else

    bool retry;
    do {
        // XXX fix nullptr
        msg.prepare_message(message::CLIENT_NODE_PROG_REQ, nullptr, prog_type, initial_args);
        send_code = send_coord(msg.buf);

        if (send_code == BUSYBEE_DISRUPTED) {
            reconfigure();
            return WEAVER_CLIENT_DISRUPTED;
        } else if (send_code != BUSYBEE_SUCCESS) {
            return WEAVER_CLIENT_INTERNALMSGERROR;
        }

        recv_code = recv_coord(&msg.buf);

        switch (recv_code) {
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_DISRUPTED:
                reconfigure();
                retry = true;
                break;

            case BUSYBEE_SUCCESS:
                if (msg.unpack_message_type() == message::NODE_PROG_RETRY) {
                    retry = true;
                } else {
                    retry = false;
                }
                break;

            default:
                return WEAVER_CLIENT_INTERNALMSGERROR;
        }
    } while (retry);

#endif

    uint64_t ignore_req_id, ignore_vt_ptr;
    node_prog::prog_type ignore_type;
    auto ret_status = msg.unpack_message_type();
    if (ret_status == message::NODE_PROG_RETURN) {
        // XXX fix nullptr
        msg.unpack_message(message::NODE_PROG_RETURN, nullptr, ignore_type, ignore_req_id, ignore_vt_ptr, return_param);
        return WEAVER_CLIENT_SUCCESS;
    } else {
        return WEAVER_CLIENT_NOTFOUND;
    }
}

//template <typename ParamsType>
//weaver_client_returncode
//client :: run_node_program(node_prog::prog_type prog_to_run,
//                           std::vector<std::pair<std::string, std::shared_ptr<ParamsType>>> &initial_args,
//                           ParamsType &return_param)
//{
//    CHECK_INIT;
//
//    message::message msg;
//    busybee_returncode send_code, recv_code;
//
//#ifdef weaver_benchmark_
//
//    // XXX fix nullptr
//    msg.prepare_message(message::CLIENT_NODE_PROG_REQ, nullptr, prog_to_run, initial_args);
//    send_code = send_coord(msg.buf);
//
//    if (send_code != BUSYBEE_SUCCESS) {
//        return WEAVER_CLIENT_INTERNALMSGERROR;
//    }
//
//    recv_code = recv_coord(&msg.buf);
//
//    if (recv_code != BUSYBEE_SUCCESS) {
//        return WEAVER_CLIENT_INTERNALMSGERROR;
//    }
//
//#else
//
//    bool retry;
//    do {
//        // XXX fix nullptr
//        msg.prepare_message(message::CLIENT_NODE_PROG_REQ, nullptr, prog_to_run, initial_args);
//        send_code = send_coord(msg.buf);
//
//        if (send_code == BUSYBEE_DISRUPTED) {
//            reconfigure();
//            return WEAVER_CLIENT_DISRUPTED;
//        } else if (send_code != BUSYBEE_SUCCESS) {
//            return WEAVER_CLIENT_INTERNALMSGERROR;
//        }
//
//        recv_code = recv_coord(&msg.buf);
//
//        switch (recv_code) {
//            case BUSYBEE_TIMEOUT:
//            case BUSYBEE_DISRUPTED:
//                reconfigure();
//                retry = true;
//                break;
//
//            case BUSYBEE_SUCCESS:
//                if (msg.unpack_message_type() == message::NODE_PROG_RETRY) {
//                    retry = true;
//                } else {
//                    retry = false;
//                }
//                break;
//
//            default:
//                return WEAVER_CLIENT_INTERNALMSGERROR;
//        }
//    } while (retry);
//
//#endif
//
//    uint64_t ignore_req_id, ignore_vt_ptr;
//    node_prog::prog_type ignore_type;
//    auto ret_status = msg.unpack_message_type();
//    if (ret_status == message::NODE_PROG_RETURN) {
//        // XXX fix nullptr
//        std::shared_ptr<ParamsType> ret_ptr;
//        msg.unpack_message(message::NODE_PROG_RETURN, nullptr, ignore_type, ignore_req_id, ignore_vt_ptr, ret_ptr);
//        return_param = *ret_ptr;
//        return WEAVER_CLIENT_SUCCESS;
//    } else {
//        return WEAVER_CLIENT_NOTFOUND;
//    }
//}
//
#define SPECIFIC_NODE_PROG(type) \
    return run_node_program(type, ptr_args, return_param);
//
//weaver_client_returncode
//client :: traverse_props_program(std::vector<std::pair<std::string, node_prog::traverse_props_params>> &initial_args, node_prog::traverse_props_params &return_param)
//{
//    std::vector<std::pair<std::string, std::shared_ptr<node_prog::traverse_props_params>>> ptr_args;
//    for (auto &p: initial_args) {
//        if (p.second.node_props.size() != (p.second.edge_props.size()+1)) {
//            return WEAVER_CLIENT_LOGICALERROR;
//        }
//        auto param_ptr = std::make_shared<const node_prog::traverse_props_params>(p.second);
//        ptr_args.emplace_back(std::make_pair(p.first, param_ptr));
//    }
//    SPECIFIC_NODE_PROG(node_prog::TRAVERSE_PROPS);
//}
//
#undef SPECIFIC_NODE_PROG

weaver_client_returncode
client :: start_migration()
{
    CHECK_INIT;

    message::message msg;
    msg.prepare_message(message::START_MIGR);
    send_coord(msg.buf);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: single_stream_migration()
{
    CHECK_INIT;

    message::message msg;
    msg.prepare_message(message::ONE_STREAM_MIGR);
    send_coord(msg.buf);

    if (recv_coord(&msg.buf) != BUSYBEE_SUCCESS) {
        return WEAVER_CLIENT_INTERNALMSGERROR;
    }

    return WEAVER_CLIENT_SUCCESS;
}


weaver_client_returncode
client :: exit_weaver()
{
    CHECK_INIT;

    message::message msg;
    msg.prepare_message(message::EXIT_WEAVER);
    send_coord(msg.buf);

    return WEAVER_CLIENT_SUCCESS;
}

weaver_client_returncode
client :: get_node_count(std::vector<uint64_t> &node_count)
{
    CHECK_INIT;

    node_count.clear();

    while(true) {
        message::message msg;
        msg.prepare_message(message::CLIENT_NODE_COUNT);
        busybee_returncode send_code = send_coord(msg.buf);

        if (send_code == BUSYBEE_DISRUPTED) {
            reconfigure();
            continue;
        } else if (send_code != BUSYBEE_SUCCESS) {
            return WEAVER_CLIENT_INTERNALMSGERROR;
        }

        busybee_returncode recv_code = recv_coord(&msg.buf);

        switch (recv_code) {
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_TIMEOUT:
                reconfigure();
                break;

            case BUSYBEE_SUCCESS:
                msg.unpack_message(message::NODE_COUNT_REPLY, nullptr, node_count);
                return WEAVER_CLIENT_SUCCESS;

            default:
                return WEAVER_CLIENT_INTERNALMSGERROR;
        }
    }
}

#undef CHECK_INIT

bool
client :: aux_index()
{
    return AuxIndex;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
client :: send_coord(std::auto_ptr<e::buffer> buf)
{
    return comm->send(vtid, buf);
}

busybee_returncode
client :: recv_coord(std::auto_ptr<e::buffer> *buf)
{
    busybee_returncode ret;
    while (true) {
        ret = comm->recv(buf);
        switch (ret) {
            case BUSYBEE_SUCCESS:
            case BUSYBEE_TIMEOUT:
                return ret;

            case BUSYBEE_INTERRUPTED:
                continue;

            default:
                return ret;
        }
    }
}
#pragma GCC diagnostic pop

std::string
client :: generate_handle()
{
    std::string s = std::to_string(handle_ctr++);
    s += myid_str;
    return s;
}

bool
client :: maintain_sm_connection(replicant_returncode &rc)
{
    if (!m_sm.ensure_configuration(&rc))
    {

        return false;
    }

    return true;
}

void
client :: reconfigure()
{
    uint32_t try_sm = 0;
    replicant_returncode rc;

    while (!maintain_sm_connection(rc)) {
        if (logging) {
            if (rc == REPLICANT_INTERRUPTED) {
                CLIENTLOG << "signal received";
            } else if (rc == REPLICANT_TIMEOUT) {
                CLIENTLOG << "operation timed out";
            } else {
                CLIENTLOG << "coordinator failure: " << m_sm.error_message();
            }
            CLIENTLOG << "retry sm connection " << try_sm << std::endl;
        }

        try_sm++;
    }

    comm.reset(new cl::comm_wrapper(myid, *m_sm.config()));
    comm->reconfigure(*m_sm.config());
}
