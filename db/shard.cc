/*
 * ===============================================================
 *    Description:  Core database functionality for a shard server
 *
 *        Created:  07/25/2013 04:02:37 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Greg Hill, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <deque>
#include <fstream>
#include <string>
#include <random>
#include <signal.h>
#include <e/popt.h>
#include <e/buffer.h>
#include <pugixml.hpp>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/event_order.h"
#include "common/clock.h"
#include "db/shard.h"
#include "db/nop_data.h"
#include "db/message_wrapper.h"
#include "db/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "node_prog/base_classes.h"

DECLARE_CONFIG_CONSTANTS;

using db::node_version_t;

// global static variables
static uint64_t shard_id;
// shard pointer for shard.cc
static db::shard *S;

void migrated_nbr_update(std::unique_ptr<message::message> msg);
bool migrate_node_step1(db::node*, std::vector<uint64_t>&, uint64_t);
void migrate_node_step2_req();
void migrate_node_step2_resp(std::unique_ptr<message::message> msg, order::oracle *time_oracle);
bool check_step3();
void migrate_node_step3();
void migration_begin();
void migration_wrapper();
void migration_end();

void
end_program(int param)
{
    WDEBUG << "Ending program, param = " << param << std::endl;
    WDEBUG << "watch_set lookups originated from this shard " << S->watch_set_lookups << std::endl;
    WDEBUG << "watch_set nops originated from this shard " << S->watch_set_nops << std::endl;
    WDEBUG << "watch set piggybacks on this shard " << S->watch_set_piggybacks << std::endl;
    if (param == SIGINT) {
        // TODO proper shutdown
        //S->exit_mutex.lock();
        //S->to_exit = true;
        //S->exit_mutex.unlock();
        exit(0);
    } else {
        WDEBUG << "Not SIGINT, exiting immediately." << std::endl;
        exit(0);
    }
}


// parse the string 'line' as a uint64_t starting at index 'idx' till the first whitespace or end of string
// store result in 'n'
// if overflow occurs or unexpected char encountered, store true in 'bad'
inline void
parse_single_uint64(std::string &line, size_t &idx, uint64_t &n, bool &bad)
{
    uint64_t next_digit;
    static uint64_t zero = '0';
    static uint64_t max64_div10 = UINT64_MAX / 10;
    n = 0;
    while (line[idx] != ' '
        && line[idx] != '\t'
        && line[idx] != '\r'
        && line[idx] != '\n'
        && idx < line.length()) {
        next_digit = line[idx] - zero;
        if (next_digit > 9) { // unexpected char
            bad = true;
            WDEBUG << "Unexpected char with ascii " << (int)line[idx]
                   << " in parsing int, num currently is " << n << std::endl;
            break;
        }
        if (n > max64_div10) { // multiplication overflow
            bad = true;
            WDEBUG << "multiplication overflow" << std::endl;
            break;
        }
        n *= 10;
        if ((n + next_digit) < n) { // addition overflow
            bad = true;
            WDEBUG << "addition overflow" << std::endl;
            break;
        }
        n += next_digit;
        ++idx;
    }
}

inline void
skip_whitespace(std::string &line, size_t &i)
{
    while (i < line.length() && (line[i] == ' '
        || line[i] == '\r'
        || line[i] == '\n'
        || line[i] == '\t')) {
        ++i;
    }
}

// parse the string 'line' as '<unsigned int> <unsigned int> '
// there can be arbitrary whitespace between the two ints, and after the second int
// store the two parsed ints in 'n1' and 'n2'
// if there is overflow or unexpected char is encountered, return n1 = n2 = 0
inline size_t
parse_two_uint64(std::string &line, uint64_t &n1, uint64_t &n2)
{
    size_t i = 0;
    bool bad = false; // overflow or unexpected char

    parse_single_uint64(line, i, n1, bad);
    if (bad || i == line.length()) {
        n1 = 0;
        n2 = 0;
        WDEBUG << "Parsing error, line: " << line << std::endl;
        return -1;
    }

    skip_whitespace(line, i);

    parse_single_uint64(line, i, n2, bad);
    if (bad) {
        n1 = 0;
        n2 = 0;
        WDEBUG << "Parsing error" << std::endl;
    }

    skip_whitespace(line, i);

    return i;
}

inline void
parse_weaver_edge(std::string &line, uint64_t &n1, uint64_t &n2,
        std::vector<std::pair<std::string, std::string>> &props)
{
    size_t i = parse_two_uint64(line, n1, n2);
    while (i < line.length()) {
        size_t start1 = i;
        while (line[i] != ' ' && line[i] != '\t') {
            i++;
        }
        size_t len1 = i - start1;
        skip_whitespace(line, i);
        size_t start2 = i;
        while (i < line.length() && (line[i] != ' '
            && line[i] != '\t'
            && line[i] != '\n'
            && line[i] != '\r')) {
            i++;
        }
        size_t len2 = i - start2;
        skip_whitespace(line, i);
        props.emplace_back(std::make_pair(line.substr(start1, len1), line.substr(start2, len2)));
    }
}

inline void
split(const std::string &s, char delim, std::vector<std::string> &elems)
{
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

inline bool
get_xml_element(std::ifstream &file, const std::string &name, std::string &element)
{
    std::string line;
    bool in_elem = false;
    size_t pos;
    std::string start = "<"+name;
    std::string end = "</"+name+">";

    while(std::getline(file, line)) {
        bool appended = false;
        if ((pos = line.find(start)) != std::string::npos) {
            if (in_elem) {
                return false;
            }
            in_elem = true;
            element.append(line);
            appended = true;
        }

        if ((pos = line.find(end)) != std::string::npos) {
            if (!in_elem) {
                return false;
            }
            if (!appended) {
                element.append(line);
                appended = true;
            }
            return true;
        }

        if (!appended && in_elem) {
            element.append(line);
            appended = true;
        }
    }

    return false;
}

// initial bulk graph loading method
// 'format' stores the format of the graph file
// 'graph_file' stores the full path filename of the graph file
inline void
load_graph(db::graph_file_format format, const char *graph_file, uint64_t num_shards)
{
    std::ifstream file;
    uint64_t node0, node1; // int node_handles
    node_handle_t id0, id1;
    edge_handle_t edge_handle;
    uint64_t loc;
    std::string line, str_node;

    file.open(graph_file, std::ifstream::in);
    if (!file) {
        WDEBUG << "File not found" << std::endl;
        return;
    }

    // read, validate, and create graph
    db::node *n;
    uint64_t line_count = 0;
    uint64_t edge_count = 0;
    uint64_t max_node_handle;
    vc::vclock zero_clk(0, 0);

    switch(format) {

        case db::SNAP: {
            std::getline(file, line);
            assert(line.length() > 0 && line[0] == '#');
            char *max_node_ptr = new char[line.length()+1];
            std::strcpy(max_node_ptr, line.c_str());
            max_node_handle = strtoull(++max_node_ptr, NULL, 10);

            while (std::getline(file, line)) {
                line_count++;
                if (line_count % 100000 == 0) {
                    WDEBUG << "bulk loading: processed " << line_count << " lines" << std::endl;
                }
                if ((line.length() == 0) || (line[0] == '#')) {
                    continue;
                } else {
                    parse_two_uint64(line, node0, node1);
                    id0 = std::to_string(node0);
                    id1 = std::to_string(node1);
                    edge_handle = std::to_string(max_node_handle + (++edge_count));
                    uint64_t loc0 = ((node0 % num_shards) + ShardIdIncr);
                    uint64_t loc1 = ((node1 % num_shards) + ShardIdIncr);
                    if (loc0 == shard_id) {
                        n = S->bulk_load_acquire_node_nonlocking(id0);
                        if (n == NULL) {
                            n = S->create_node(id0, zero_clk, false, true);
                        }
                        S->create_edge_nonlocking(n, edge_handle, id1, loc1, zero_clk, true);
                    }
                    if (loc1 == shard_id) {
                        if (!S->bulk_load_node_exists_nonlocking(id1)) {
                            S->create_node(id1, zero_clk, false, true);
                        }
                    }
                }
            }
            S->bulk_load_persistent();
            break;
        }

        case db::WEAVER: {
            std::unordered_map<node_handle_t, uint64_t> all_node_map;
            std::getline(file, line);
            assert(line.length() > 0 && line[0] == '#');
            char *max_node_ptr = new char[line.length()+1];
            std::strcpy(max_node_ptr, line.c_str());
            max_node_handle = strtoull(++max_node_ptr, NULL, 10);

            // nodes
            while (std::getline(file, line)) {
                parse_two_uint64(line, node0, loc);
                id0 = std::to_string(node0);
                loc += ShardIdIncr;
                all_node_map[id0] = loc;
                assert(loc < num_shards + ShardIdIncr);
                if (loc == shard_id) {
                    n = S->bulk_load_acquire_node_nonlocking(id0);
                    if (n == nullptr) {
                        n = S->create_node(id0, zero_clk, false, true);
                    }
                }
                if (++line_count == max_node_handle) {
                     WDEBUG << "Last node pos line: " << line << std::endl;
                     break;
                }
            }

            // edges
            std::vector<std::pair<std::string, std::string>> props;
            while (std::getline(file, line)) {
                props.clear();
                parse_weaver_edge(line, node0, node1, props);
                id0 = std::to_string(node0);
                id1 = std::to_string(node1);
                edge_handle = std::to_string(max_node_handle + (++edge_count));
                uint64_t loc0 = all_node_map[id0];
                uint64_t loc1 = all_node_map[id1];
                if (loc0 == shard_id) {
                    n = S->bulk_load_acquire_node_nonlocking(id0);
                    assert(n != nullptr);
                    S->create_edge_nonlocking(n, edge_handle, id1, loc1, zero_clk, true);
                    for (auto &p: props) {
                        S->set_edge_property_nonlocking(n, edge_handle, p.first, p.second, zero_clk);
                    }
                }
            }

            S->bulk_load_persistent();
            break;
        }

        case db::GRAPHML: {
            auto hash_string = std::hash<std::string>();
            pugi::xml_document doc;
            std::string element;

            // nodes
            int num_nodes = 0;
            while (get_xml_element(file, "node", element) && !element.empty()) {
                assert(doc.load_buffer(element.c_str(), element.size()));
                pugi::xml_node node = doc.child("node");

                id0 = node.attribute("id").value();
                loc = (hash_string(id0) % num_shards) + ShardIdIncr;
                if (loc == shard_id) {
                    n = S->bulk_load_acquire_node_nonlocking(id0);
                    assert(n == nullptr);
                    n = S->create_node(id0, zero_clk, false, true);

                    bool prop_delim = (BulkLoadPropertyValueDelimiter != '\0');
                    for (pugi::xml_node prop: node.children("data")) {
                        std::string key = prop.attribute("key").value();
                        std::string value = prop.child_value();
                        if (prop_delim) {
                            std::vector<std::string> values;
                            split(value, BulkLoadPropertyValueDelimiter, values);
                            for (std::string &v: values) {
                                (key == BulkLoadNodeAliasKey)? S->add_node_alias_nonlocking(n, v) :
                                                               S->set_node_property_nonlocking(n, key, v, zero_clk);
                            }
                        } else {
                            (key == BulkLoadNodeAliasKey)? S->add_node_alias_nonlocking(n, value) :
                                                           S->set_node_property_nonlocking(n, key, value, zero_clk);
                        }
                    }
                }

                element.clear();
                if (++num_nodes % 10000 == 0) {
                    WDEBUG << "created " << num_nodes << " nodes, " << id0 << " has size " << message::size(*n) << std::endl;
                }
            }


            // edges
            file.close();
            file.open(graph_file, std::ifstream::in);
            element.clear();
            while (get_xml_element(file, "edge", element) && !element.empty()) {
                assert(doc.load_buffer(element.c_str(), element.size()));
                pugi::xml_node edge = doc.child("edge");

                id0 = edge.attribute("source").value();
                id1 = edge.attribute("target").value();
                edge_handle = edge.attribute("id").value();
                uint64_t loc0 = (hash_string(id0) % num_shards) + ShardIdIncr;
                uint64_t loc1 = (hash_string(id1) % num_shards) + ShardIdIncr;
                if (loc0 == shard_id) {
                    n = S->bulk_load_acquire_node_nonlocking(id0);
                    assert(n != nullptr);
                    S->create_edge_nonlocking(n, edge_handle, id1, loc1, zero_clk, true);

                    for (pugi::xml_node prop: edge.children("data")) {
                        std::string key = prop.attribute("key").value();
                        std::string value = prop.child_value();
                        if (BulkLoadPropertyValueDelimiter != '\0') {
                            std::vector<std::string> values;
                            split(value, BulkLoadPropertyValueDelimiter, values);
                            for (std::string &v: values) {
                                S->set_edge_property_nonlocking(n, edge_handle, key, v, zero_clk);
                            }
                        } else {
                            S->set_edge_property_nonlocking(n, edge_handle, key, value, zero_clk);
                        }
                    }
                }
                edge_count++;
                if (edge_count % 10000 == 0) {
                    WDEBUG << "edge " << edge_count << std::endl;
                }

                element.clear();
            }

            S->bulk_load_persistent();
            break;
        }

        default:
            WDEBUG << "Unknown graph file format " << std::endl;
            return;
    }
    file.close();

    WDEBUG << "Loaded graph at shard " << shard_id << " with " << S->shard_node_count[shard_id - ShardIdIncr]
            << " nodes and " << edge_count << " edges" << std::endl;
}

void
migrated_nbr_update(std::unique_ptr<message::message> msg)
{
    node_handle_t node;
    uint64_t old_loc, new_loc;
    msg->unpack_message(message::MIGRATED_NBR_UPDATE, node, old_loc, new_loc);
    S->update_migrated_nbr(node, old_loc, new_loc);
}

void
migrated_nbr_ack(uint64_t from_loc, std::vector<vc::vclock_t> &target_prog_clk, uint64_t node_count)
{
    S->migration_mutex.lock();
    for (uint64_t i = 0; i < NumVts; i++) {
        if (order::oracle::happens_before_no_kronos(S->target_prog_clk[i], target_prog_clk[i])) {
            S->target_prog_clk[i] = target_prog_clk[i];
        }
    }
    S->migr_edge_acks[from_loc - ShardIdIncr] = true;
    S->shard_node_count[from_loc - ShardIdIncr] = node_count;
    S->migration_mutex.unlock();
}

void
unpack_migrate_request(db::message_wrapper *request)
{
    switch (request->type) {
        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(std::move(request->msg));
            break;

        case message::MIGRATE_SEND_NODE:
            migrate_node_step2_resp(std::move(request->msg), request->time_oracle);
            break;

        case message::MIGRATED_NBR_ACK: {
            uint64_t from_loc, node_count;
            std::vector<vc::vclock_t> target_prog_clk;
            request->msg->unpack_message(request->type, from_loc, target_prog_clk, node_count);
            migrated_nbr_ack(from_loc, target_prog_clk, node_count);
            break;
        }

        default:
            WDEBUG << "unknown type" << std::endl;
    }
    delete request;
}

void
apply_writes(uint64_t vt_id, vc::vclock &vclk, uint64_t qts, transaction::pending_tx &tx)
{
    // apply all writes
    // acquire_node_write blocks if a preceding write has not been executed
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::EDGE_CREATE_REQ:
                S->create_edge(upd->handle, upd->handle1, upd->handle2, upd->loc2, vclk, qts);
                break;

            case transaction::NODE_DELETE_REQ:
                S->delete_node(upd->handle1, vclk, qts);
                break;

            case transaction::EDGE_DELETE_REQ:
                S->delete_edge(upd->handle1, upd->handle2, vclk, qts);
                break;

            case transaction::NODE_SET_PROPERTY:
                S->set_node_property(upd->handle1, std::move(upd->key), std::move(upd->value), vclk, qts);
                break;

            case transaction::EDGE_SET_PROPERTY:
                S->set_edge_property(upd->handle1, upd->handle2, std::move(upd->key), std::move(upd->value), vclk, qts);
                break;

            case transaction::ADD_AUX_INDEX:
                S->add_node_alias(upd->handle1, upd->handle, vclk, qts);
                break;

            default:
                continue;
        }
    }

    // record clocks for future reads
    S->record_completed_tx(vclk);

    // send tx confirmation to coordinator
    message::message conf_msg;
    conf_msg.prepare_message(message::TX_DONE, tx.id, shard_id);
    S->comm.send(vt_id, conf_msg.buf);
}

void
unpack_tx_request(db::message_wrapper *request)
{
    uint64_t vt_id;
    vc::vclock vclk;
    uint64_t qts;
    transaction::pending_tx tx(transaction::UPDATE);
    request->msg->unpack_message(message::TX_INIT, vt_id, vclk, qts, tx);

    // execute all create_node writes
    // establish tx order at all graph nodes for all other writes
    db::node *n;
    for (auto upd: tx.writes) {
        n = nullptr;
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                S->create_node(upd->handle, vclk, false);
                break;

            // elem1
            case transaction::EDGE_CREATE_REQ:
            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY:
            case transaction::ADD_AUX_INDEX:
                n = S->acquire_node_latest(upd->handle1);
                break;

            // elem2
            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY:
                n = S->acquire_node_latest(upd->handle2);
                break;

            default:
                WDEBUG << "unknown type" << std::endl;
        }

        if (upd->type != transaction::NODE_CREATE_REQ) {
            assert(n != nullptr);
            n->tx_queue.emplace_back(std::make_pair(vt_id, qts));
            S->release_node(n);
        }
    }

    // increment qts so that threadpool moves forward
    // since tx order has already been established, conflicting txs will be processed in correct order
    S->increment_qts(vt_id, 1);

    apply_writes(vt_id, vclk, qts, tx);

    delete request;
}

// process nop
// migration-related checks, and possibly initiating migration
inline void
nop(db::message_wrapper *request)
{
    uint64_t vt_id;
    vc::vclock vclk;
    uint64_t qts;
    transaction::pending_tx tx(transaction::NOP);
    request->msg->unpack_message(message::TX_INIT, vt_id, vclk, qts, tx);

    message::message msg;
    std::shared_ptr<transaction::nop_data> nop_arg = tx.nop;
    bool check_move_migr, check_init_migr, check_migr_step3;

    // increment qts
    S->increment_qts(vt_id, 1);

    // note done progs for state clean up
    assert(nop_arg->done_reqs.size() == 1);
    auto done_req_iter = nop_arg->done_reqs.begin();
    assert(done_req_iter->first == shard_id-ShardIdIncr);
    S->add_done_requests(done_req_iter->second);

    // migration
    S->migration_mutex.lock();

    // increment nop count, trigger migration step 2 after check
    check_move_migr = true;
    check_init_migr = false;
    if (S->current_migr) {
        S->nop_count.at(vt_id)++;
        for (uint64_t &x: S->nop_count) {
            check_move_migr = check_move_migr && (x == 2);
        }
    } else {
        check_move_migr = false;
    }
    if (!S->migrated && S->migr_token) {
        if (S->migr_token_hops == 0) {
            // return token to vt
            WDEBUG << "Returning token to VT " << S->migr_vt << std::endl;
            WDEBUG << "Shard node counts: ";
            for (auto &x: S->shard_node_count) {
                std::cerr << " " << x;
            }
            std::cerr << std::endl;
            msg.prepare_message(message::MIGRATION_TOKEN);
            S->comm.send(S->migr_vt, msg.buf);
            S->migrated = true;
            S->migr_token = false;
        } else if (S->migr_chance++ > 2) {
            S->migrated = true;
            check_init_migr = true;
            S->migr_chance = 0;
            WDEBUG << "Got token at shard " << shard_id << ", migr hops = " << S->migr_token_hops << std::endl;
        }
    }

    // node max done id for migrated node clean up
    assert(order::oracle::equal_or_happens_before_no_kronos(S->max_done_clk[vt_id], nop_arg->max_done_clk));
    S->max_done_clk[vt_id] = std::move(nop_arg->max_done_clk);
    check_migr_step3 = check_step3();

    // atmost one check should be true
    assert(!(check_move_migr && check_init_migr)
        && !(check_init_migr && check_migr_step3)
        && !(check_move_migr && check_migr_step3));

    uint64_t cur_node_count = S->shard_node_count[shard_id - ShardIdIncr];
    uint64_t max_idx = S->shard_node_count.size() > nop_arg->shard_node_count.size() ?
                       nop_arg->shard_node_count.size() : S->shard_node_count.size();
    for (uint64_t shrd = 0; shrd < max_idx; shrd++) {
        if ((shrd + ShardIdIncr) == shard_id) {
            continue;
        }
        S->shard_node_count[shrd] = nop_arg->shard_node_count[shrd];
    }
    S->migration_mutex.unlock();

    // initiate permanent deletion
    S->permanent_delete_loop(vt_id, nop_arg->outstanding_progs != 0, request->time_oracle);

    // record clock; reads go through
    S->record_completed_tx(tx.timestamp);

    // ack to VT
    //std::cerr << "nop ack, qts = " << qts << ", vclk " << vclk.vt_id << " : ";
    //for (uint64_t c: vclk.clock) {
    //    std::cerr << c << " ";
    //}
    //std::cerr << std::endl;
    msg.prepare_message(message::VT_NOP_ACK, shard_id, qts, cur_node_count);
    S->comm.send(vt_id, msg.buf);

    // call appropriate function based on check after acked to vt
    if (check_move_migr) {
        migrate_node_step2_req();
    } else if (check_init_migr) {
        migration_begin();
    } else if (check_migr_step3) {
        migrate_node_step3();
    }

    delete request;
}

node_prog::Node_State_Base*
get_state_if_exists(db::node &node, uint64_t req_id, node_prog::prog_type ptype)
{
    auto &state_map = node.prog_states[(int)ptype];
    std::shared_ptr<node_prog::Node_State_Base> toRet;
    auto state_iter = state_map.find(req_id);
    if (state_iter != state_map.end()) {
        return state_iter->second.get();
    } 
    return NULL;
}

// assumes holding node lock
template <typename NodeStateType>
NodeStateType& get_or_create_state(node_prog::prog_type ptype,
    uint64_t req_id,
    db::node *node,
    std::vector<db::node_version_t> *nodes_that_created_state)
{
    auto &state_map = node->prog_states[(int)ptype];
    auto state_iter = state_map.find(req_id);
    if (state_iter != state_map.end()) {
        return dynamic_cast<NodeStateType &>(*(state_iter->second));
    } else {
        NodeStateType *ptr = new NodeStateType();
        state_map[req_id] = std::unique_ptr<node_prog::Node_State_Base>(ptr);
        assert(nodes_that_created_state != NULL);
        nodes_that_created_state->emplace_back(std::make_pair(node->get_handle(), node->base.get_creat_time()));
        return *ptr;
    }
}

// vector pointers can be null if we don't want to fill that vector
inline void
fill_changed_properties(std::unordered_map<std::string, std::vector<std::shared_ptr<db::property>>> &props,
    std::vector<node_prog::property> *props_added,
    std::vector<node_prog::property> *props_deleted,
    vc::vclock &time_cached,
    vc::vclock &cur_time,
    order::oracle *time_oracle)
{
    for (auto &iter : props) {
        for (std::shared_ptr<db::property> p: iter.second) {
            db::property &prop = *p;
            bool del_before_cur = (time_oracle->compare_two_vts(prop.get_del_time(), cur_time) == 0);

            if (props_added != NULL) {
                bool creat_after_cached = (time_oracle->compare_two_vts(prop.get_creat_time(), time_cached) == 1);
                bool creat_before_cur = (time_oracle->compare_two_vts(prop.get_creat_time(), cur_time) == 0);

                if (creat_after_cached && creat_before_cur && !del_before_cur) {
                    props_added->emplace_back(prop.key, prop.value);
                }
            }

            if (props_deleted != NULL) {
                bool del_after_cached = (time_oracle->compare_two_vts(prop.get_del_time(), time_cached) == 1);

                if (del_after_cached && del_before_cur) {
                    props_deleted->emplace_back(prop.key, prop.value);
                }
            }
        }
    }
}

// records all changes to nodes given in ids vector between time_cached and cur_time
// returns false if cache should be invalidated
inline bool
fetch_node_cache_contexts(uint64_t loc,
    std::vector<node_handle_t> &handles,
    std::vector<node_prog::node_cache_context> &toFill,
    vc::vclock& time_cached,
    vc::vclock& cur_time,
    order::oracle *time_oracle)
{
    for (node_handle_t &handle: handles) {
        db::node *node = S->acquire_node_version(handle, time_cached, time_oracle);

        if (node == nullptr
         || node->state == db::node::mode::MOVED
         || (node->last_perm_deletion != NULL && time_oracle->compare_two_vts(time_cached, *node->last_perm_deletion) == 0)) {
            if (node != nullptr) {
                S->release_node(node);
            }
            WDEBUG << "node not found or migrated or some data permanently deleted, invalidating cached value" << std::endl;
            toFill.clear(); // contexts aren't valid so don't send back
            return false;
        } else {
            // node exists

            if (time_oracle->compare_two_vts(node->base.get_del_time(), cur_time) == 0) { // node has been deleted
                toFill.emplace_back(loc, handle, true);
            } else {
                node_prog::node_cache_context *context = nullptr;
                std::vector<node_prog::property> temp_props_added;
                std::vector<node_prog::property> temp_props_deleted;

                fill_changed_properties(node->base.properties, &temp_props_added, &temp_props_deleted, time_cached, cur_time, time_oracle);
                // check for changes to node properties
                if (!temp_props_added.empty() || !temp_props_deleted.empty()) {
                    toFill.emplace_back(loc, handle, false);
                    context = &toFill.back();

                    context->props_added = std::move(temp_props_added);
                    context->props_deleted = std::move(temp_props_deleted);
                    temp_props_added.clear();
                    temp_props_deleted.clear();
                }
                // now check for any edge changes
                for (auto &iter: node->out_edges) {
                    for (db::edge *e: iter.second) {
                        assert(e != nullptr);

                        bool del_after_cached = (time_oracle->compare_two_vts(time_cached, e->base.get_del_time()) == 0);
                        bool creat_after_cached = (time_oracle->compare_two_vts(time_cached, e->base.get_creat_time()) == 0);

                        bool del_before_cur = (time_oracle->compare_two_vts(e->base.get_del_time(), cur_time) == 0);
                        bool creat_before_cur = (time_oracle->compare_two_vts(e->base.get_creat_time(), cur_time) == 0);

                        if (creat_after_cached && creat_before_cur && !del_before_cur) {
                            if (context == nullptr) {
                                toFill.emplace_back(loc, handle, false);
                                context = &toFill.back();
                            }

                            context->edges_added.emplace_back(e->get_handle(), e->nbr);
                            node_prog::edge_cache_context &edge_context = context->edges_added.back();
                            // don't care about props deleted before req time for an edge created after cache value was stored
                            fill_changed_properties(e->base.properties, &edge_context.props_added, nullptr, time_cached, cur_time, time_oracle);
                        } else if (del_after_cached && del_before_cur) {
                            if (context == nullptr) {
                                toFill.emplace_back(loc, handle, false);
                                context = &toFill.back();
                            }
                            context->edges_deleted.emplace_back(e->get_handle(), e->nbr);
                            node_prog::edge_cache_context &edge_context = context->edges_deleted.back();
                            // don't care about props added after cache time on a deleted edge
                            fill_changed_properties(e->base.properties, nullptr, &edge_context.props_deleted, time_cached, cur_time, time_oracle);
                        } else if (del_after_cached && !creat_after_cached) {
                            // see if any properties changed on edge that didnt change
                            fill_changed_properties(e->base.properties, &temp_props_added,
                                    &temp_props_deleted, time_cached, cur_time, time_oracle);
                            if (!temp_props_added.empty() || !temp_props_deleted.empty()) {
                                if (context == nullptr) {
                                    toFill.emplace_back(loc, handle, false);
                                    context = &toFill.back();
                                }
                                context->edges_modified.emplace_back(e->get_handle(), e->nbr);

                                context->edges_modified.back().props_added = std::move(temp_props_added);
                                context->edges_modified.back().props_deleted = std::move(temp_props_deleted);

                                temp_props_added.clear();
                                temp_props_deleted.clear();
                            }
                        }
                    }
                }
            }
        }
        S->release_node(node);
    }
    return true;
}

void
unpack_and_fetch_context(db::message_wrapper *request)
{
    std::vector<node_handle_t> ids;
    vc::vclock req_vclock, time_cached;
    uint64_t vt_id, req_id, from_shard;
    std::tuple<cache_key_t, uint64_t, node_handle_t> cache_tuple;
    node_prog::prog_type pType;

    request->msg->unpack_message(message::NODE_CONTEXT_FETCH, pType, req_id, vt_id, req_vclock, time_cached, cache_tuple, ids, from_shard);
    std::vector<node_prog::node_cache_context> contexts;

    bool cache_valid = fetch_node_cache_contexts(S->shard_id, ids, contexts, time_cached, req_vclock, request->time_oracle);

    message::message m;
    m.prepare_message(message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock, cache_tuple, contexts, cache_valid);
    S->comm.send(from_shard, m.buf);
    delete request;
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
struct fetch_state
{
    node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> prog_state;
    po6::threads::mutex monitor;
    uint64_t replies_left;
    bool cache_valid;

    fetch_state(node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> & copy_from)
        : prog_state(copy_from.clone_without_start_node_params()), cache_valid(false) { }

    // delete standard copy onstructors
    fetch_state (const fetch_state&) = delete;
    fetch_state& operator=(fetch_state const&) = delete;
};

/* precondition: node_to_check is locked when called
   returns true if it has updated cached value or there is no valid one and the node prog loop should continue
   returns false and frees node if it needs to fetch context on other shards, saves required state to continue node program later
 */
template <typename ParamsType, typename NodeStateType, typename CacheValueType>
inline bool cache_lookup(db::node*& node_to_check,
    cache_key_t cache_key,
    node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> &np,
    std::pair<node_handle_t, ParamsType> &cur_node_params,
    order::oracle *time_oracle)
{
    assert(node_to_check != NULL);
    assert(!np.cache_value); // cache_value is not already assigned
    np.cache_value = NULL; // it is unallocated anyway
    if (node_to_check->cache.find(cache_key) == node_to_check->cache.end()) {
        return true;
    } else {
        auto entry = node_to_check->cache[cache_key];
        std::shared_ptr<node_prog::Cache_Value_Base> cval = entry.val;
        std::shared_ptr<vc::vclock> time_cached(entry.clk);
        std::shared_ptr<std::vector<db::remote_node>> watch_set = entry.watch_set;

        auto state = get_state_if_exists(*node_to_check, np.req_id, np.prog_type_recvd);
        if (state != NULL && state->contexts_found.find(np.req_id) != state->contexts_found.end()) {
            np.cache_value.reset(new node_prog::cache_response<CacheValueType>(node_to_check->cache, cache_key, cval, watch_set));
#ifdef weaver_debug_
            S->watch_set_lookups_mutex.lock();
            S->watch_set_nops++;
            S->watch_set_lookups_mutex.unlock();
#endif
            return true;
        }

        int64_t cmp_1 = time_oracle->compare_two_vts(*time_cached, *np.req_vclock);
        if (cmp_1 >= 1) { // cached value is newer or from this same request
            return true;
        }
        assert(cmp_1 == 0);

        if (watch_set->empty()) { // no context needs to be fetched
            np.cache_value.reset(new node_prog::cache_response<CacheValueType>(node_to_check->cache, cache_key, cval, watch_set));
            return true;
        }

        std::tuple<cache_key_t, uint64_t, node_handle_t> cache_tuple(cache_key, np.req_id, node_to_check->get_handle());

        S->node_prog_running_states_mutex.lock();
        if (S->node_prog_running_states.find(cache_tuple) != S->node_prog_running_states.end()) {
            fetch_state<ParamsType, NodeStateType, CacheValueType> *fstate = (fetch_state<ParamsType, NodeStateType, CacheValueType> *) S->node_prog_running_states[cache_tuple];
            fstate->monitor.lock(); // maybe move up
            S->node_prog_running_states_mutex.unlock();
            assert(fstate->replies_left > 0);
            fstate->prog_state.start_node_params.push_back(cur_node_params);
            fstate->monitor.unlock();

            S->release_node(node_to_check);
            node_to_check = NULL;

#ifdef weaver_debug_
            S->watch_set_lookups_mutex.lock();
            S->watch_set_piggybacks++;
            S->watch_set_lookups_mutex.unlock();
#endif
            return false;
        }

        std::unique_ptr<node_prog::cache_response<CacheValueType>> future_cache_response(
                new node_prog::cache_response<CacheValueType>(node_to_check->cache, cache_key, cval, watch_set));
        S->release_node(node_to_check);
        node_to_check = NULL;

        // add running state to shard global structure while context is being fetched
        fetch_state<ParamsType, NodeStateType, CacheValueType> *fstate = new fetch_state<ParamsType, NodeStateType, CacheValueType>(np);
        fstate->monitor.lock();
        S->node_prog_running_states[cache_tuple] = fstate; 
        S->node_prog_running_states_mutex.unlock();

#ifdef weaver_debug_
        S->watch_set_lookups_mutex.lock();
        S->watch_set_lookups++;
        S->watch_set_lookups_mutex.unlock();
#endif

        // map from loc to list of ids on that shard we need context from for this request
        std::unordered_map<uint64_t, std::vector<node_handle_t>> contexts_to_fetch; 

        for (db::remote_node &watch_node : *watch_set) {
            contexts_to_fetch[watch_node.loc].emplace_back(watch_node.handle);
        }

        fstate->replies_left = contexts_to_fetch.size();
        fstate->prog_state.cache_value.swap(future_cache_response);
        fstate->prog_state.start_node_params.push_back(cur_node_params);
        fstate->cache_valid = true;
        assert(fstate->prog_state.start_node_params.size() == 1);
        fstate->monitor.unlock();

        uint64_t num_shards = get_num_shards();
        for (uint64_t i = ShardIdIncr; i < num_shards + ShardIdIncr; i++) {
            if (contexts_to_fetch.find(i) != contexts_to_fetch.end()) {
                auto &context_list = contexts_to_fetch[i];
                assert(context_list.size() > 0);
                std::unique_ptr<message::message> m(new message::message());
                m->prepare_message(message::NODE_CONTEXT_FETCH, np.prog_type_recvd, np.req_id, np.vt_id, np.req_vclock, *time_cached, cache_tuple, context_list, S->shard_id);
                S->comm.send(i, m->buf);
            }
        }
        return false;
    } 
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
inline void node_prog_loop(typename node_prog::node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func,
        node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> &np,
        order::oracle *time_oracle)
{
    assert(time_oracle != nullptr);

    message::message out_msg;
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::deque<std::pair<node_handle_t, ParamsType>>> batched_node_progs;
    // node state function
    std::function<NodeStateType&()> node_state_getter;
    std::function<void(std::shared_ptr<CacheValueType>,
                       std::shared_ptr<std::vector<db::remote_node>>,
                       cache_key_t)> add_cache_func;

    std::vector<node_version_t> nodes_that_created_state;

    node_handle_t node_handle;
    bool done_request = false;
    db::remote_node this_node(S->shard_id, "");

    while (!done_request && !np.start_node_params.empty()) {
        auto &id_params = np.start_node_params.front();
        node_handle = id_params.first;
        ParamsType &params = id_params.second;
        this_node.handle = node_handle;

        db::node *node = S->acquire_node_version(node_handle, *np.req_vclock, time_oracle);

        if (node == NULL || time_oracle->compare_two_vts(node->base.get_del_time(), *np.req_vclock)==0) {
            if (node != NULL) {
                S->release_node(node);
            } else {
                // node is being migrated here, but not yet completed
                std::vector<std::pair<node_handle_t, ParamsType>> buf_node_params;
                buf_node_params.emplace_back(id_params);
                std::unique_ptr<message::message> m(new message::message());
                m->prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.vt_prog_ptr, buf_node_params);
                S->migration_mutex.lock();
                if (S->deferred_reads.find(node_handle) == S->deferred_reads.end()) {
                    S->deferred_reads.emplace(node_handle, std::vector<std::unique_ptr<message::message>>());
                }
                S->deferred_reads[node_handle].emplace_back(std::move(m));
                WDEBUG << "Buffering read for node " << node_handle << std::endl;
                S->migration_mutex.unlock();
            }
            np.start_node_params.pop_front(); // pop off this one
        } else if (node->state == db::node::mode::MOVED) {
            // queueing/forwarding node program
            std::vector<std::pair<node_handle_t, ParamsType>> fwd_node_params;
            fwd_node_params.emplace_back(id_params);
            std::unique_ptr<message::message> m(new message::message());
            m->prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.vt_prog_ptr, fwd_node_params);
            uint64_t new_loc = node->new_loc;
            S->release_node(node);
            S->comm.send(new_loc, m->buf);
            np.start_node_params.pop_front(); // pop off this one
        } else { // node does exist
            assert(node->state == db::node::mode::STABLE);
#ifdef WEAVER_NEW_CLDG
            assert(false && "new_cldg not supported right now");
                /*
                if (np.prev_server >= ShardIdIncr) {
                    node->msg_count[np.prev_server - ShardIdIncr]++;
                }
                */
#endif
            if (S->check_done_request(np.req_id, *np.req_vclock)) {
                done_request = true;
                S->release_node(node);
                break;
            }

            if (MaxCacheEntries) {
                if (params.search_cache() && !np.cache_value) {
                    // cache value not already found, lookup in cache
                    bool run_prog_now = cache_lookup<ParamsType, NodeStateType, CacheValueType>(node, params.cache_key(), np, id_params, time_oracle);
                    if (!run_prog_now) { 
                        // go to next node while we fetch cache context for this one, cache_lookup releases node if false
                        np.start_node_params.pop_front();
                        continue;
                    }
                }

                using namespace std::placeholders;
                add_cache_func = std::bind(&db::node::add_cache_value, // function pointer
                    node, // reference to object whose member-function is invoked
                    np.req_vclock, // first argument of the function
                    _1, _2, _3); // 1 is cache value, 2 is watch set, 3 is key
            }

            node_state_getter = std::bind(get_or_create_state<NodeStateType>, np.prog_type_recvd, np.req_id, node, &nodes_that_created_state); 

            node->base.view_time = np.req_vclock; 
            node->base.time_oracle = time_oracle;
            assert(np.req_vclock);
            assert(np.req_vclock->clock.size() == ClkSz);
            // call node program
            auto next_node_params = func(*node, this_node,
                    params, // actual parameters for this node program
                    node_state_getter, add_cache_func,
                    (node_prog::cache_response<CacheValueType>*) np.cache_value.get());
            if (MaxCacheEntries) {
                if (np.cache_value) {
                    auto state = get_state_if_exists(*node, np.req_id, np.prog_type_recvd);
                    if (state) {
                        state->contexts_found.insert(np.req_id);
                    }
                }
                np.cache_value.reset(nullptr);
            }
            node->base.view_time = nullptr; 
            node->base.time_oracle = nullptr;
            S->release_node(node);
            np.start_node_params.pop_front(); // pop off this one before potentially add new front

            // batch the newly generated node programs for onward propagation
#ifdef WEAVER_CLDG
            std::unordered_map<node_handle_t, uint32_t> agg_msg_count;
#endif
            uint64_t num_shards = get_num_shards();
            for (std::pair<db::remote_node, ParamsType> &res : next_node_params.second) {
                db::remote_node& rn = res.first; 
                assert(rn.loc < num_shards + ShardIdIncr);
                if (rn == db::coordinator || rn.loc == np.vt_id) {
                    // mark requests as done, will be done for other shards by no-ops from coordinator
                    std::vector<std::pair<uint64_t, node_prog::prog_type>> completed_request {std::make_pair(np.req_id, np.prog_type_recvd)};
                    S->add_done_requests(completed_request);
                    done_request = true;
                    // signal to send back to vector timestamper that issued request
                    std::unique_ptr<message::message> m(new message::message());
                    m->prepare_message(message::NODE_PROG_RETURN, np.prog_type_recvd, np.req_id, np.vt_prog_ptr, res.second);
                    S->comm.send(np.vt_id, m->buf);
                    break; // can only send one message back
                } else {
                    std::deque<std::pair<node_handle_t, ParamsType>> &next_deque = (rn.loc == S->shard_id) ? np.start_node_params : batched_node_progs[rn.loc]; // TODO this is dumb just have a single data structure later
                    if (next_node_params.first == node_prog::search_type::DEPTH_FIRST) {
                        next_deque.emplace_front(rn.handle, std::move(res.second));
                    } else { // BREADTH_FIRST
                        next_deque.emplace_back(rn.handle, std::move(res.second));
                    }
#ifdef WEAVER_CLDG
                    agg_msg_count[node_handle]++;
#endif
                }
            }
#ifdef WEAVER_CLDG
            S->msg_count_mutex.lock();
            for (auto &p: agg_msg_count) {
                S->agg_msg_count[p.first] += p.second;
            }
            S->msg_count_mutex.unlock();
#endif
        }
        uint64_t num_shards = get_num_shards();
        assert(batched_node_progs.size() < num_shards);
        for (auto &loc_progs_pair : batched_node_progs) {
            assert(loc_progs_pair.first != shard_id && loc_progs_pair.first < num_shards + ShardIdIncr);
            if (loc_progs_pair.second.size() > BATCH_MSG_SIZE) {
                out_msg.prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.vt_prog_ptr, loc_progs_pair.second);
                S->comm.send(loc_progs_pair.first, out_msg.buf);
                loc_progs_pair.second.clear();
            }
        }
        if (MaxCacheEntries) {
            assert(np.cache_value == false); // unique ptr is not assigned
        }
    }
    uint64_t num_shards = get_num_shards();
    if (!done_request) {
        for (auto &loc_progs_pair : batched_node_progs) {
            if (!loc_progs_pair.second.empty()) {
                assert(loc_progs_pair.first != shard_id && loc_progs_pair.first < num_shards + ShardIdIncr);
                out_msg.prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.vt_prog_ptr, loc_progs_pair.second);
                S->comm.send(loc_progs_pair.first, out_msg.buf);
                loc_progs_pair.second.clear();
            }
        }
    }

    if (!nodes_that_created_state.empty()) {
        S->mark_nodes_using_state(np.req_id, nodes_that_created_state);
    }
}

void
unpack_node_program(db::message_wrapper *request)
{
    node_prog::prog_type pType;

    request->msg->unpack_partial_message(message::NODE_PROG, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_and_run_db(std::move(request->msg), request->time_oracle);
    delete request;
}

void
unpack_context_reply(db::message_wrapper *request)
{
    node_prog::prog_type pType;

    request->msg->unpack_partial_message(message::NODE_CONTEXT_REPLY, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_context_reply_db(std::move(request->msg), request->time_oracle);
    delete request;
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_context_reply_db(std::unique_ptr<message::message> msg, order::oracle *time_oracle)
{
    vc::vclock req_vclock;
    node_prog::prog_type pType;
    uint64_t req_id, vt_id;
    std::tuple<cache_key_t, uint64_t, node_handle_t> cache_tuple;
    std::vector<node_prog::node_cache_context> contexts_to_add; 
    bool cache_valid;
    msg->unpack_message(message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock,
            cache_tuple, contexts_to_add, cache_valid);

    S->node_prog_running_states_mutex.lock();
    auto lookup_iter = S->node_prog_running_states.find(cache_tuple);
    assert(lookup_iter != S->node_prog_running_states.end());
    struct fetch_state<ParamsType, NodeStateType, CacheValueType> *fstate = (struct fetch_state<ParamsType, NodeStateType, CacheValueType> *) lookup_iter->second;
    fstate->monitor.lock();
    fstate->replies_left--;
    bool run_now = fstate->replies_left == 0;
    if (run_now) {
        //remove from map
        size_t num_erased = S->node_prog_running_states.erase(cache_tuple);
        assert(num_erased == 1);
        UNUSED(num_erased); // if asserts are off
    }
    S->node_prog_running_states_mutex.unlock();

    auto& existing_context = fstate->prog_state.cache_value->get_context();

    if (fstate->cache_valid) {
        if (cache_valid) {
            existing_context.insert(existing_context.end(), contexts_to_add.begin(), contexts_to_add.end()); // XXX try to avoid copy here
        } else {
            // invalidate cache
            existing_context.clear();
            assert(fstate->prog_state.cache_value != nullptr);
            fstate->prog_state.cache_value->invalidate();
            fstate->prog_state.cache_value.reset(nullptr); // clear cached value
            fstate->cache_valid = false;
        }
    }

    if (run_now) {
        node_prog_loop<ParamsType, NodeStateType, CacheValueType>(enclosed_node_prog_func, fstate->prog_state, time_oracle);
        fstate->monitor.unlock();
        delete fstate;
    }
    else {
        fstate->monitor.unlock();
    }
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_run_db(std::unique_ptr<message::message> msg, order::oracle *time_oracle)
{
    node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> np;

    // unpack the node program
    try {
        msg->unpack_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.vt_prog_ptr, np.start_node_params);
        assert(np.req_vclock->clock.size() == ClkSz);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        assert(false);
        return;
    }

    // update max prog id
    S->migration_mutex.lock();
    if (order::oracle::happens_before_no_kronos(S->max_seen_clk[np.vt_id], np.req_vclock->clock)) {
        S->max_seen_clk[np.vt_id] = np.req_vclock->clock;
    }
    S->migration_mutex.unlock();

    // check if request completed
    if (S->check_done_request(np.req_id, *np.req_vclock)) {
        return; // done request
    }

    assert(!np.cache_value); // a cache value should not be allocated yet
    node_prog_loop<ParamsType, NodeStateType, CacheValueType>(enclosed_node_prog_func, np, time_oracle);
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_start_coord(std::unique_ptr<message::message>, uint64_t, coordinator::hyper_stub*)
{ }

inline uint64_t
get_balanced_assignment(std::vector<uint64_t> &shard_node_count, std::vector<uint32_t> &max_indices)
{
    uint64_t min_cap = shard_node_count[max_indices[0]];
    std::vector<uint32_t> min_indices;
    for (uint32_t &idx: max_indices) {
        if (shard_node_count[idx] < min_cap) {
            min_indices.clear();
            min_indices.emplace_back(idx);
        } else if (shard_node_count[idx] == min_cap) {
            min_indices.emplace_back(idx);
        }
    }

    std::random_device rd;
    std::default_random_engine generator(rd());
    std::uniform_int_distribution<uint64_t> distribution(0, min_indices.size()-1);
    int ret_idx = distribution(generator);
    return min_indices[ret_idx];
}

// decide node migration shard based on migration score
// mark node as "moved" so that subsequent requests are queued up
// send migration information to coordinator mapper
// return false if no migration happens (max migr score = this shard), else return true
bool
migrate_node_step1(db::node *n,
    std::vector<uint64_t> &shard_node_count,
    uint64_t migr_num_shards)
{
    // find arg max migr score
    uint64_t max_pos = shard_id - ShardIdIncr; // don't migrate if all equal
    std::vector<uint32_t> max_indices(1, max_pos);
    for (uint32_t j = 0; j < migr_num_shards; j++) {
        if (j == (shard_id - ShardIdIncr)) {
            continue;
        }
        if (n->migr_score[max_pos] < n->migr_score[j]) {
            max_pos = j;
            max_indices.clear();
            max_indices.emplace_back(j);
        } else if (n->migr_score[max_pos] == n->migr_score[j]) {
            max_indices.emplace_back(j);
        }
    }
    uint64_t migr_loc = get_balanced_assignment(shard_node_count, max_indices) + ShardIdIncr;
    if (migr_loc > shard_id) {
        n->already_migr = true;
    }

    // no migration to self
    if (migr_loc == shard_id) {
        S->release_node(n);
        return false;
    }

    // mark node as "moved"
    n->state = db::node::mode::MOVED;
    n->new_loc = migr_loc;
    S->migr_node = n->get_handle();
    S->migr_shard = migr_loc;

    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &x: n->out_edges) {
        for (db::edge *e: x.second) {
            const node_handle_t &remote_node = e->nbr.handle;
            node_version_t local_node = std::make_pair(S->migr_node, e->base.get_creat_time());
            S->remove_from_edge_map(remote_node, local_node);
        }
    }
    S->edge_map_mutex.unlock();

    S->release_node(n);

    // update Hyperdex map for this node
    S->update_node_mapping(S->migr_node, migr_loc);

    // begin migration
    S->migration_mutex.lock();
    S->current_migr = true;
    for (uint64_t &x: S->nop_count) {
        x = 0;
    }
    S->migration_mutex.unlock();

    return true;
}

// pack node in big message and send to new location
void
migrate_node_step2_req()
{
    db::node *n;
    message::message msg;

    S->migration_mutex.lock();
    S->current_migr = false;
    for (uint64_t idx = 0; idx < NumVts; idx++) {
        vc::vclock_t &target_clk = S->target_prog_clk[idx];
        std::fill(target_clk.begin(), target_clk.end(), 0);
    }
    S->migration_mutex.unlock();

    n = S->acquire_node_latest(S->migr_node);
    assert(n != NULL);
    msg.prepare_message(message::MIGRATE_SEND_NODE, S->migr_node, shard_id, *n);
    S->release_node(n);
    S->comm.send(S->migr_shard, msg.buf);
}

// receive and place node which has been migrated to this shard
// apply buffered reads and writes to node
// update nbrs of migrated nbrs
void
migrate_node_step2_resp(std::unique_ptr<message::message> msg, order::oracle *time_oracle)
{
    // unpack and place node
    uint64_t from_loc;
    node_handle_t node_handle;
    db::node *n;

    // create a new node, unpack the message
    vc::vclock dummy_clock;
    msg->unpack_partial_message(message::MIGRATE_SEND_NODE, node_handle);
    n = S->create_node(node_handle, dummy_clock, true); // node will be acquired on return
    try {
        msg->unpack_message(message::MIGRATE_SEND_NODE, node_handle, from_loc, *n);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        return;
    }

    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &x: n->out_edges) {
        for (db::edge *e: x.second) {
            const node_handle_t &remote_node = e->nbr.handle;
            S->edge_map[remote_node].emplace(std::make_pair(node_handle, e->base.get_creat_time()));
        }
    }
    S->edge_map_mutex.unlock();

    S->migration_mutex.lock();
    // apply buffered writes
    if (S->deferred_writes.find(node_handle) != S->deferred_writes.end()) {
        for (auto &dw: S->deferred_writes[node_handle]) {
            switch (dw.type) {
                case transaction::NODE_DELETE_REQ:
                    S->delete_node_nonlocking(n, dw.vclk);
                    break;

                case transaction::EDGE_CREATE_REQ:
                    S->create_edge_nonlocking(n, dw.edge_handle, dw.remote_node, dw.remote_loc, dw.vclk);
                    break;

                case transaction::EDGE_DELETE_REQ:
                    S->delete_edge_nonlocking(n, dw.edge_handle, dw.vclk);
                    break;

                default:
                    WDEBUG << "unexpected type" << std::endl;
            }
        }
        S->deferred_writes.erase(node_handle);
    }

    // update nbrs
    S->migr_updating_nbrs = true;
    for (uint64_t upd_shard = ShardIdIncr; upd_shard < ShardIdIncr + S->migr_edge_acks.size(); upd_shard++) {
        if (upd_shard == shard_id) {
            continue;
        }
        msg->prepare_message(message::MIGRATED_NBR_UPDATE, node_handle, from_loc, shard_id);
        S->comm.send(upd_shard, msg->buf);
    }
    n->state = db::node::mode::STABLE;

    std::vector<uint64_t> prog_state_reqs;
    for (auto &state_map: n->prog_states) {
        for (auto &state: state_map) {
            prog_state_reqs.emplace_back(state.first);
        }
    }

    // release node for new reads and writes
    std::vector<node_version_t> this_node_vec(1, std::make_pair(node_handle, n->base.get_creat_time()));
    S->release_node(n);

    for (uint64_t req_id: prog_state_reqs) {
        S->mark_nodes_using_state(req_id, this_node_vec);
    }

    // move deferred reads to local for releasing migration_mutex
    std::vector<std::unique_ptr<message::message>> deferred_reads;
    if (S->deferred_reads.find(node_handle) != S->deferred_reads.end()) {
        deferred_reads = std::move(S->deferred_reads[node_handle]);
        S->deferred_reads.erase(node_handle);
    }
    S->migration_mutex.unlock();

    // update local nbrs
    S->update_migrated_nbr(node_handle, from_loc, shard_id);

    // apply buffered reads
    for (auto &m: deferred_reads) {
        node_prog::prog_type pType;
        m->unpack_partial_message(message::NODE_PROG, pType);
        WDEBUG << "APPLYING BUFREAD for node " << node_handle << std::endl;
        assert(node_prog::programs.find(pType) != node_prog::programs.end());
        node_prog::programs[pType]->unpack_and_run_db(std::move(m), time_oracle);
    }
}

// check if all nbrs updated, if so call step3
// caution: assuming caller holds S->migration_mutex
bool
check_step3()
{
    bool init_step3 = weaver_util::all(S->migr_edge_acks);
    for (uint64_t i = 0; i < NumVts && init_step3; i++) {
        init_step3 = order::oracle::happens_before_no_kronos(S->target_prog_clk[i], S->max_done_clk[i]);
    }
    if (init_step3) {
        weaver_util::reset_all(S->migr_edge_acks);
        S->migr_updating_nbrs = false;
    }
    return init_step3;
}

// successfully migrated node to new location, continue migration process
void
migrate_node_step3()
{
    S->delete_migrated_node(S->migr_node);
    migration_wrapper();
}

inline bool
check_migr_node(db::node *n, uint64_t migr_num_shards)
{
    if (n == NULL
     || n->base.get_del_time() != S->max_clk
     || n->state == db::node::mode::MOVED
     || n->already_migr) {
        if (n != NULL) {
            n->already_migr = false;
            S->release_node(n);
        }
        return false;
    } else {
        for (double &x: n->migr_score) {
            x = 0;
        }
        n->migr_score.resize(migr_num_shards, 0);
        return true;
    }
}

#ifdef WEAVER_CLDG
// stream list of nodes and cldg repartition
inline void
cldg_migration_wrapper(std::vector<uint64_t> &shard_node_count, uint64_t migr_num_shards, uint64_t shard_cap)
{
    bool no_migr = true;
    while (S->cldg_iter != S->cldg_nodes.end()) {
        db::node *n;
        uint64_t migr_node = S->cldg_iter->first;
        S->cldg_iter++;
        n = S->acquire_node_latest(migr_node);

        // check if okay to migrate
        if (!check_migr_node(n, migr_num_shards)) {
            continue;
        }

        // communication-LDG
        for (uint64_t &cnt: n->msg_count) {
            cnt = 0;
        }

        db::edge *e;
        // get aggregate msg counts per shard
        //std::vector<uint64_t> msg_count(NumShards, 0);
        for (auto &e_iter: n->out_edges) {
            e = e_iter.second;
            //msg_count[e->nbr.loc - ShardIdIncr] += e->msg_count;
            n->msg_count[e->nbr.loc - ShardIdIncr] += e->msg_count;
        }
        // EWMA update to msg count
        //for (uint64_t i = 0; i < NumShards; i++) {
        //    double new_val = 0.4 * n->msg_count[i] + 0.6 * msg_count[i];
        //    n->msg_count[i] = new_val;
        //}

        // update migration score based on CLDG
        for (uint64_t j = 0; j < migr_num_shards; j++) {
            double penalty = 1.0 - ((double)shard_node_count[j])/shard_cap;
            n->migr_score[j] = n->msg_count[j] * penalty;
        }

        if (migrate_node_step1(n, shard_node_count, migr_num_shards)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        migration_end();
    }
}
#endif

// stream list of nodes and ldg repartition
inline void
ldg_migration_wrapper(std::vector<uint64_t> &shard_node_count, uint64_t migr_num_shards, uint64_t shard_cap)
{
    bool no_migr = true;
    while (S->ldg_iter != S->ldg_nodes.end()) {
        db::node *n;
        const node_handle_t &migr_node = *S->ldg_iter;
        S->ldg_iter++;
        n = S->acquire_node_latest(migr_node);

        // check if okay to migrate
        if (!check_migr_node(n, migr_num_shards)) {
            continue;
        }

        // regular LDG
        for (auto &e_iter: n->out_edges) {
            for (db::edge *e: e_iter.second) {
                n->migr_score[e->nbr.loc - ShardIdIncr] += 1;
            }
        }
        for (uint64_t j = 0; j < migr_num_shards; j++) {
            n->migr_score[j] *= (1 - ((double)shard_node_count[j])/shard_cap);
        }

        if (migrate_node_step1(n, shard_node_count, migr_num_shards)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        migration_end();
    }
}

// sort nodes in order of number of requests propagated
void
migration_wrapper()
{
    S->migration_mutex.lock();
    std::vector<uint64_t> shard_node_count = S->shard_node_count;
    float total_node_count = 0;
    for (uint64_t c: shard_node_count) {
        total_node_count += c;
    }
    uint64_t shard_cap = (uint64_t)(1.1 * total_node_count);
    uint64_t num_shards = S->migr_num_shards;
    S->migration_mutex.unlock();

#ifdef WEAVER_CLDG
    cldg_migration_wrapper(shard_node_count, num_shards, shard_cap);
#endif

#ifdef WEAVER_NEW_CLDG
    cldg_migration_wrapper(shard_node_count, num_shards, shard_cap);
#else
    ldg_migration_wrapper(shard_node_count, num_shards, shard_cap);
#endif
}

// method to sort pairs based on second coordinate
bool agg_count_compare(std::pair<node_handle_t, uint32_t> p1, std::pair<node_handle_t, uint32_t> p2)
{
    return (p1.second > p2.second);
}

void
migration_begin()
{
    S->migration_mutex.lock();
    S->ldg_nodes = S->node_list;
    S->migration_mutex.unlock();

#ifdef WEAVER_CLDG
    S->msg_count_mutex.lock();
    auto agg_msg_count = std::move(S->agg_msg_count);
    S->msg_count_mutex.unlock();
    std::vector<std::pair<node_handle_t, uint32_t>> sorted_nodes;
    for (const node_handle_t &n: S->ldg_nodes) {
        if (agg_msg_count.find(n) != agg_msg_count.end()) {
            sorted_nodes.emplace_back(std::make_pair(n, agg_msg_count[n]));
        } else {
            sorted_nodes.emplace_back(std::make_pair(n, 0));
        }
    }
    std::sort(sorted_nodes.begin(), sorted_nodes.end(), agg_count_compare);
    S->cldg_nodes = std::move(sorted_nodes);
    S->cldg_iter = S->cldg_nodes.begin();
#endif

#ifdef WEAVER_NEW_CLDG
    uint64_t mcnt;
    for (const node_handle_t &nid: S->ldg_nodes) {
        mcnt = 0;
        db::node *n = S->acquire_node_latest(nid);
        for (uint64_t cnt: n->msg_count) {
            mcnt += cnt;
        }
        S->release_node(n);
        S->cldg_nodes.emplace_back(std::make_pair(nid, mcnt));
    }
    std::sort(S->cldg_nodes.begin(), S->cldg_nodes.end(), agg_count_compare);
    S->cldg_iter = S->cldg_nodes.begin();
#else
    S->ldg_iter = S->ldg_nodes.begin();
#endif

    migration_wrapper();
}

void
migration_end()
{
    message::message msg;
    S->migration_mutex.lock();
    S->migr_token = false;
    msg.prepare_message(message::MIGRATION_TOKEN, --S->migr_token_hops, S->migr_num_shards, S->migr_vt);
    S->migration_mutex.unlock();

    uint64_t next_shard; 
    if ((shard_id + 1 - ShardIdIncr) >= S->migr_num_shards) {
        next_shard = ShardIdIncr;
    } else {
        next_shard = shard_id + 1;
    }
    S->comm.send(next_shard, msg.buf);
}

// server msg recv loop for the shard server
void
recv_loop(uint64_t thread_id)
{
    uint64_t vt_id, req_id;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    db::queued_request *qreq;
    db::message_wrapper *mwrap;
    node_prog::prog_type pType;
    vc::vclock vclk;
    uint64_t qts;
    transaction::tx_type txtype;
    uint64_t tx_id;
    busybee_returncode bb_code;
    enum db::queue_order tx_order;
    order::oracle *time_oracle = S->time_oracles[thread_id];

    while (true) {

        S->comm.quiesce_thread(thread_id);
        rec_msg.reset(new message::message());
        bb_code = S->comm.recv(&rec_msg->buf);

        if (bb_code != BUSYBEE_SUCCESS && bb_code != BUSYBEE_TIMEOUT) {
            continue;
        }

        if (bb_code == BUSYBEE_SUCCESS) {
            // exec or enqueue this request
            auto unpacker = rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE);
            unpack_buffer(unpacker, mtype);
            rec_msg->change_type(mtype);
            vclk.clock.clear();

            switch (mtype) {
                case message::TX_INIT: {
                    rec_msg->unpack_partial_message(message::TX_INIT, vt_id, vclk, qts, txtype, tx_id);
                    assert(vclk.clock.size() == ClkSz);
                    assert(txtype != transaction::FAIL);

                    if (S->check_done_tx(tx_id)) {
                        // tx already executed, this must have been resent because of vt failure
                        S->increment_qts(vt_id, 1);

                        message::message conf_msg;
                        conf_msg.prepare_message(message::TX_DONE, tx_id, shard_id);
                        S->comm.send(vt_id, conf_msg.buf);
                    } else {
                        mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                        tx_order = S->qm.check_wr_request(vclk, qts);
                        assert(tx_order == db::PRESENT || tx_order == db::FUTURE);

                        if (txtype == transaction::UPDATE) {
                            // write tx
                            if (tx_order == db::PRESENT) {
                                mwrap->time_oracle = time_oracle;
                                unpack_tx_request(mwrap);
                            } else {
                                // enqueue for future execution
                                qreq = new db::queued_request(qts, vclk, unpack_tx_request, mwrap);
                                S->qm.enqueue_write_request(vt_id, qreq);
                            }
                        } else {
                            // nop
                            assert(tx_order != db::PAST);
                            // nop goes through queues for both PRESENT and FUTURE
                            qreq = new db::queued_request(qts, vclk, nop, mwrap);
                            S->qm.enqueue_write_request(vt_id, qreq);
                        }
                    }
                    break;
                }

                case message::NODE_PROG: {
                    rec_msg->unpack_partial_message(message::NODE_PROG, pType, vt_id, vclk, req_id);
                    assert(vclk.clock.size() == ClkSz);

                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    if (S->qm.check_rd_request(vclk.clock)) {
                        mwrap->time_oracle = time_oracle;
                        unpack_node_program(mwrap);
                    } else {
                        qreq = new db::queued_request(req_id, vclk, unpack_node_program, mwrap);
                        S->qm.enqueue_read_request(vt_id, qreq);
                    }
                    break;
                }

                case message::NODE_CONTEXT_FETCH:
                case message::NODE_CONTEXT_REPLY: {
                    void (*f)(db::message_wrapper*);
                    if (mtype == message::NODE_CONTEXT_FETCH) {
                        f = unpack_and_fetch_context;
                    } else { // NODE_CONTEXT_REPLY
                        f = unpack_context_reply;
                    }
                    rec_msg->unpack_partial_message(mtype, pType, req_id, vt_id, vclk);
                    assert(vclk.clock.size() == ClkSz);
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    if (S->qm.check_rd_request(vclk.clock)) {
                        mwrap->time_oracle = time_oracle;
                        f(mwrap);
                    } else {
                        qreq = new db::queued_request(req_id, vclk, f, mwrap);
                        S->qm.enqueue_read_request(vt_id, qreq);
                    }
                    break;
                }

                case message::MIGRATE_SEND_NODE:
                case message::MIGRATED_NBR_UPDATE:
                case message::MIGRATED_NBR_ACK:
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    mwrap->time_oracle = time_oracle;
                    unpack_migrate_request(mwrap);
                    break;

                case message::MIGRATION_TOKEN:
                    S->migration_mutex.lock();
                    rec_msg->unpack_message(mtype, S->migr_token_hops, S->migr_num_shards, S->migr_vt);
                    S->migr_token = true;
                    S->migrated = false;
                    S->migration_mutex.unlock();
                    break;

                case message::LOADED_GRAPH: {
                    uint64_t load_time;
                    rec_msg->unpack_message(message::LOADED_GRAPH, load_time);
                    S->graph_load_mutex.lock();
                    if (load_time > S->max_load_time) {
                        S->max_load_time = load_time;
                    }
                    if (++S->load_count == S->bulk_load_num_shards) {
                        WDEBUG << "Loaded graph on all shards, time taken = " << (S->max_load_time/MEGA) << " ms." << std::endl;
                    } else {
                        WDEBUG << "Loaded graph on " << S->load_count << " shards, current time "
                                << (S->max_load_time/MEGA) << "ms." << std::endl;
                    }
                    S->graph_load_mutex.unlock();
                    break;
                }

                case message::EXIT_WEAVER:
                    exit(0);
                    
                default:
                    WDEBUG << "unexpected msg type " << message::to_string(mtype) << std::endl;
            }
        }

        // execute all queued requests that can be executed now
        // will break from loop when no more requests can be executed, in which case we need to recv
        while (S->qm.exec_queued_request(time_oracle));
    }
}

bool
generate_token(uint64_t* token)
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        return false;
    }

    if (sysrand.read(token, sizeof(*token)) != sizeof(*token))
    {
        return false;
    }

    return true;
}

void
server_manager_link_loop(po6::net::hostname sm_host, po6::net::location my_loc, bool backup)
{
    // Most of the following code has been borrowed from
    // Robert Escriva's HyperDex.
    // see https://github.com/rescrv/HyperDex for the original code.

    S->sm_stub.set_server_manager_address(sm_host.address.c_str(), sm_host.port);

    server::type_t type = backup? server::BACKUP_SHARD : server::SHARD;
    if (!S->sm_stub.register_id(S->serv_id, my_loc, type))
    {
        return;
    }

    uint64_t myid = UINT64_MAX;
    if (!S->sm_stub.get_replid(myid)) {
        WDEBUG << "fail get_replid" << std::endl;
        return;
    }

    bool cluster_jump = false;

    while (!S->sm_stub.should_exit())
    {
        S->exit_mutex.lock();
        if (S->to_exit) {
            S->sm_stub.request_shutdown();
            S->to_exit = false;
        }
        S->exit_mutex.unlock();

        if (!S->sm_stub.maintain_link())
        {
            continue;
        }

        const configuration& old_config(S->config);
        const configuration& new_config(S->sm_stub.config());

        if (old_config.cluster() != 0 &&
            old_config.cluster() != new_config.cluster())
        {
            cluster_jump = true;
            break;
        }

        if (old_config.version() > new_config.version())
        {
            WDEBUG << "received new configuration version=" << new_config.version()
                   << " that's older than our current configuration version="
                   << old_config.version();
            continue;
        }
        // if old_config.version == new_config.version, still fetch

        S->config_mutex.lock();
        S->prev_config = S->config;
        S->config = new_config;
        if (!S->first_config) {
            S->first_config = true;
            S->first_config_cond.signal();
        }
        S->reconfigure();
        S->config_mutex.unlock();

        // let the coordinator know we've moved to this config
        S->sm_stub.config_ack(new_config.version());

    }

    WDEBUG << "exiting server manager link loop" << std::endl;
    if (cluster_jump)
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager changed on us.\n"
               << "This is most likely an operations error."
               << "================================================================================\n";
    }
    else if (S->sm_stub.should_exit() && !S->sm_stub.config().exists(S->serv_id))
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager says it doesn't know about this node.\n"
               << "================================================================================\n";
    }
    else if (S->sm_stub.should_exit())
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because server manager stub says we should exit.\n"
               << "Most likely because we requested shutdown due to program interrupt.\n"
               << "================================================================================\n";
    }
    exit(0);
}

void
install_signal_handler(int signum, void (*handler)(int))
{
    struct sigaction sa;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    int ret = sigaction(signum, &sa, NULL);
    assert(ret == 0);
}

// caution: assume holding S->config_mutex for S->pause_bb
void
init_worker_threads(std::vector<std::thread*> &threads)
{
    for (int i = 0; i < NUM_SHARD_THREADS; i++) {
        std::thread *t = new std::thread(recv_loop, i);
        threads.emplace_back(t);
    }
    S->pause_bb = true;
}

// caution: assume holding S->config_mutex
void
init_shard()
{
    std::vector<server> servers = S->config.get_servers();
    shard_id = UINT64_MAX;
    for (const server &srv: servers) {
        if (srv.id == S->serv_id) {
            assert(srv.type == server::SHARD);
            shard_id = srv.virtual_id + NumVts;
        }
    }
    assert(shard_id != UINT64_MAX);

    // registered this server with server_manager, we now know the shard_id
    S->init(shard_id);
}

int
main(int argc, const char *argv[])
{
    // signal handlers
    install_signal_handler(SIGINT, end_program);
    install_signal_handler(SIGHUP, end_program);
    install_signal_handler(SIGTERM, end_program);

    google::InitGoogleLogging(argv[0]);
    //google::InstallFailureSignalHandler();
    google::LogToStderr();
    //google::SetLogDestination(google::INFO, "weaver-shard-");

    // signals
    //sigset_t ss;
    //if (sigfillset(&ss) < 0) {
    //    WDEBUG << "sigfillset failed" << std::endl;
    //    return -1;
    //}
    //sigdelset(&ss, SIGPROF);
    //sigdelset(&ss, SIGINT);
    //sigdelset(&ss, SIGHUP);
    //sigdelset(&ss, SIGTERM);
    //sigdelset(&ss, SIGTSTP);
    //if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0) {
    //    WDEBUG << "pthread sigmask failed" << std::endl;
    //    return -1;
    //}

    // command line params
    const char* listen_host = "127.0.0.1";
    long listen_port = 5201;
    const char *config_file = "./weaver.yaml";
    const char *graph_file = NULL;
    const char *graph_format = "snap";
    bool backup = false;
    long bulk_load_num_shards = 1;
    // arg parsing borrowed from HyperDex
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: 127.0.0.1)")
            .metavar("IP").as_string(&listen_host);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 5201)")
            .metavar("port").as_long(&listen_port);
    ap.arg().name('b', "backup-shard")
            .description("make this a backup shard")
            .set_true(&backup);
    ap.arg().long_name("config-file")
            .description("full path of weaver.yaml configuration file (default ./weaver.yaml)")
            .metavar("filename").as_string(&config_file);
    ap.arg().long_name("graph-file")
            .description("full path of bulk load input graph file (no default)")
            .metavar("filename").as_string(&graph_file);
    ap.arg().long_name("graph-format")
            .description("bulk load input graph format (default snap)")
            .metavar("filename").as_string(&graph_format);
    ap.arg().long_name("bulk-load-num-shards")
            .description("number of shards during bulk loading (default 1)")
            .metavar("num").as_long(&bulk_load_num_shards);

    if (!ap.parse(argc, argv) || ap.args_sz() != 0) {
        WDEBUG << "args parsing failure" << std::endl;
        return -1;
    }

    // configuration file parse
    if (!init_config_constants(config_file)) {
        WDEBUG << "error in init_config_constants, exiting now." << std::endl;
        return -1;
    }

    po6::net::location my_loc(listen_host, listen_port);
    uint64_t sid;
    assert(generate_token(&sid));
    S = new db::shard(sid, my_loc);

    // server manager link
    std::thread sm_thr(server_manager_link_loop,
        po6::net::hostname(ServerManagerIpaddr, ServerManagerPort),
        my_loc,
        backup);
    sm_thr.detach();

    S->config_mutex.lock();

    // wait for first config to arrive from server_manager
    while (!S->first_config) {
        S->first_config_cond.wait();
    }

    std::vector<std::thread*> worker_threads;

    if (backup) {
        while (!S->active_backup) {
            S->backup_cond.wait();
        }

        init_shard();
        S->config_mutex.unlock();

        // release config_mutex while restoring shard data which may take a while
        S->restore_backup();
        for (uint64_t i = 0; i < NumVts; i++) {
            message::message msg;
            msg.prepare_message(message::RESTORE_DONE);
            S->comm.send(i, msg.buf);
        }

        S->config_mutex.lock();
        init_worker_threads(worker_threads);
        S->config_mutex.unlock();
    } else {
        init_shard();

        init_worker_threads(worker_threads);

        S->config_mutex.unlock();

        // bulk loading
        if (graph_file != NULL) {
            S->bulk_load_num_shards = (uint64_t)bulk_load_num_shards;

            db::graph_file_format format = db::SNAP;
            if (strcmp(graph_format, "tsv") == 0) {
                format = db::TSV;
            } else if (strcmp(graph_format, "snap") == 0) {
                format = db::SNAP;
            } else if (strcmp(graph_format, "weaver") == 0) {
                format = db::WEAVER;
            } else if (strcmp(graph_format, "graphml") == 0) {
                format = db::GRAPHML;
            } else {
                WDEBUG << "Invalid graph file format" << std::endl;
            }

            wclock::weaver_timer timer;
            uint64_t load_time = timer.get_time_elapsed();
            load_graph(format, graph_file, (uint64_t)bulk_load_num_shards);
            load_time = timer.get_time_elapsed() - load_time;
            message::message msg;
            msg.prepare_message(message::LOADED_GRAPH, load_time);
            S->comm.send(ShardIdIncr, msg.buf);
        }
    }

    std::cout << "Weaver: shard instance " << S->shard_id << std::endl;
    std::cout << "THIS IS AN ALPHA RELEASE WHICH SHOULD NOT BE USED IN PRODUCTION" << std::endl;

    for (auto t: worker_threads) {
        t->join();
    }

    return 0;
}

#undef weaver_debug_
