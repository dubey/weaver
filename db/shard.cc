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
#include <iostream>
#include <string>
#include <signal.h>
#include <e/popt.h>
#include <e/buffer.h>

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

// global extern variables
uint64_t NumVts;
uint64_t NumShards;
uint64_t NumBackups;
uint64_t NumEffectiveServers;
uint64_t NumActualServers;
uint64_t ShardIdIncr;
char *ShardsFile;
// global static variables
static uint64_t shard_id;
// shard pointer for shard.cc
static db::shard *S;

void migrated_nbr_update(std::unique_ptr<message::message> msg);
bool migrate_node_step1(db::hyper_stub *hs, db::element::node*, std::vector<uint64_t>&);
void migrate_node_step2_req();
void migrate_node_step2_resp(db::hyper_stub *hs, std::unique_ptr<message::message> msg);
bool check_step3();
void migrate_node_step3(db::hyper_stub *hs);
void migration_begin(db::hyper_stub *hs);
void migration_wrapper(db::hyper_stub *hs);
void migration_end(db::hyper_stub *hs);

void
end_program(int param)
{
    WDEBUG << "Ending program, param = " << param << ", kronos num cache hits = " << order::cache_hits << std::endl;
    WDEBUG << "watch_set lookups originated from this shard " << S->watch_set_lookups << std::endl;
    WDEBUG << "watch_set nops originated from this shard " << S->watch_set_nops << std::endl;
    WDEBUG << "watch set piggybacks on this shard " << S->watch_set_piggybacks << std::endl;
    if (param == SIGINT) {
        S->exit_mutex.lock();
        S->to_exit = true;
        S->exit_mutex.unlock();
    } else {
        WDEBUG << "Not SIGINT, exiting immediately." << std::endl;
        exit(0);
    }
}


inline void
create_node(db::hyper_stub *hs, vc::vclock &t_creat, uint64_t node_id)
{
    S->create_node(hs, node_id, t_creat, false);
}

inline void
create_edge(db::hyper_stub *hs, vc::vclock &t_creat, vc::qtimestamp_t &qts, uint64_t edge_id, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    S->create_edge(hs, edge_id, n1, n2, loc2, t_creat, qts);
}

inline void
delete_node(db::hyper_stub *hs, vc::vclock &t_del, vc::qtimestamp_t &qts, uint64_t node_id)
{
    S->delete_node(hs, node_id, t_del, qts);
}

inline void
delete_edge(db::hyper_stub *hs, vc::vclock &t_del, vc::qtimestamp_t &qts, uint64_t edge_id, uint64_t node_id)
{
    S->delete_edge(hs, edge_id, node_id, t_del, qts);
}

inline void
set_node_property(db::hyper_stub *hs, vc::vclock &vclk, vc::qtimestamp_t &qts, uint64_t node_id, std::unique_ptr<std::string> key,
    std::unique_ptr<std::string> value)
{
    S->set_node_property(hs, node_id, std::move(key), std::move(value), vclk, qts);
}

inline void
set_edge_property(db::hyper_stub *hs, vc::vclock &vclk, vc::qtimestamp_t &qts, uint64_t edge_id, uint64_t node_id,
    std::unique_ptr<std::string> key, std::unique_ptr<std::string> value)
{
    S->set_edge_property(hs, node_id, edge_id, std::move(key), std::move(value), vclk, qts);
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

// called on a separate thread during bulk graph loading process
// XXX check for FT
// issues Hyperdex calls to store the node map
inline void
init_single_nmap(int thread_id, std::unordered_map<uint64_t, uint64_t> *node_map)
{
    WDEBUG << "node map thread " << thread_id << " starting, will put " << node_map->size() << " entries in hdex" << std::endl;
    nmap::nmap_stub node_mapper;
    node_mapper.put_mappings(*node_map);
    WDEBUG << "node map thread " << thread_id << " done" << std::endl;
}

inline void
init_nmap(std::vector<std::unordered_map<uint64_t, uint64_t>> *node_maps_ptr)
{
    auto &node_maps = *node_maps_ptr;
    int num_thr = node_maps.size();
    WDEBUG << "creating " << num_thr << " threads for node map inserts" << std::endl;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_thr; i++) {
        threads.push_back(std::thread(init_single_nmap, i, &node_maps[i]));
    }
    for (int i = 0; i < num_thr; i++) {
        threads[i].join();
    }
    WDEBUG << "all node map threads done" << std::endl;
}

// initial bulk graph loading method
// 'format' stores the format of the graph file
// 'graph_file' stores the full path filename of the graph file
inline void
load_graph(db::graph_file_format format, const char *graph_file)
{
    std::ifstream file;
    uint64_t node0, node1, loc, edge_id;
    std::string line, str_node;
    std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>> graph;

    file.open(graph_file, std::ifstream::in);
    if (!file) {
        WDEBUG << "File not found" << std::endl;
        return;
    }
    
    // read, validate, and create graph
    db::element::node *n;
    uint64_t line_count = 0;
    uint64_t edge_count = 1;
    uint64_t max_node_id = 0;
    std::unordered_set<uint64_t> seen_nodes;
    std::unordered_map<uint64_t, uint64_t> node_map;
    std::vector<std::unordered_map<uint64_t, uint64_t>> node_maps(NUM_THREADS, std::unordered_map<uint64_t, uint64_t>());
    vc::vclock zero_clk(0, 0);
    uint8_t thread_select = 0;

    switch(format) {

        case db::SNAP: {
            std::getline(file, line);
            assert(line.length() > 0 && line[0] == '#');
            char *max_node_ptr = new char[line.length()+1];
            std::strcpy(max_node_ptr, line.c_str());
            max_node_id = strtoull(++max_node_ptr, NULL, 10);

            while (std::getline(file, line)) {
                line_count++;
                if (line_count % 100000 == 0) {
                    WDEBUG << "bulk loading: processed " << line_count << " lines" << std::endl;
                }
                if ((line.length() == 0) || (line[0] == '#')) {
                    continue;
                } else {
                    parse_two_uint64(line, node0, node1);
                    edge_id = max_node_id + (edge_count++);
                    uint64_t loc0 = ((node0 % NumShards) + ShardIdIncr);
                    uint64_t loc1 = ((node1 % NumShards) + ShardIdIncr);
                    assert(loc0 < NumShards + ShardIdIncr);
                    assert(loc1 < NumShards + ShardIdIncr);
                    if (loc0 == shard_id) {
                        n = S->acquire_node_nonlocking(node0);
                        if (n == NULL) {
                            n = S->create_node(0, node0, zero_clk, false, true);
                            node_maps[thread_select][node0] = shard_id;
                            thread_select = (thread_select + 1) % NUM_THREADS;
                        }
                        S->create_edge_nonlocking(0, n, edge_id, node1, loc1, zero_clk, true);
                    }
                    if (loc1 == shard_id) {
                        if (!S->node_exists_nonlocking(node1)) {
                            S->create_node(0, node1, zero_clk, false, true);
                            node_maps[thread_select][node1] = shard_id;
                            thread_select = (thread_select + 1) % NUM_THREADS;
                        }
                    }
                }
            }
            //S->bulk_load_persistent();
            init_nmap(&node_maps);
            break;
        }

        case db::WEAVER: {
            std::unordered_map<uint64_t, uint64_t> all_node_map;
            std::getline(file, line);
            assert(line.length() > 0 && line[0] == '#');
            char *max_node_ptr = new char[line.length()+1];
            std::strcpy(max_node_ptr, line.c_str());
            max_node_id = strtoull(++max_node_ptr, NULL, 10);

            // nodes
            while (std::getline(file, line)) {
                parse_two_uint64(line, node0, loc);
                loc += ShardIdIncr;
                all_node_map[node0] = loc;
                assert(loc < NumShards + ShardIdIncr);
                if (loc == shard_id) {
                    n = S->acquire_node_nonlocking(node0);
                    if (n == NULL) {
                        n = S->create_node(0, node0, zero_clk, false, true);
                        node_maps[thread_select][node0] = shard_id;
                        thread_select = (thread_select + 1) % NUM_THREADS;
                    }
                }
                if (++line_count == max_node_id) {
                     WDEBUG << "Last node pos line: " << line << std::endl;
                     break;
                }
            }
            std::thread nmap_thr(init_nmap, &node_maps);

            // edges
            std::vector<std::pair<std::string, std::string>> props;
            while (std::getline(file, line)) {
                props.clear();
                parse_weaver_edge(line, node0, node1, props);
                edge_id = max_node_id + (edge_count++);
                uint64_t loc0 = all_node_map[node0];
                uint64_t loc1 = all_node_map[node1];
                if (loc0 == shard_id) {
                    n = S->acquire_node_nonlocking(node0);
                    assert(n != NULL);
                    S->create_edge_nonlocking(0, n, edge_id, node1, loc1, zero_clk, true);
                    for (auto &p: props) {
                        S->set_edge_property_nonlocking(n, edge_id, p.first, p.second, zero_clk);
                    }
                }
            }

            //S->bulk_load_persistent();
            nmap_thr.join();
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
    uint64_t node, old_loc, new_loc;
    msg->unpack_message(message::MIGRATED_NBR_UPDATE, node, old_loc, new_loc);
    S->update_migrated_nbr(node, old_loc, new_loc);
}

void
migrated_nbr_ack(uint64_t from_loc, std::vector<uint64_t> &target_req_id, uint64_t node_count)
{
    S->migration_mutex.lock();
    for (uint64_t i = 0; i < NumVts; i++) {
        if (S->target_prog_id[i] < target_req_id[i]) {
            S->target_prog_id[i] = target_req_id[i];
        }
    }
    S->migr_edge_acks[from_loc - ShardIdIncr] = true;
    S->shard_node_count[from_loc - ShardIdIncr] = node_count;
    S->migration_mutex.unlock();
}

void
unpack_migrate_request(db::hyper_stub *hs, void *req)
{
    db::message_wrapper *request = (db::message_wrapper*)req;

    switch (request->type) {
        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(std::move(request->msg));
            break;

        case message::MIGRATE_SEND_NODE:
            migrate_node_step2_resp(hs, std::move(request->msg));
            break;

        case message::MIGRATED_NBR_ACK: {
            uint64_t from_loc, node_count;
            std::vector<uint64_t> done_ids;
            request->msg->unpack_message(request->type, from_loc, done_ids, node_count);
            migrated_nbr_ack(from_loc, done_ids, node_count);
            break;
        }

        default:
            WDEBUG << "unknown type" << std::endl;
    }
    delete request;
}

void
apply_writes(db::hyper_stub *hs, uint64_t vt_id, uint64_t tx_id, vc::vclock &vclk, vc::qtimestamp_t &qts, transaction::pending_tx &tx)
{
    // apply all writes
    // acquire_node_write blocks if a preceding write has not been executed
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::EDGE_CREATE_REQ:
                create_edge(hs, vclk, qts, upd->id, upd->elem1, upd->elem2, upd->loc2);
                break;

            case transaction::NODE_DELETE_REQ:
                delete_node(hs, vclk, qts, upd->elem1);
                break;

            case transaction::EDGE_DELETE_REQ:
                delete_edge(hs, vclk, qts, upd->elem1, upd->elem2);
                break;

            case transaction::NODE_SET_PROPERTY:
                set_node_property(hs, vclk, qts, upd->elem1, std::move(upd->key), std::move(upd->value));
                break;

            case transaction::EDGE_SET_PROPERTY:
                set_edge_property(hs, vclk, qts, upd->elem1, upd->elem2, std::move(upd->key), std::move(upd->value));
                break;

            default:
                continue;
        }
    }

    // record clocks for future reads
    S->record_completed_tx(vclk);

    // send tx confirmation to coordinator
    message::message conf_msg;
    conf_msg.prepare_message(message::TX_DONE, tx_id);
    S->comm.send(vt_id, conf_msg.buf);
}

// if tx_order is PAST, then the tx has been ordered and should be stored in node
// if tx_order is present or future, then we can just go through regular ordering mechanism
void
reexec_tx_request(db::hyper_stub *hs, db::message_wrapper *request)
{
    uint64_t vt_id, tx_id;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    transaction::pending_tx tx;
    request->msg->unpack_message(message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);

    apply_writes(hs, vt_id, tx_id, vclk, qts, tx);

    delete request;
}

void
unpack_tx_request(db::hyper_stub *hs, void *req)
{
    db::message_wrapper *request = (db::message_wrapper*)req;
    uint64_t vt_id, tx_id;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    transaction::pending_tx tx;
    request->msg->unpack_message(message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);

    // execute all create_node writes
    // establish tx order at all graph nodes for all other writes
    db::element::node *n;
    for (auto upd: tx.writes) {
        n = NULL;
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                create_node(hs, vclk, upd->id);
                break;

            case transaction::EDGE_CREATE_REQ:
            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY: // elem1
                n = S->acquire_node(upd->elem1);
                break;

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY: // elem2
                n = S->acquire_node(upd->elem2);
                break;

            default:
                WDEBUG << "unknown type" << std::endl;
        }
        if (n != NULL) {
            bool already_ordered = false;
            for (auto &p: n->tx_queue) {
                if (p.first == vt_id && p.second == qts[vt_id]) {
                    // tx already exists in queue, due to some previous failure
                    already_ordered = true;
                    break;
                }
            }
            if (!already_ordered) {
                n->tx_queue.emplace_back(std::make_pair(vt_id, qts[vt_id]));
            }
            hs->update_tx_queue(*n);
            S->release_node(n);
        }
    }

    // increment qts so that threadpool moves forward
    // since tx order has already been established, conflicting txs will be processed in correct order
    S->increment_qts(hs, vt_id, tx.writes.size());

    apply_writes(hs, vt_id, tx_id, vclk, qts, tx);

    delete request;
}

void
check_nop(uint64_t , db::nop_data *nop_arg)
{
    // TODO
    S->record_completed_tx(nop_arg->vclk);
}

// process nop
// migration-related checks, and possibly initiating migration
inline void
nop(db::hyper_stub *hs, void *noparg)
{
    message::message msg;
    db::nop_data *nop_arg = (db::nop_data*)noparg;
    bool check_move_migr, check_init_migr, check_migr_step3;

    // increment qts
    S->increment_qts(hs, nop_arg->vt_id, 1);

    // note done progs for state clean up
    S->add_done_requests(nop_arg->done_reqs);

    // migration
    S->migration_mutex.lock();

    // increment nop count, trigger migration step 2 after check
    check_move_migr = true;
    check_init_migr = false;
    if (S->current_migr) {
        S->nop_count.at(nop_arg->vt_id)++;
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
    assert(S->max_done_id[nop_arg->vt_id] <= nop_arg->max_done_id);
    S->max_done_id[nop_arg->vt_id] = nop_arg->max_done_id;
    S->max_done_clk[nop_arg->vt_id] = std::move(nop_arg->max_done_clk);
    check_migr_step3 = check_step3();

    // atmost one check should be true
    assert(!(check_move_migr && check_init_migr)
        && !(check_init_migr && check_migr_step3)
        && !(check_move_migr && check_migr_step3));

    uint64_t cur_node_count = S->shard_node_count[shard_id - ShardIdIncr];
    for (uint64_t shrd = 0; shrd < NumShards; shrd++) {
        if ((shrd + ShardIdIncr) == shard_id) {
            continue;
        }
        S->shard_node_count[shrd] = nop_arg->shard_node_count[shrd];
    }
    S->migration_mutex.unlock();

    // initiate permanent deletion
    S->permanent_delete_loop(nop_arg->vt_id, nop_arg->outstanding_progs != 0);

    // record clock; reads go through
    S->record_completed_tx(nop_arg->vclk);

    // ack to VT
    msg.prepare_message(message::VT_NOP_ACK, shard_id, nop_arg->qts[shard_id-ShardIdIncr], cur_node_count);
    S->comm.send(nop_arg->vt_id, msg.buf);
    free(nop_arg);

    // call appropriate function based on check after acked to vt
    if (check_move_migr) {
        migrate_node_step2_req();
    } else if (check_init_migr) {
        migration_begin(hs);
    } else if (check_migr_step3) {
        migrate_node_step3(hs);
    }
}

node_prog::Node_State_Base*
get_state_if_exists(db::element::node &node, uint64_t req_id, node_prog::prog_type ptype)
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
    db::element::node *node,
    std::vector<uint64_t> *nodes_that_created_state)
{
    auto &state_map = node->prog_states[(int)ptype];
    auto state_iter = state_map.find(req_id);
    if (state_iter != state_map.end()) {
        return dynamic_cast<NodeStateType &>(*(state_iter->second));
    } else {
        NodeStateType *ptr = new NodeStateType();
        state_map[req_id] = std::unique_ptr<node_prog::Node_State_Base>(ptr);
        assert(nodes_that_created_state != NULL);
        nodes_that_created_state->emplace_back(node->base.get_id());
        return *ptr;
    }
}

// vector pointers can be null if we don't want to fill that vector
inline void
fill_changed_properties(std::vector<db::element::property> &props,
    std::vector<node_prog::property> *props_added,
    std::vector<node_prog::property> *props_deleted,
    vc::vclock &time_cached,
    vc::vclock &cur_time)
{
    for (db::element::property &prop : props) {
        bool del_before_cur = (order::compare_two_vts(prop.get_del_time(), cur_time) == 0);

        if (props_added != NULL) {
            bool creat_after_cached = (order::compare_two_vts(prop.get_creat_time(), time_cached) == 1);
            bool creat_before_cur = (order::compare_two_vts(prop.get_creat_time(), cur_time) == 0);

            if (creat_after_cached && creat_before_cur && !del_before_cur) {
                props_added->emplace_back(prop.key, prop.value);
            }
        }

        if (props_deleted != NULL) {
            bool del_after_cached = (order::compare_two_vts(prop.get_del_time(), time_cached) == 1);

            if (del_after_cached && del_before_cur) {
                props_deleted->emplace_back(prop.key, prop.value);
            }
        }
    }
}

// records all changes to nodes given in ids vector between time_cached and cur_time
// returns false if cache should be invalidated
inline bool
fetch_node_cache_contexts(uint64_t loc,
    std::vector<uint64_t>& ids,
    std::vector<node_prog::node_cache_context>& toFill,
    vc::vclock& time_cached,
    vc::vclock& cur_time)
{
    for (uint64_t id : ids) {
        db::element::node *node = S->acquire_node(id);
        if (node == NULL || node->state == db::element::node::mode::MOVED ||
                (node->last_perm_deletion != NULL && order::compare_two_vts(time_cached, *node->last_perm_deletion) == 0)) {
            if (node != NULL) {
                S->release_node(node);
            }
            WDEBUG << "node not found or migrated, invalidating cached value" << std::endl;
            toFill.clear(); // contexts aren't valid so don't send back
            return false;
        } else { // node exists
            if (order::compare_two_vts(node->base.get_del_time(), cur_time) == 0) { // node has been deleted
                toFill.emplace_back(loc, id, true);
            } else {
                node_prog::node_cache_context *context = NULL;
                std::vector<node_prog::property> temp_props_added;
                std::vector<node_prog::property> temp_props_deleted;

                fill_changed_properties(node->base.properties, &temp_props_added, &temp_props_deleted, time_cached, cur_time);
                // check for changes to node properties
                if (!temp_props_added.empty() || !temp_props_deleted.empty()) {
                    toFill.emplace_back(loc, id, false);
                    context = &toFill.back();

                    context->props_added = std::move(temp_props_added);
                    context->props_deleted = std::move(temp_props_deleted);
                    temp_props_added.clear();
                    temp_props_deleted.clear();
                }
                // now check for any edge changes
                for (auto &iter: node->out_edges) {
                    db::element::edge* e = iter.second;
                    assert(e != NULL);

                    bool del_after_cached = (order::compare_two_vts(time_cached, e->base.get_del_time()) == 0);
                    bool creat_after_cached = (order::compare_two_vts(time_cached, e->base.get_creat_time()) == 0);

                    bool del_before_cur = (order::compare_two_vts(e->base.get_del_time(), cur_time) == 0);
                    bool creat_before_cur = (order::compare_two_vts(e->base.get_creat_time(), cur_time) == 0);

                    if (creat_after_cached && creat_before_cur && !del_before_cur) {
                        WDEBUG << "edge created!" << std::endl;
                        if (context == NULL) {
                            toFill.emplace_back(loc, id, false);
                            context = &toFill.back();
                        }

                        context->edges_added.emplace_back(e->base.get_id(), e->nbr);
                        node_prog::edge_cache_context &edge_context = context->edges_added.back();
                        // don't care about props deleted before req time for an edge created after cache value was stored
                        fill_changed_properties(e->base.properties, &edge_context.props_added,
                                NULL, time_cached, cur_time);
                    } else if (del_after_cached && del_before_cur) {
                        if (context == NULL) {
                            toFill.emplace_back(loc, id, false);
                            context = &toFill.back();
                        }
                        context->edges_deleted.emplace_back(e->base.get_id(), e->nbr);
                        node_prog::edge_cache_context &edge_context = context->edges_deleted.back();
                        // don't care about props added after cache time on a deleted edge
                        fill_changed_properties(e->base.properties, NULL,
                                &edge_context.props_deleted, time_cached, cur_time);
                    } else if (del_after_cached && !creat_after_cached) {
                        // see if any properties changed on edge that didnt change
                        fill_changed_properties(e->base.properties, &temp_props_added,
                                &temp_props_deleted, time_cached, cur_time);
                        if (!temp_props_added.empty() || !temp_props_deleted.empty()) {
                            if (context == NULL) {
                                toFill.emplace_back(loc, id, false);
                                context = &toFill.back();
                            }
                            context->edges_modified.emplace_back(e->base.get_id(), e->nbr);

                            context->edges_modified.back().props_added = std::move(temp_props_added);
                            context->edges_modified.back().props_deleted = std::move(temp_props_deleted);

                            temp_props_added.clear();
                            temp_props_deleted.clear();
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
unpack_and_fetch_context(db::hyper_stub*, void *req)
{
    db::message_wrapper *request = (db::message_wrapper*) req;
    std::vector<uint64_t> ids;
    vc::vclock req_vclock, time_cached;
    uint64_t vt_id, req_id, from_shard;
    std::tuple<uint64_t, uint64_t, uint64_t> lookup_tuple;
    node_prog::prog_type pType;

    request->msg->unpack_message(message::NODE_CONTEXT_FETCH, pType, req_id, vt_id, req_vclock, time_cached, lookup_tuple, ids, from_shard);
    std::vector<node_prog::node_cache_context> contexts;

    bool cache_valid = fetch_node_cache_contexts(S->shard_id, ids, contexts, time_cached, req_vclock);

    message::message m;
    m.prepare_message(message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock, lookup_tuple, contexts, cache_valid);
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
inline bool cache_lookup(db::element::node*& node_to_check,
    uint64_t cache_key,
    node_prog::node_prog_running_state<ParamsType,
    NodeStateType,
    CacheValueType>& np,
    std::pair<uint64_t, ParamsType>& cur_node_params)
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
        std::shared_ptr<std::vector<db::element::remote_node>> watch_set = entry.watch_set;

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

        int64_t cmp_1 = order::compare_two_vts(*time_cached, *np.req_vclock);
        if (cmp_1 >= 1) { // cached value is newer or from this same request
            return true;
        }
        assert(cmp_1 == 0);

        if (watch_set->empty()) { // no context needs to be fetched
            np.cache_value.reset(new node_prog::cache_response<CacheValueType>(node_to_check->cache, cache_key, cval, watch_set));
            return true;
        }

        std::tuple<uint64_t, uint64_t, uint64_t> lookup_tuple(cache_key, np.req_id, node_to_check->base.get_id());

        S->node_prog_running_states_mutex.lock();
        if (S->node_prog_running_states.find(lookup_tuple) != S->node_prog_running_states.end()) {
            fetch_state<ParamsType, NodeStateType, CacheValueType> *fstate = (fetch_state<ParamsType, NodeStateType, CacheValueType> *) S->node_prog_running_states[lookup_tuple];
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
        S->node_prog_running_states[lookup_tuple] = fstate; 
        S->node_prog_running_states_mutex.unlock();

#ifdef weaver_debug_
        S->watch_set_lookups_mutex.lock();
        S->watch_set_lookups++;
        S->watch_set_lookups_mutex.unlock();
#endif

        // map from loc to list of ids on that shard we need context from for this request
        std::unordered_map<uint64_t, std::vector<uint64_t>> contexts_to_fetch; 

        for (db::element::remote_node& watch_node : *watch_set) {
            contexts_to_fetch[watch_node.loc].emplace_back(watch_node.id);
        }

        fstate->replies_left = contexts_to_fetch.size();
        fstate->prog_state.cache_value.swap(future_cache_response);
        fstate->prog_state.start_node_params.push_back(cur_node_params);
        fstate->cache_valid = true;
        assert(fstate->prog_state.start_node_params.size() == 1);
        fstate->monitor.unlock();


        for (uint64_t i = ShardIdIncr; i < NumShards + ShardIdIncr; i++) {
            if (contexts_to_fetch.find(i) != contexts_to_fetch.end()) {
                auto & context_list = contexts_to_fetch[i];
                assert(context_list.size() > 0);
                std::unique_ptr<message::message> m(new message::message());
                m->prepare_message(message::NODE_CONTEXT_FETCH, np.prog_type_recvd, np.req_id, np.vt_id, np.req_vclock, *time_cached, lookup_tuple, context_list, S->shard_id);
                S->comm.send(i, m->buf);
            }
        }
        return false;
    } 
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
inline void node_prog_loop(typename node_prog::node_function_type<ParamsType, NodeStateType, CacheValueType>::value_type func,
        node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> &np)
{
    message::message out_msg;
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::deque<std::pair<uint64_t, ParamsType>>> batched_node_progs;
    // node state function
    std::function<NodeStateType&()> node_state_getter;
    std::function<void(std::shared_ptr<CacheValueType>,
            std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)> add_cache_func;

    std::vector<uint64_t> nodes_that_created_state;

    uint64_t node_id;
    bool done_request = false;
    db::element::remote_node this_node(S->shard_id, 0);

    while (!done_request && !np.start_node_params.empty()) {
        auto &id_params = np.start_node_params.front();
        node_id = id_params.first;
        ParamsType& params = id_params.second;
        this_node.id = node_id;
        db::element::node *node = S->acquire_node(node_id);
        if (node == NULL || order::compare_two_vts(node->base.get_del_time(), *np.req_vclock)==0) {
            if (node != NULL) {
                S->release_node(node);
            } else {
                // node is being migrated here, but not yet completed
                std::vector<std::pair<uint64_t, ParamsType>> buf_node_params;
                buf_node_params.emplace_back(id_params);
                std::unique_ptr<message::message> m(new message::message());
                m->prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, buf_node_params);
                S->migration_mutex.lock();
                if (S->deferred_reads.find(node_id) == S->deferred_reads.end()) {
                    S->deferred_reads.emplace(node_id, std::vector<std::unique_ptr<message::message>>());
                }
                S->deferred_reads[node_id].emplace_back(std::move(m));
                WDEBUG << "Buffering read for node " << node_id << std::endl;
                S->migration_mutex.unlock();
            }
        } else if (node->state == db::element::node::mode::MOVED) {
            // queueing/forwarding node program
            //std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
            std::vector<std::pair<uint64_t, ParamsType>> fwd_node_params;
            fwd_node_params.emplace_back(id_params);
            std::unique_ptr<message::message> m(new message::message());
            m->prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, fwd_node_params);
            uint64_t new_loc = node->new_loc;
            S->release_node(node);
            S->comm.send(new_loc, m->buf);
        } else { // node does exist
            assert(node->state == db::element::node::mode::STABLE);
#ifdef WEAVER_NEW_CLDG
            assert(false && "new_cldg not supported right now");
                /*
                if (np.prev_server >= ShardIdIncr) {
                    node->msg_count[np.prev_server - ShardIdIncr]++;
                }
                */
#endif
            if (S->check_done_request(np.req_id)) {
                done_request = true;
                S->release_node(node);
                break;
            }

            if (MAX_CACHE_ENTRIES) {
                if (params.search_cache() && !np.cache_value) {
                    // cache value not already found, lookup in cache
                    bool run_prog_now = cache_lookup<ParamsType, NodeStateType, CacheValueType>(node, params.cache_key(), np, id_params);
                    if (!run_prog_now) { 
                        // go to next node while we fetch cache context for this one, cache_lookup releases node if false
                        np.start_node_params.pop_front();
                        continue;
                    }
                }

                using namespace std::placeholders;
                add_cache_func = std::bind(&db::element::node::add_cache_value, // function pointer
                    node, // reference to object whose member-function is invoked
                    np.req_vclock, // first argument of the function
                    _1, _2, _3); // 1 is cache value, 2 is watch set, 3 is key
            }

            node_state_getter = std::bind(get_or_create_state<NodeStateType>, np.prog_type_recvd, np.req_id, node, &nodes_that_created_state); 

            node->base.view_time = np.req_vclock; 
            assert(np.req_vclock);
            assert(np.req_vclock->clock.size() == NumVts);
            // call node program
            auto next_node_params = func(*node, this_node,
                    params, // actual parameters for this node program
                    node_state_getter, add_cache_func,
                    (node_prog::cache_response<CacheValueType>*) np.cache_value.get());
            if (MAX_CACHE_ENTRIES) {
                if (np.cache_value) {
                    auto state = get_state_if_exists(*node, np.req_id, np.prog_type_recvd);
                    if (state) {
                        state->contexts_found.insert(np.req_id);
                    }
                }
                np.cache_value.reset(NULL);
            }
            node->base.view_time = NULL; 
            S->release_node(node);
            np.start_node_params.pop_front(); // pop off this one before potentially add new front

            // batch the newly generated node programs for onward propagation
#ifdef WEAVER_CLDG
            std::unordered_map<uint64_t, uint32_t> agg_msg_count;
#endif
            for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params.second) {
                db::element::remote_node& rn = res.first; 
                assert(rn.loc < NumShards + ShardIdIncr);
                if (rn == db::element::coordinator || rn.loc == np.vt_id) {
                    // mark requests as done, will be done for other shards by no-ops from coordinator
                    std::vector<std::pair<uint64_t, node_prog::prog_type>> completed_request {std::make_pair(np.req_id, np.prog_type_recvd)};
                    S->add_done_requests(completed_request);
                    done_request = true;
                    // signal to send back to vector timestamper that issued request
                    std::unique_ptr<message::message> m(new message::message());
                    m->prepare_message(message::NODE_PROG_RETURN, np.prog_type_recvd, np.req_id, res.second);
                    S->comm.send(np.vt_id, m->buf);
                    break; // can only send one message back
                } else {
                    std::deque<std::pair<uint64_t, ParamsType>> &next_deque = (rn.loc == S->shard_id) ? np.start_node_params : batched_node_progs[rn.loc]; // TODO this is dumb just have a single data structure later
                    if (next_node_params.first == node_prog::search_type::DEPTH_FIRST) {
                        next_deque.emplace_front(rn.id, std::move(res.second));
                    } else { // BREADTH_FIRST
                        next_deque.emplace_back(rn.id, std::move(res.second));
                    }
#ifdef WEAVER_CLDG
                    agg_msg_count[node_id]++;
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
            // Only per hop batching
        }
        assert(batched_node_progs.size() < NumShards);
        for (auto &loc_progs_pair : batched_node_progs) {
            assert(loc_progs_pair.first != shard_id && loc_progs_pair.first < NumShards + ShardIdIncr);
            if (loc_progs_pair.second.size() > BATCH_MSG_SIZE) {
                out_msg.prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, loc_progs_pair.second);
                S->comm.send(loc_progs_pair.first, out_msg.buf);
                loc_progs_pair.second.clear();
            }
        }
        if (MAX_CACHE_ENTRIES) {
            assert(np.cache_value == false); // unique ptr is not assigned
        }
    }
    if (!done_request) {
        for (auto &loc_progs_pair : batched_node_progs) {
            if (!loc_progs_pair.second.empty()) {
                assert(loc_progs_pair.first != shard_id && loc_progs_pair.first < NumShards + ShardIdIncr);
                out_msg.prepare_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, loc_progs_pair.second);
                S->comm.send(loc_progs_pair.first, out_msg.buf);
                loc_progs_pair.second.clear();
            }
        }
    }

    if (!nodes_that_created_state.empty()) {
        S->mark_nodes_using_state(np.req_id, nodes_that_created_state);
    }
#ifdef WEAVER_MSG_COUNT
    S->meta_update_mutex.lock();
    S->msg_count += msg_count;
    S->meta_update_mutex.unlock();
#endif
}

void
unpack_node_program(db::hyper_stub*, void *req)
{
    db::message_wrapper *request = (db::message_wrapper*)req;
    node_prog::prog_type pType;

    request->msg->unpack_partial_message(message::NODE_PROG, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_and_run_db(std::move(request->msg));
    delete request;
}

void
unpack_context_reply(db::hyper_stub*, void *req)
{
    db::message_wrapper *request = (db::message_wrapper*)req;
    node_prog::prog_type pType;

    request->msg->unpack_partial_message(message::NODE_CONTEXT_REPLY, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_context_reply_db(std::move(request->msg));
    delete request;
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_context_reply_db(std::unique_ptr<message::message> msg)
{
    vc::vclock req_vclock;
    node_prog::prog_type pType;
    uint64_t req_id, vt_id;
    std::tuple<uint64_t, uint64_t, uint64_t> lookup_tuple;
    std::vector<node_prog::node_cache_context> contexts_to_add; 
    bool cache_valid;
    msg->unpack_message(message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock,
            lookup_tuple, contexts_to_add, cache_valid);

    S->node_prog_running_states_mutex.lock();
    auto lookup_iter = S->node_prog_running_states.find(lookup_tuple);
    assert(lookup_iter != S->node_prog_running_states.end());
    struct fetch_state<ParamsType, NodeStateType, CacheValueType> *fstate = (struct fetch_state<ParamsType, NodeStateType, CacheValueType> *) lookup_iter->second;
    fstate->monitor.lock();
    fstate->replies_left--;
    bool run_now = fstate->replies_left == 0;
    if (run_now) {
        //remove from map
        size_t num_erased = S->node_prog_running_states.erase(lookup_tuple);
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
        node_prog_loop<ParamsType, NodeStateType, CacheValueType>(enclosed_node_prog_func, fstate->prog_state);
        fstate->monitor.unlock();
        delete fstate;
    }
    else {
        fstate->monitor.unlock();
    }
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_run_db(std::unique_ptr<message::message> msg)
{
    node_prog::node_prog_running_state<ParamsType, NodeStateType, CacheValueType> np;

    // unpack the node program
    try {
        msg->unpack_message(message::NODE_PROG, np.prog_type_recvd, np.vt_id, np.req_vclock, np.req_id, np.start_node_params);
        assert(np.req_vclock->clock.size() == NumVts);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        assert(false);
        return;
    }

    // update max prog id
    S->migration_mutex.lock();
    if (S->max_prog_id[np.vt_id] < np.req_id) {
        S->max_prog_id[np.vt_id] = np.req_id;
    }
    S->migration_mutex.unlock();

    // check if request completed
    if (S->check_done_request(np.req_id)) {
        return; // done request
    }

    assert(!np.cache_value); // a cache value should not be allocated yet
    node_prog_loop<ParamsType, NodeStateType, CacheValueType>(enclosed_node_prog_func, np);
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void
node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: unpack_and_start_coord(std::unique_ptr<message::message>, uint64_t, nmap::nmap_stub*)
{ }

// delete all in-edges for a permanently deleted node
inline void
update_deleted_node(db::hyper_stub*, void *node)
{
    uint64_t *node_id = (uint64_t*)node;
    std::unordered_set<uint64_t> nbrs;
    db::element::node *n;
    db::element::edge *e;
    S->edge_map_mutex.lock();
    if (S->edge_map.find(*node_id) != S->edge_map.end()) {
        nbrs = std::move(S->edge_map[*node_id]);
        S->edge_map.erase(*node_id);
    }
    S->edge_map_mutex.unlock();

    std::vector<uint64_t> to_del;
    bool found;
    for (uint64_t nbr: nbrs) {
        n = S->acquire_node(nbr);
        to_del.clear();
        found = false;
        for (auto &x: n->out_edges) {
            e = x.second;
            if (e->nbr.id == *node_id) {
                delete e;
                to_del.emplace_back(x.first);
                found = true;
            }
        }
        assert(found);
        UNUSED(found);
        for (uint64_t del_edge: to_del) {
            n->out_edges.erase(del_edge);
        }
        S->release_node(n);
    }
}

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
    int ret_idx = rand() % min_indices.size();
    return min_indices[ret_idx];
}

// decide node migration shard based on migration score
// mark node as "moved" so that subsequent requests are queued up
// send migration information to coordinator mapper
// return false if no migration happens (max migr score = this shard), else return true
bool
migrate_node_step1(db::hyper_stub *hs,
    db::element::node *n,
    std::vector<uint64_t> &shard_node_count)
{
    // find arg max migr score
    uint64_t max_pos = shard_id - ShardIdIncr; // don't migrate if all equal
    std::vector<uint32_t> max_indices(1, max_pos);
    for (uint32_t j = 0; j < NumShards; j++) {
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
    n->state = db::element::node::mode::MOVED;
    n->new_loc = migr_loc;
    S->migr_node = n->base.get_id();
    S->migr_shard = migr_loc;

    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &e: n->out_edges) {
        uint64_t node = e.second->nbr.id;
        assert(S->edge_map.find(node) != S->edge_map.end());
        auto &node_set = S->edge_map[node];
        node_set.erase(S->migr_node);
        if (node_set.empty()) {
            S->edge_map.erase(node);
        }
    }
    S->edge_map_mutex.unlock();

    // persist status
    hs->update_migr_status(S->migr_node, db::MOVING);

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
    db::element::node *n;
    message::message msg;

    S->migration_mutex.lock();
    S->current_migr = false;
    for (uint64_t idx = 0; idx < NumVts; idx++) {
        S->target_prog_id[idx] = 0;
    }
    S->migration_mutex.unlock();

    n = S->acquire_node(S->migr_node);
    assert(n != NULL);
    msg.prepare_message(message::MIGRATE_SEND_NODE, S->migr_node, shard_id, *n);
    S->release_node(n);
    S->comm.send(S->migr_shard, msg.buf);
}

// receive and place node which has been migrated to this shard
// apply buffered reads and writes to node
// update nbrs of migrated nbrs
void
migrate_node_step2_resp(db::hyper_stub *hs, std::unique_ptr<message::message> msg)
{
    // unpack and place node
    uint64_t from_loc;
    uint64_t node_id;
    db::element::node *n;

    // create a new node, unpack the message
    vc::vclock dummy_clock;
    msg->unpack_partial_message(message::MIGRATE_SEND_NODE, node_id);
    n = S->create_node(hs, node_id, dummy_clock, true); // node will be acquired on return
    try {
        msg->unpack_message(message::MIGRATE_SEND_NODE, node_id, from_loc, *n);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        return;
    }

    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &e: n->out_edges) {
        uint64_t node = e.second->nbr.id;
        S->edge_map[node].emplace(node_id);
    }
    S->edge_map_mutex.unlock();

    S->migration_mutex.lock();
    // apply buffered writes
    if (S->deferred_writes.find(node_id) != S->deferred_writes.end()) {
        for (auto &dw: S->deferred_writes[node_id]) {
            switch (dw.type) {
                case message::NODE_DELETE_REQ:
                    S->delete_node_nonlocking(hs, n, dw.vclk);
                    break;

                case message::EDGE_CREATE_REQ:
                    S->create_edge_nonlocking(hs, n, dw.edge, dw.remote_node, dw.remote_loc, dw.vclk);
                    break;

                case message::EDGE_DELETE_REQ:
                    S->delete_edge_nonlocking(hs, n, dw.edge, dw.vclk);
                    break;

                default:
                    WDEBUG << "unexpected type" << std::endl;
            }
        }
        S->deferred_writes.erase(node_id);
    }

    // update nbrs
    for (uint64_t upd_shard = ShardIdIncr; upd_shard < ShardIdIncr + NumShards; upd_shard++) {
        if (upd_shard == shard_id) {
            continue;
        }
        msg->prepare_message(message::MIGRATED_NBR_UPDATE, node_id, from_loc, shard_id);
        S->comm.send(upd_shard, msg->buf);
    }
    n->state = db::element::node::mode::STABLE;
    hs->update_migr_status(node_id, db::STABLE);

    std::vector<uint64_t> prog_state_reqs;
    for (auto &state_map: n->prog_states) {
        for (auto &state: state_map) {
            prog_state_reqs.emplace_back(state.first);
        }
    }

    // release node for new reads and writes
    S->release_node(n);

    std::vector<uint64_t> this_node_vec(1, node_id);
    for (uint64_t req_id: prog_state_reqs) {
        S->mark_nodes_using_state(req_id, this_node_vec);
    }

    // move deferred reads to local for releasing migration_mutex
    std::vector<std::unique_ptr<message::message>> deferred_reads;
    if (S->deferred_reads.find(node_id) != S->deferred_reads.end()) {
        deferred_reads = std::move(S->deferred_reads[node_id]);
        S->deferred_reads.erase(node_id);
    }
    S->migration_mutex.unlock();

    // update local nbrs
    S->update_migrated_nbr(node_id, from_loc, shard_id);

    // apply buffered reads
    for (auto &m: deferred_reads) {
        node_prog::prog_type pType;
        m->unpack_partial_message(message::NODE_PROG, pType);
        WDEBUG << "APPLYING BUFREAD for node " << node_id << std::endl;
        assert(node_prog::programs.find(pType) != node_prog::programs.end());
        node_prog::programs[pType]->unpack_and_run_db(std::move(m));
    }
}

// check if all nbrs updated, if so call step3
// caution: assuming caller holds S->migration_mutex
bool
check_step3()
{
    bool init_step3 = weaver_util::all(S->migr_edge_acks);
    for (uint64_t i = 0; i < NumVts && init_step3; i++) {
        init_step3 = (S->target_prog_id[i] <= S->max_done_id[i]);
    }
    if (init_step3) {
        weaver_util::reset_all(S->migr_edge_acks);
    }
    return init_step3;
}

// successfully migrated node to new location, continue migration process
void
migrate_node_step3(db::hyper_stub *hs)
{
    S->delete_migrated_node(S->migr_node);
    migration_wrapper(hs);
}

inline bool
check_migr_node(db::element::node *n)
{
    if (n == NULL || order::compare_two_clocks(n->base.get_del_time().clock, S->max_clk.clock) != 2 ||
        n->state == db::element::node::mode::MOVED || n->already_migr) {
        if (n != NULL) {
            n->already_migr = false;
            S->release_node(n);
        }
        return false;
    } else {
        for (double &x: n->migr_score) {
            x = 0;
        }
        return true;
    }
}

// stream list of nodes and cldg repartition
inline void
cldg_migration_wrapper(db::hyper_stub *hs, std::vector<uint64_t> &shard_node_count, uint64_t shard_cap)
{
    bool no_migr = true;
    while (S->cldg_iter != S->cldg_nodes.end()) {
        db::element::node *n;
        uint64_t migr_node = S->cldg_iter->first;
        S->cldg_iter++;
        n = S->acquire_node(migr_node);

        // check if okay to migrate
        if (!check_migr_node(n)) {
            continue;
        }

        // communication-LDG
#ifdef WEAVER_CLDG
        for (uint64_t &cnt: n->msg_count) {
            cnt = 0;
        }

        db::element::edge *e;
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
#endif

        // update migration score based on CLDG
        for (uint64_t j = 0; j < NumShards; j++) {
            double penalty = 1.0 - ((double)shard_node_count[j])/shard_cap;
            n->migr_score[j] = n->msg_count[j] * penalty;
        }

        if (migrate_node_step1(hs, n, shard_node_count)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        migration_end(hs);
    }
}

// stream list of nodes and ldg repartition
inline void
ldg_migration_wrapper(db::hyper_stub *hs, std::vector<uint64_t> &shard_node_count, uint64_t shard_cap)
{
    bool no_migr = true;
    while (S->ldg_iter != S->ldg_nodes.end()) {
        db::element::node *n;
        uint64_t migr_node = *S->ldg_iter;
        S->ldg_iter++;
        n = S->acquire_node(migr_node);

        // check if okay to migrate
        if (!check_migr_node(n)) {
            continue;
        }

        // regular LDG
        db::element::edge *e;
        for (auto &e_iter: n->out_edges) {
            e = e_iter.second;
            n->migr_score[e->nbr.loc - ShardIdIncr] += 1;
        }
        for (uint64_t j = 0; j < NumShards; j++) {
            n->migr_score[j] *= (1 - ((double)shard_node_count[j])/shard_cap);
        }

        if (migrate_node_step1(hs, n, shard_node_count)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        migration_end(hs);
    }
}

// sort nodes in order of number of requests propagated
void
migration_wrapper(db::hyper_stub *hs)
{
    S->migration_mutex.lock();
    std::vector<uint64_t> shard_node_count = S->shard_node_count;
    float total_node_count = 0;
    for (uint64_t c: shard_node_count) {
        total_node_count += c;
    }
    uint64_t shard_cap = (uint64_t)(1.1 * total_node_count);
    S->migration_mutex.unlock();

#ifdef WEAVER_CLDG
    cldg_migration_wrapper(hs, shard_node_count, shard_cap);
#endif

#ifdef WEAVER_NEW_CLDG
    cldg_migration_wrapper(hs, shard_node_count, shard_cap);
#else
    ldg_migration_wrapper(hs, shard_node_count, shard_cap);
#endif
}

void
migration_begin(db::hyper_stub *hs)
{
    S->meta_update_mutex.lock();
    S->ldg_nodes = S->node_list;
    S->meta_update_mutex.unlock();

#ifdef WEAVER_CLDG
    S->msg_count_mutex.lock();
    auto agg_msg_count = std::move(S->agg_msg_count);
    S->msg_count_mutex.unlock();
    std::vector<std::pair<uint64_t, uint32_t>> sorted_nodes;
    for (uint64_t n: S->ldg_nodes) {
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
    for (uint64_t nid: S->ldg_nodes) {
        mcnt = 0;
        db::element::node *n = S->acquire_node(nid);
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

    migration_wrapper(hs);
}

// method to sort pairs based on second coordinate
bool agg_count_compare(std::pair<uint64_t, uint32_t> p1, std::pair<uint64_t, uint32_t> p2)
{
    return (p1.second > p2.second);
}

void
migration_end(db::hyper_stub *hs)
{
    hs->update_migr_token(db::INACTIVE); // persist token

    message::message msg;
    S->migration_mutex.lock();
    S->migr_token = false;
    msg.prepare_message(message::MIGRATION_TOKEN, --S->migr_token_hops, S->migr_vt);
    S->migration_mutex.unlock();
    uint64_t next_id; 
    if ((shard_id + 1 - ShardIdIncr) >= NumShards) {
        next_id = ShardIdIncr;
    } else {
        next_id = shard_id + 1;
    }
    S->comm.send(next_id, msg.buf);
}

// server msg recv loop for the shard server
void
recv_loop(uint64_t thread_id)
{
    uint64_t vt_id, req_id;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::queued_request *qreq;
    db::message_wrapper *mwrap;
    node_prog::prog_type pType;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    db::hyper_stub *hs = S->hstub[thread_id];

    busybee_returncode bb_code;
    while (true) {
        S->comm.quiesce_thread(thread_id);
        bb_code = S->comm.recv(&rec_msg->buf);
        if (bb_code != BUSYBEE_SUCCESS && bb_code != BUSYBEE_TIMEOUT) {
            continue;
        }

        if (bb_code == BUSYBEE_SUCCESS) {
            // exec or enqueue this request
            auto unpacker = rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE);
            unpack_buffer(unpacker, mtype);
            rec_msg->change_type(mtype);
            //sender -= ID_INCR;
            vclk.clock.clear();
            qts.clear();

            switch (mtype) {
                case message::TX_INIT: {
                    rec_msg->unpack_partial_message(message::TX_INIT, vt_id, vclk, qts);
                    assert(qts.size() == NumShards);
                    assert(vclk.clock.size() == NumVts);
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    enum db::queue_order tx_order = S->qm.check_wr_request(vclk, qts[shard_id-ShardIdIncr]);
                    if (tx_order == db::PRESENT) {
                        unpack_tx_request(hs, (void*)mwrap);
                    } else if (tx_order == db::PAST) {
                        // vt resending tx which has been executed
                        reexec_tx_request(hs, mwrap);
                    } else {
                        // enqueue for future execution
                        qreq = new db::queued_request(qts[shard_id-ShardIdIncr], vclk, unpack_tx_request, mwrap);
                        S->qm.enqueue_write_request(vt_id, qreq);
                    }
                    break;
                }

                case message::NODE_PROG:
                    rec_msg->unpack_partial_message(message::NODE_PROG, pType, vt_id, vclk, req_id);
                    assert(vclk.clock.size() == NumVts);
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    if (S->qm.check_rd_request(vclk.clock)) {
                        unpack_node_program(hs, (void*)mwrap);
                    } else {
                        qreq = new db::queued_request(req_id, vclk, unpack_node_program, mwrap);
                        S->qm.enqueue_read_request(vt_id, qreq);
                    }
                    break;

                case message::NODE_CONTEXT_FETCH:
                case message::NODE_CONTEXT_REPLY: {
                    void (*f)(db::hyper_stub*, void*);
                    if (mtype == message::NODE_CONTEXT_FETCH) {
                        f = unpack_and_fetch_context;
                    } else { // NODE_CONTEXT_REPLY
                        f = unpack_context_reply;
                    }
                    rec_msg->unpack_partial_message(mtype, pType, req_id, vt_id, vclk);
                    assert(vclk.clock.size() == NumVts);
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    if (S->qm.check_rd_request(vclk.clock)) {
                        f(hs, mwrap);
                    } else {
                        qreq = new db::queued_request(req_id, vclk, f, mwrap);
                        S->qm.enqueue_read_request(vt_id, qreq);
                    }
                    break;
                }

                case message::VT_NOP: {
                    db::nop_data *nop_arg = new db::nop_data();
                    rec_msg->unpack_message(mtype, vt_id, vclk, qts, nop_arg->req_id,
                        nop_arg->done_reqs, nop_arg->max_done_id, nop_arg->max_done_clk,
                        nop_arg->outstanding_progs, nop_arg->shard_node_count);
                    nop_arg->vt_id = vt_id;
                    nop_arg->vclk = vclk;
                    nop_arg->qts = qts;
                    assert(nop_arg->vclk.clock.size() == NumVts);
                    assert(nop_arg->max_done_clk.size() == NumVts);
                    enum db::queue_order tx_order = S->qm.check_wr_request(vclk, qts[shard_id-ShardIdIncr]);
                    assert(tx_order != db::PAST);
                    // nop goes through queues for both PRESENT and FUTURE
                    qreq = new db::queued_request(qts[shard_id-ShardIdIncr], nop_arg->vclk, nop, (void*)nop_arg);
                    S->qm.enqueue_write_request(vt_id, qreq);
                    break;
                }

                case message::PERMANENTLY_DELETED_NODE: {
                    uint64_t *node = (uint64_t*)malloc(sizeof(uint64_t));
                    rec_msg->unpack_message(mtype, *node);
                    update_deleted_node(hs, (void*)node);
                    break;
                }

                case message::MIGRATE_SEND_NODE:
                case message::MIGRATED_NBR_UPDATE:
                case message::MIGRATED_NBR_ACK:
                    mwrap = new db::message_wrapper(mtype, std::move(rec_msg));
                    unpack_migrate_request(hs, (void*)mwrap);
                    break;

                case message::MIGRATION_TOKEN:
                    hs->update_migr_token(db::ACTIVE);
                    S->migration_mutex.lock();
                    rec_msg->unpack_message(mtype, S->migr_token_hops, S->migr_vt);
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
                    if (++S->load_count == NumShards) {
                        WDEBUG << "Loaded graph on all shards, time taken = " << (S->max_load_time/MEGA) << " ms." << std::endl;
                    } else {
                        WDEBUG << "Loaded graph on " << S->load_count << " shards, current time "
                                << (S->max_load_time/MEGA) << "ms." << std::endl;
                    }
                    S->graph_load_mutex.unlock();
                    break;
                }

                case message::MSG_COUNT: {
                    rec_msg->unpack_message(message::MSG_COUNT, vt_id);
                    S->meta_update_mutex.lock();
                    rec_msg->prepare_message(message::MSG_COUNT, shard_id, S->msg_count);
                    S->msg_count = 0;
                    S->meta_update_mutex.unlock();
                    S->comm.send(vt_id, rec_msg->buf);
                    break;
                }
                
                case message::SET_QTS: {
                    uint64_t rec_qts;
                    rec_msg->unpack_message(message::SET_QTS, vt_id, rec_qts);
                    WDEBUG << "got set qts from vt " << vt_id << ", qts " << rec_qts << std::endl;
                    S->restore_mutex.lock();
                    while (!S->restore_done) {
                        S->restore_cv.wait();
                    }
                    S->qm.set_qts(vt_id, rec_qts);
                    S->restore_mutex.unlock();
                    break;
                }

                case message::EXIT_WEAVER:
                    exit(0);
                    
                default:
                    WDEBUG << "unexpected msg type " << mtype << std::endl;
            }
            rec_msg.reset(new message::message());
        }

        // execute all queued requests that can be executed now
        // will break from loop when no more requests can be executed, in which case we need to recv
        while (S->qm.exec_queued_request(hs));
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
server_manager_link_loop(po6::net::hostname sm_host, po6::net::location my_loc)
{
    // Most of the following code has been 'borrowed' from
    // Robert Escriva's HyperDex.
    // see https://github.com/rescrv/HyperDex for the original code.

    S->sm_stub.set_server_manager_address(sm_host.address.c_str(), sm_host.port);

    if (!S->sm_stub.register_id(S->server, my_loc, 0))
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
        S->config = new_config;
        if (!S->first_config) {
            S->first_config = true;
            S->first_config_cond.signal();
        } else {
            while (!S->shard_init) {
                S->shard_init_cond.wait();
            }
            S->reconfigure();
        }
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
    else if (S->sm_stub.should_exit() && !S->sm_stub.config().exists(S->server))
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

int
main(int argc, const char *argv[])
{
    // signal handlers
    install_signal_handler(SIGINT, end_program);
    install_signal_handler(SIGHUP, end_program);
    install_signal_handler(SIGTERM, end_program);
    install_signal_handler(SIGTSTP, end_program);

    // signals
    sigset_t ss;
    if (sigfillset(&ss) < 0) {
        WDEBUG << "sigfillset failed" << std::endl;
        return -1;
    }
    //sigdelset(&ss, SIGPROF);
    //sigdelset(&ss, SIGPROF);
    //sigdelset(&ss, SIGINT);
    //sigdelset(&ss, SIGTSTP);
    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0) {
        WDEBUG << "pthread sigmask failed" << std::endl;
        return -1;
    }

    // command line params
    const char* listen_host = "auto";
    long listen_port = 5000;
    const char *config_file = "/usr/local/etc/weaver.yaml";
    const char *graph_file = NULL;
    const char *graph_format = "snap";
    long backup_input = LONG_MAX;
    // arg parsing borrowed from HyperDex
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: auto)")
            .metavar("IP").as_string(&listen_host);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 5000)")
            .metavar("port").as_long(&listen_port);
    ap.arg().name('b', "backup-number")
            .description("backup number (not backup by default)")
            .metavar("num").as_long(&backup_input);
    ap.arg().long_name("config-file")
            .description("full path of weaver.yaml configuration file (default /usr/local/etc/weaver.yaml)")
            .metavar("filename").as_string(&config_file);
    ap.arg().long_name("graph-file")
            .description("full path of bulk load input graph file (no default)")
            .metavar("filename").as_string(&graph_file);
    ap.arg().long_name("graph-format")
            .description("bulk load input graph format (default snap)")
            .metavar("filename").as_string(&graph_format);

    if (!ap.parse(argc, argv) || ap.args_sz() != 0) {
        WDEBUG << "args parsing failure" << std::endl;
        return -1;
    }

    // configuration file parse
    init_config_constants(config_file);

    // init shard, also check cmdline params
    //shard_id = (uint64_t)sid_input + ShardIdIncr;
    //if (shard_id >= NumEffectiveServers) {
    //    WDEBUG << "bad shard id " << shard_id << std::endl;
    //    return -1;
    //}

    //uint64_t server_id = shard_id;

    if (backup_input != LONG_MAX) {
        assert(graph_file == NULL);
        // backup shard
        if ((uint64_t)backup_input > NumBackups) {
            WDEBUG << "bad backup number" << std::endl;
            return -1;
        }
        //server_id = shard_id + NumEffectiveServers*backup_input;
    }

    //S = new db::shard(shard_id, server_id);
    po6::net::location my_loc(listen_host, listen_port);
    uint64_t sid;
    assert(generate_token(&sid));
    S = new db::shard(sid, my_loc);

    // server manager link
    std::thread sm_thr(server_manager_link_loop,
        po6::net::hostname(SERVER_MANAGER_IPADDR, SERVER_MANAGER_PORT),
        my_loc);
    sm_thr.detach();

    S->config_mutex.lock();

    // wait for first config to arrive from server_manager
    while (!S->first_config) {
        S->first_config_cond.wait();
    }

    std::vector<std::pair<server_id, po6::net::location>> addresses;
    S->config.get_all_addresses(&addresses);
    shard_id = UINT64_MAX;
    for (auto &p: addresses) {
        if (p.second == my_loc) {
            uint64_t wid = S->config.get_weaver_id(p.first);
            assert(S->config.get_shard_or_vt(p.first));
            shard_id = wid + NumVts;
        }
    }
    assert(shard_id != UINT64_MAX);

    // registered this server with server_manager, config has fairly recent value
    if (backup_input == LONG_MAX) {
        S->init(shard_id, false); // primary
    } else {
        S->init(shard_id, true); // backup
    }

    S->shard_init = true;
    S->shard_init_cond.signal();
    S->config_mutex.unlock();

    // start all threads
    std::vector<std::thread*> worker_threads;
    for (int i = 0; i < NUM_THREADS; i++) {
        std::thread *t = new std::thread(recv_loop, i);
        worker_threads.emplace_back(t);
    }

    // bulk loading
    if (graph_file != NULL) {
        db::graph_file_format format = db::SNAP;
        if (strcmp(graph_format, "tsv") == 0) {
            format = db::TSV;
        } else if (strcmp(graph_format, "snap") == 0) {
            format = db::SNAP;
        } else if (strcmp(graph_format, "weaver") == 0) {
            format = db::WEAVER;
        } else {
            WDEBUG << "Invalid graph file format" << std::endl;
        }

        wclock::weaver_timer timer;
        uint64_t load_time = timer.get_time_elapsed();
        load_graph(format, graph_file);
        load_time = timer.get_time_elapsed() - load_time;
        message::message msg;
        msg.prepare_message(message::LOADED_GRAPH, load_time);
        S->comm.send(ShardIdIncr, msg.buf);
    }

    if (backup_input != LONG_MAX) {
        // wait till this server becomes primary shard
        S->config_mutex.lock();
        while (!S->active_backup) {
            S->backup_cond.wait();
        }
        S->config_mutex.unlock();
    } else {
        // this server is primary shard, start now
        std::cout << "Weaver: shard instance " << S->shard_id << std::endl;
    }

    for (auto t: worker_threads) {
        t->join();
    }

    return 0;
}

#undef weaver_debug_
