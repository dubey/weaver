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

#include <iostream>
#include <string>
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#define __WEAVER_DEBUG__
#include "common/weaver_constants.h"
#include "common/event_order.h"
#include "common/message_cache_context.h"
#include "common/message_graph_elem.h"
#include "common/nmap_stub.h"
#include "shard.h"
#include "nop_data.h"
#include "db/cache/prog_cache.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
//#include "node_prog/triangle_program.h"

// global static variables
static uint64_t shard_id;
// shard pointer for shard.cc
static db::shard *S;
// for Hyperdex entries while loading graph from file
static po6::threads::mutex init_mutex;
static po6::threads::cond init_cv(&init_mutex);
static po6::threads::cond start_load_cv(&init_mutex);
static std::deque<std::unordered_map<uint64_t, uint64_t>> init_node_maps;
static bool init_nodes;
static uint16_t start_load;
// threadpool shard pointer
db::shard *db::thread::pool::S = NULL; // reinitialized in graph constructor

void migrated_nbr_update(std::unique_ptr<message::message> msg);
bool migrate_node_step1(db::element::node*, std::vector<uint64_t>&);
void migrate_node_step2_req();
void migrate_node_step2_resp(std::unique_ptr<message::message> msg);
bool check_step3();
void migrate_node_step3();
void migration_wrapper();
void shard_daemon_begin();
void shard_daemon_end();

// SIGINT handler
void
end_program(int param)
{
    WDEBUG << "Ending program, param = " << param << ", kronos num calls " << order::call_times->size()
        << ", kronos num cache hits = " << order::cache_hits << std::endl;
    //std::ofstream ktime("kronos_time.rec");
    //for (auto x: *order::call_times) {
    //    ktime << x << std::endl;
    //}
    //ktime.close();
    exit(0);
}


inline void
create_node(vc::vclock &t_creat, uint64_t node_handle)
{
    S->create_node(node_handle, t_creat, false);
}

inline void
create_edge(vc::vclock &t_creat, vc::qtimestamp_t &qts, uint64_t edge_handle, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    S->create_edge(edge_handle, n1, n2, loc2, t_creat, qts);
}

inline void
delete_node(vc::vclock &t_del, vc::qtimestamp_t &qts, uint64_t node_handle)
{
    S->delete_node(node_handle, t_del, qts);
}

inline void
delete_edge(vc::vclock &t_del, vc::qtimestamp_t &qts, uint64_t edge_handle, uint64_t node_handle)
{
    S->delete_edge(edge_handle, node_handle, t_del, qts);
}

inline void
set_node_property(vc::vclock &vclk, vc::qtimestamp_t &qts, uint64_t node_handle, std::unique_ptr<std::string> key,
    std::unique_ptr<std::string> value)
{
    S->set_node_property(node_handle, std::move(key), std::move(value), vclk, qts);
}

inline void
set_edge_property(vc::vclock &vclk, vc::qtimestamp_t &qts, uint64_t edge_handle, uint64_t node_handle,
    std::unique_ptr<std::string> key, std::unique_ptr<std::string> value)
{
    S->set_edge_property(node_handle, edge_handle, std::move(key), std::move(value), vclk, qts);
}

// parse the string 'line' as a uint64_t starting at index 'idx' till the first whitespace or end of string
// store result in 'n'
// if overflow occurs or unexpected char encountered, store true in 'bad'
inline void
parse_single_uint64(std::string &line, size_t &idx, uint64_t &n, bool &bad)
{
    uint64_t next_digit;
    static uint64_t zero = '0';
    static uint64_t max64_div10 = MAX_UINT64 / 10;
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
            break;
        }
        n *= 10;
        if ((n + next_digit) < n) { // addition overflow
            bad = true;
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
        WDEBUG << "Parsing error" << std::endl;
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

// initial bulk graph loading method
// 'format' stores the format of the graph file
// 'graph_file' stores the full path filename of the graph file
inline void
load_graph(db::graph_file_format format, const char *graph_file)
{
    std::ifstream file;
    uint64_t node0, node1, loc, edge_handle;
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
    uint64_t max_node_handle = 0;
    std::unordered_set<uint64_t> seen_nodes;
    std::unordered_map<uint64_t, uint64_t> node_map;
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
                if ((line.length() == 0) || (line[0] == '#')) {
                    continue;
                } else {
                    parse_two_uint64(line, node0, node1);
                    edge_handle = max_node_handle + (edge_count++);
                    uint64_t loc0 = ((node0 % NUM_SHARDS) + SHARD_ID_INCR);
                    uint64_t loc1 = ((node1 % NUM_SHARDS) + SHARD_ID_INCR);
                    assert(loc0 < NUM_SHARDS + SHARD_ID_INCR);
                    assert(loc1 < NUM_SHARDS + SHARD_ID_INCR);
                    if (loc0 == shard_id) {
                        n = S->acquire_node_nonlocking(node0);
                        if (n == NULL) {
                            n = S->create_node(node0, zero_clk, false, true);
                            node_map[node0] = shard_id;
                        }
                        S->create_edge_nonlocking(n, edge_handle, node1, loc1, zero_clk, true);
                    }
                    if (loc1 == shard_id) {
                        if (!S->node_exists_nonlocking(node1)) {
                            S->create_node(node1, zero_clk, false, true);
                            node_map[node1] = shard_id;
                        }
                    }
                    if (node_map.size() > 100000) {
                        init_mutex.lock();
                        init_node_maps.emplace_back(std::move(node_map));
                        init_cv.broadcast();
                        init_mutex.unlock();
                        node_map.clear();
                    }
                }
            }
            init_mutex.lock();
            while (start_load < 1) {
                start_load_cv.wait();
            }
            init_node_maps.emplace_back(std::move(node_map));
            init_nodes = true;
            init_cv.broadcast();
            init_mutex.unlock();
            break;
        }

        case db::WEAVER: {
            std::unordered_map<uint64_t, uint64_t> all_node_map;
            std::getline(file, line);
            assert(line.length() > 0 && line[0] == '#');
            char *max_node_ptr = new char[line.length()+1];
            std::strcpy(max_node_ptr, line.c_str());
            max_node_handle = strtoull(++max_node_ptr, NULL, 10);
            // nodes
            while (std::getline(file, line)) {
                parse_two_uint64(line, node0, loc);
                loc += SHARD_ID_INCR;
                all_node_map[node0] = loc;
                assert(loc < NUM_SHARDS + SHARD_ID_INCR);
                if (loc == shard_id) {
                    n = S->acquire_node_nonlocking(node0);
                    if (n == NULL) {
                        n = S->create_node(node0, zero_clk, false, true);
                        node_map[node0] = shard_id;
                    }
                }
                if (node_map.size() > 100000) {
                    init_mutex.lock();
                    init_node_maps.emplace_back(std::move(node_map));
                    init_cv.broadcast();
                    init_mutex.unlock();
                    node_map.clear();
                }
                if (++line_count == max_node_handle) {
                     WDEBUG << "Last node pos line: " << line << std::endl;
                     break;
                }
            }
            init_mutex.lock();
            while (start_load < 1) {
                start_load_cv.wait();
            }
            init_node_maps.emplace_back(std::move(node_map));
            init_nodes = true;
            init_cv.broadcast();
            init_mutex.unlock();
            // edges
            std::vector<std::pair<std::string, std::string>> props;
            while (std::getline(file, line)) {
                props.clear();
                //parse_two_uint64(line, node0, node1);
                parse_weaver_edge(line, node0, node1, props);
                edge_handle = max_node_handle + (edge_count++);
                uint64_t loc0 = all_node_map[node0];
                uint64_t loc1 = all_node_map[node1];
                if (loc0 == shard_id) {
                    n = S->acquire_node_nonlocking(node0);
                    if (n == NULL) {
                        WDEBUG << "Not found node " << node0 << std::endl;
                    }
                    assert(n != NULL);
                    S->create_edge_nonlocking(n, edge_handle, node1, loc1, zero_clk, true);
                    for (auto &p: props) {
                        S->set_edge_property_nonlocking(n, edge_handle, p.first, p.second, zero_clk);
                    }
                }
            }
            break;
        }

        default:
            WDEBUG << "Unknown graph file format " << std::endl;
            return;
    }
    file.close();

    WDEBUG << "Loaded graph at shard " << shard_id << " with " << S->shard_node_count[shard_id - SHARD_ID_INCR]
            << " nodes and " << edge_count << " edges" << std::endl;
}

// called on a separate thread during bulk graph loading process
// issues Hyperdex calls to store the node map
inline void
init_nmap()
{
    nmap::nmap_stub node_mapper;
    init_mutex.lock();
    start_load++;
    start_load_cv.signal();
    while (!init_nodes || !init_node_maps.empty()) {
        if (init_node_maps.empty()) {
            init_cv.wait();
        } else {
            auto &node_map = init_node_maps.front();
            WDEBUG << "NMAP init node map at shard " << shard_id << ", map size = " << node_map.size() << std::endl;
            init_mutex.unlock();
            node_mapper.put_mappings(node_map);
            init_mutex.lock();
            init_node_maps.pop_front();
        }
    }
    init_mutex.unlock();
    WDEBUG << "Done init nmap thread, exiting now" << std::endl;
}

void
migrated_nbr_update(std::unique_ptr<message::message> msg)
{
    uint64_t node, old_loc, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, node, old_loc, new_loc);
    S->update_migrated_nbr(node, old_loc, new_loc);
}

void
migrated_nbr_ack(uint64_t from_loc, std::vector<uint64_t> &target_req_id, uint64_t node_count)
{
    S->migration_mutex.lock();
    for (int i = 0; i < NUM_VTS; i++) {
        if (S->target_prog_id[i] < target_req_id[i]) {
            S->target_prog_id[i] = target_req_id[i];
        }
    }
    S->migr_edge_acks.set(from_loc - SHARD_ID_INCR);
    S->shard_node_count[from_loc - SHARD_ID_INCR] = node_count;
    S->migration_mutex.unlock();
}

void
unpack_migrate_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;

    switch (request->type) {
        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(std::move(request->msg));
            break;

        case message::MIGRATE_SEND_NODE:
            migrate_node_step2_resp(std::move(request->msg));
            break;

        case message::MIGRATED_NBR_ACK: {
            uint64_t from_loc, node_count;
            std::vector<uint64_t> done_ids;
            message::unpack_message(*request->msg, request->type, from_loc, done_ids, node_count);
            migrated_nbr_ack(from_loc, done_ids, node_count);
            break;
        }

        default:
            WDEBUG << "unknown type" << std::endl;
    }
    delete request;
}

void
unpack_tx_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    uint64_t vt_id, tx_id;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    transaction::pending_tx tx;
    message::unpack_message(*request->msg, message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);

    // execute all create_node writes
    // establish tx order at all graph nodes for all other writes
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                create_node(vclk, upd->handle);
                break;

            case transaction::EDGE_CREATE_REQ:
            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY: // elem1
                S->node_tx_order(upd->elem1, vt_id, qts[vt_id]);
                break;

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY: // elem2
                S->node_tx_order(upd->elem2, vt_id, qts[vt_id]);
                break;

            default:
                WDEBUG << "unknown type" << std::endl;
        }
    }

    // increment qts so that threadpool moves forward
    // since tx order has already been established, conflicting txs will be processed in correct order
    S->increment_qts(vt_id, tx.writes.size());

    // apply all writes
    // acquire_node_write blocks if a preceding write has not been executed
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::EDGE_CREATE_REQ:
                create_edge(vclk, qts, upd->handle, upd->elem1, upd->elem2, upd->loc2);
                break;

            case transaction::NODE_DELETE_REQ:
                delete_node(vclk, qts, upd->elem1);
                break;

            case transaction::EDGE_DELETE_REQ:
                delete_edge(vclk, qts, upd->elem1, upd->elem2);
                break;

            case transaction::NODE_SET_PROPERTY:
                set_node_property(vclk, qts, upd->elem1, std::move(upd->key), std::move(upd->value));
                break;

            case transaction::EDGE_SET_PROPERTY:
                set_edge_property(vclk, qts, upd->elem1, upd->elem2, std::move(upd->key), std::move(upd->value));
                break;

            default:
                continue;
        }
    }

    // increment qts for next writes
    S->record_completed_tx(vt_id, vclk.clock);
    delete request;

    // send tx confirmation to coordinator
    message::message conf_msg;
    message::prepare_message(conf_msg, message::TX_DONE, tx_id);
    S->send(vt_id, conf_msg.buf);
}

// process nop
// migration-related checks, and possibly initiating migration
inline void
nop(void *noparg)
{
    message::message msg;
    db::nop_data *nop_arg = (db::nop_data*)noparg;
    bool check_move_migr, check_init_migr, check_migr_step3;
    
    // increment qts
    S->increment_qts(nop_arg->vt_id, 1);
    
    // note done progs for state clean up
    S->add_done_requests(nop_arg->done_reqs);
    
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
            message::prepare_message(msg, message::MIGRATION_TOKEN);
            S->send(S->migr_vt, msg.buf);
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

    uint64_t cur_node_count = S->shard_node_count[shard_id - SHARD_ID_INCR];
    for (uint64_t shrd = 0; shrd < NUM_SHARDS; shrd++) {
        if ((shrd + SHARD_ID_INCR) == shard_id) {
            continue;
        }
        S->shard_node_count[shrd] = nop_arg->shard_node_count[shrd];
    }
    S->migration_mutex.unlock();
    
    // call appropriate function based on check
    if (check_move_migr) {
        migrate_node_step2_req();
    } else if (check_init_migr) {
        shard_daemon_begin();
    } else if (check_migr_step3) {
        migrate_node_step3();
    }

    // initiate permanent deletion
    S->permanent_delete_loop(nop_arg->vt_id, nop_arg->outstanding_progs != 0);

    // record clock; reads go through
    S->record_completed_tx(nop_arg->vt_id, nop_arg->vclk.clock);

    // ack to VT
    message::prepare_message(msg, message::VT_NOP_ACK, shard_id, cur_node_count);
    S->send(nop_arg->vt_id, msg.buf);
    free(nop_arg);
}

template <typename NodeStateType>
std::shared_ptr<NodeStateType> get_node_state(node_prog::prog_type pType,
        uint64_t req_id, uint64_t node_handle)
{
    std::shared_ptr<NodeStateType> ret;
    auto state = S->fetch_prog_req_state(pType, req_id, node_handle);
    if (state) {
        ret = std::dynamic_pointer_cast<NodeStateType>(state);
    }
    return ret;
}

template <typename NodeStateType>
NodeStateType& return_state(node_prog::prog_type pType, uint64_t req_id,
        uint64_t node_handle, std::shared_ptr<NodeStateType> toRet)
{
    if (toRet) {
        return *toRet;
    } else {
        std::shared_ptr<NodeStateType> newState(new NodeStateType());
        S->insert_prog_req_state(pType, req_id, node_handle,
                std::dynamic_pointer_cast<node_prog::Node_State_Base>(newState));
        return *newState;
    }
}

/*
inline void modify_triangle_params(void * triangle_params, size_t num_nodes, db::element::remote_node& node) {
    node_prog::triangle_params * params = (node_prog::triangle_params *) triangle_params;
    params->responses_left = num_nodes;
    params->super_node = node;
}
*/

/* fill changes since time on node
 */
inline void
fill_node_cache_context(db::caching::node_cache_context& context, db::element::node& node, vc::vclock& cache_time, vc::vclock& cur_time)
{
    context.node_deleted = (order::compare_two_vts(node.get_del_time(), cur_time) == 0);
    for (auto &iter: node.out_edges) {
        db::element::edge* e = iter.second;
        assert(e != NULL);

        bool del_after_cached = (order::compare_two_vts(e->get_del_time(), cache_time) == 1);
        bool creat_after_cached = (order::compare_two_vts(e->get_creat_time(), cache_time) == 1);

        bool del_before_cur = (order::compare_two_vts(e->get_del_time(), cur_time) == 0);
        bool creat_before_cur = (order::compare_two_vts(e->get_creat_time(), cur_time) == 0);

        assert(creat_before_cur); // TODO: is this check needed/valid
        assert(del_after_cached);

        if (creat_after_cached && creat_before_cur && !del_before_cur){
            context.edges_added.push_back(*e);
        }
        if (del_after_cached && del_before_cur) {
            context.edges_deleted.push_back(*e);
        }
    }
}

inline void
fetch_node_cache_contexts(uint64_t loc, std::vector<uint64_t>& handles, std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& toFill,
        vc::vclock& cache_entry_time, vc::vclock& req_vclock)
{
    // TODO maybe make this skip over locked nodes and retry fetching later
    for (uint64_t handle : handles){
        db::element::node *node = S->acquire_node(handle);
        assert(node != NULL); // TODO could be garbage collected, or migrated but not completed?
        if (node->state == db::element::node::mode::MOVED) {
            WDEBUG << "cache dont support this yet" << std::endl;
            assert(false);
        } else { // node exists
            toFill.emplace_back();
            toFill.back().first.loc = loc;
            toFill.back().first.handle = handle;
            fill_node_cache_context(toFill.back().second, *node, cache_entry_time, req_vclock);
        }
        S->release_node(node);
    }
}

void
unpack_and_fetch_context(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    std::vector<uint64_t> handles;
    vc::vclock req_vclock, cache_entry_time;
    uint64_t vt_id, req_id, from_shard;
    std::pair<uint64_t, uint64_t> lookup_pair;
    node_prog::prog_type pType;

    message::unpack_message(*request->msg, message::NODE_CONTEXT_FETCH, pType, req_id, vt_id, req_vclock, cache_entry_time, lookup_pair, handles, from_shard);
    std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>> contexts;

    fetch_node_cache_contexts(S->shard_id, handles, contexts, cache_entry_time, req_vclock);

    message::message m;
    message::prepare_message(m, message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock, lookup_pair, contexts);
    S->send(from_shard, m.buf);
    delete request;
}

template <typename ParamsType, typename NodeStateType>
struct fetch_state{
    node_prog::node_prog_running_state<ParamsType, NodeStateType> prog_state;
    po6::threads::mutex counter_mutex;
    uint64_t replies_left;
    fetch_state(node_prog::node_prog_running_state<ParamsType, NodeStateType> & copy_from)
        : prog_state(copy_from.clone_without_start_node_params()) {};

    // delete standard copy onstructors
    fetch_state (const fetch_state&) = delete;
    fetch_state& operator=(fetch_state const&) = delete;
};

/* precondition: node_to_check is locked when called
   returns true if it has updated cached value or there is no valid one and the node prog loop should continue
   returns false and frees node if it needs to fetch context on other shards, saves required state to continue node program later
 */
template <typename ParamsType, typename NodeStateType>
inline bool cache_lookup(db::element::node*& node_to_check, uint64_t cache_key, node_prog::node_prog_running_state<ParamsType, NodeStateType>& np,
        std::tuple<uint64_t, ParamsType, db::element::remote_node>& cur_node_params)
{
    assert(node_to_check != NULL);
    assert(np.cache_value == false); // cache_value is not already assigned
    np.cache_value = NULL; // it is unallocated anyway
    if (node_to_check->cache.cache.count(cache_key) == 0){
        return true;
    } else {
        auto entry = node_to_check->cache.cache.at(cache_key);
        std::shared_ptr<node_prog::Cache_Value_Base>& cval = std::get<0>(entry);
        std::shared_ptr<vc::vclock> cache_entry_time(std::get<1>(entry));
        std::shared_ptr<std::vector<db::element::remote_node>>& watch_set = std::get<2>(entry);

        int64_t cmp_1 = order::compare_two_vts(*cache_entry_time, *np.req_vclock);
        if (cmp_1 >= 1){ // cached value is newer or from this same request
            return true;
        }
        assert(cmp_1 == 0);

        std::unique_ptr<db::caching::cache_response> cache_response(new db::caching::cache_response(node_to_check->cache, cache_key, cval, watch_set));

        if (watch_set->empty()){ // no context needs to be fetched
            np.cache_value = std::move(cache_response);
            return true;
        }

        // save node info for re-aquirining node if needed
        db::element::node *local_node_ptr = node_to_check;
        uint64_t local_node_handle = node_to_check->get_handle();
        uint64_t uid = node_to_check->cache.gen_uid();
        S->release_node(node_to_check);


        // map from loc to list of handles on that shard we need context from for this request
        std::vector<uint64_t> local_contexts_to_fetch; 
        std::unordered_map<uint64_t, std::vector<uint64_t>> contexts_to_fetch; 
        for (db::element::remote_node& watch_node : *watch_set)
        {
            if (watch_node.loc == S->shard_id) { 
                local_contexts_to_fetch.emplace_back(watch_node.handle); // fetch this state after we send requests to other servers
            } else { // on another shard
                contexts_to_fetch[watch_node.loc].emplace_back(watch_node.handle);
            } 
        }
        if (contexts_to_fetch.empty()){ // all contexts needed were on local shard
            fetch_node_cache_contexts(S->shard_id, local_contexts_to_fetch, cache_response->context, *cache_entry_time, *np.req_vclock);
            np.cache_value = std::move(cache_response);
            node_to_check = S->acquire_node(local_node_handle);
            assert(node_to_check == local_node_ptr && "migration not supported yet");
            UNUSED(local_node_ptr);
            return true;
        }

        //WDEBUG << "caching waiting for rest of context to be fetched" << std::endl;
        // add waiting stuff to shard global structure

        std::pair<uint64_t, uint64_t> lookup_pair(uid, local_node_handle);
        // saving copying node_prog_running_state np for later
        fetch_state<ParamsType, NodeStateType> *fstate = new fetch_state<ParamsType, NodeStateType>(np);
        fstate->replies_left = contexts_to_fetch.size();
        fstate->prog_state.cache_value = std::move(cache_response);
        fstate->prog_state.start_node_params.push_back(cur_node_params);
        assert(fstate->prog_state.start_node_params.size() == 1);
        fstate->counter_mutex.lock();


        // map from node_handle, lookup_pair to node_prog_running_state
        S->node_prog_running_states_mutex.lock();
        S->node_prog_running_states[lookup_pair] = fstate; 
        //WDEBUG << "Inserting prog state with lookup pair where local_node_handle is " << local_node_handle << std::endl;
        S->node_prog_running_states_mutex.unlock();
        for (auto& shard_list_pair : contexts_to_fetch){
            std::unique_ptr<message::message> m(new message::message()); // XXX can we re-use messages?
            message::prepare_message(*m, message::NODE_CONTEXT_FETCH, np.prog_type_recvd, np.req_id, np.vt_id, np.req_vclock, *cache_entry_time, lookup_pair, shard_list_pair.second, S->shard_id);
            S->send(shard_list_pair.first, m->buf);
        }

        fetch_node_cache_contexts(S->shard_id, local_contexts_to_fetch, fstate->prog_state.cache_value->context, *cache_entry_time, *np.req_vclock);
        fstate->counter_mutex.unlock();
        return false;
    } 
}

template <typename ParamsType, typename NodeStateType>
inline void node_prog_loop(
        typename node_prog::node_function_type<ParamsType, NodeStateType>::value_type func,
        node_prog::node_prog_running_state<ParamsType, NodeStateType> &np)
{
    assert(np.start_node_params.size() == 1 || !np.cache_value); // if cache value passed in the start node params should be size 1
    // tuple of (node handle, node prog params, prev node)
    typedef std::tuple<uint64_t, ParamsType, db::element::remote_node> node_params_t;
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::vector<node_params_t>> batched_node_progs;
    // node state function
    std::function<NodeStateType&()> node_state_getter;
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
            std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)> add_cache_func;

    uint64_t node_handle;
    bool done_request = false;
    db::element::remote_node this_node(S->shard_id, 0);

    while (!np.start_node_params.empty() && !done_request) {
        for (auto &handle_params : np.start_node_params) {
            node_handle = std::get<0>(handle_params);
            ParamsType& params = std::get<1>(handle_params);
            this_node.handle = node_handle;
            db::element::node *node = S->acquire_node(node_handle);
            if (node == NULL || order::compare_two_vts(node->get_del_time(), *np.req_vclock)==0) { // TODO: TIMESTAMP
                if (node != NULL) {
                    S->release_node(node);
                } else {
                    // node is being migrated here, but not yet completed
                    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> buf_node_params;
                    buf_node_params.emplace_back(handle_params);
                    std::unique_ptr<message::message> m(new message::message());
                    message::prepare_message(*m, message::NODE_PROG, np.prog_type_recvd, np.global_req, np.vt_id, np.req_vclock, np.req_id, buf_node_params);
                    S->migration_mutex.lock();
                    if (S->deferred_reads.find(node_handle) == S->deferred_reads.end()) {
                        S->deferred_reads.emplace(node_handle, std::vector<std::unique_ptr<message::message>>());
                    }
                    S->deferred_reads[node_handle].emplace_back(std::move(m));
                    WDEBUG << "Buffering read for node " << node_handle << std::endl;
                    S->migration_mutex.unlock();
                }
            } else if (node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node program
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                fwd_node_params.emplace_back(handle_params);
                std::unique_ptr<message::message> m(new message::message());
                message::prepare_message(*m, message::NODE_PROG, np.prog_type_recvd, np.global_req, np.vt_id, np.req_vclock, np.req_id, fwd_node_params);
                uint64_t new_loc = node->new_loc;
                S->release_node(node);
                S->send(new_loc, m->buf);
            } else { // node does exist
                assert(node->state == db::element::node::mode::STABLE);
                if (MAX_CACHE_ENTRIES)
                {
                if (params.search_cache()){ // && !node->checking_cache){
                    if (np.cache_value) { // cache value already found (from a fetch)
                        node->checking_cache = false;
                    } else if (!node->checking_cache) { // lookup cache value if another prog isnt already
                        node->checking_cache = true;
                        bool run_prog_now = cache_lookup<ParamsType, NodeStateType>(node, params.cache_key(), np, handle_params);
                        if (run_prog_now) { // we fetched context and will give it to node program
                            node->checking_cache = false;
                        } else { // go to next node while we fetch cache context for this one, cache_lookup releases node if false
                            continue;
                        }
                    }
                }
                }

                // bind cache getter and putter function variables to functions
                std::shared_ptr<NodeStateType> state = get_node_state<NodeStateType>(np.prog_type_recvd,
                        np.req_id, node_handle);
                node_state_getter = std::bind(return_state<NodeStateType>,
                        np.prog_type_recvd, np.req_id, node_handle, state);

                if (S->check_done_request(np.req_id)) {
                    done_request = true;
                    S->release_node(node);
                    break;
                }
                if (MAX_CACHE_ENTRIES)
                {
                using namespace std::placeholders;
                add_cache_func = std::bind(&db::caching::program_cache::add_cache_value, &(node->cache),
                        np.prog_type_recvd, _1, _2, _3, np.req_vclock); // 1 is cache value, 2 is watch set, 3 is key
                }

                // call node program
                auto next_node_params = func(np.req_id, *node, this_node,
                        params, // actual parameters for this node program
                        node_state_getter, np.req_vclock, add_cache_func, std::move(np.cache_value));

                // batch the newly generated node programs for onward propagation
#ifdef WEAVER_CLDG
                std::unordered_map<uint64_t, uint32_t> agg_msg_count;
#endif
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t loc = res.first.loc;
                    assert(loc < NUM_SHARDS + SHARD_ID_INCR);
                    if (loc < SHARD_ID_INCR) {
                    }
                    if (loc == np.vt_id) {
                        // signal to send back to vector timestamper that issued request
                        // TODO mark done
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        std::unique_ptr<message::message> m(new message::message());
                        message::prepare_message(*m, message::NODE_PROG_RETURN, np.prog_type_recvd, np.req_id, temppair);
                        S->send(np.vt_id, m->buf);
                    } else {
                        batched_node_progs[loc].emplace_back(res.first.handle, std::move(res.second), this_node);
#ifdef WEAVER_CLDG
                        agg_msg_count[node_handle]++;
#endif
                    }
                }
                S->release_node(node);
#ifdef WEAVER_CLDG
                S->msg_count_mutex.lock();
                for (auto &p: agg_msg_count) {
                    S->agg_msg_count[p.first] += p.second;
                }
                S->msg_count_mutex.unlock();
#endif
                uint64_t msg_count = 0;
                // Only per hop batching
                for (uint64_t next_loc = SHARD_ID_INCR; next_loc < NUM_SHARDS + SHARD_ID_INCR; next_loc++) {
                    if ((batched_node_progs.find(next_loc) != batched_node_progs.end() && !batched_node_progs[next_loc].empty())
                        && next_loc != S->shard_id) {
                        std::unique_ptr<message::message> m(new message::message());
                        message::prepare_message(*m, message::NODE_PROG, np.prog_type_recvd, np.global_req, np.vt_id, np.req_vclock, np.req_id, batched_node_progs[next_loc]);
                        S->send(next_loc, m->buf);
                        batched_node_progs[next_loc].clear();
                        msg_count++;
                    }
                }
            }
            if (MAX_CACHE_ENTRIES)
            {
                assert(np.cache_value == false); // unique ptr is not assigned
            }
        }
        np.start_node_params = std::move(batched_node_progs[S->shard_id]);

        if (S->check_done_request(np.req_id)) {
            done_request = true;
        }
    }
#ifdef WEAVER_MSG_COUNT
    S->update_mutex.lock();
    S->msg_count += msg_count;
    S->update_mutex.unlock();
#endif
}

void
unpack_node_program(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    node_prog::prog_type pType;

    message::unpack_partial_message(*request->msg, message::NODE_PROG, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_and_run_db(std::move(request->msg));
    delete request;
}

void
unpack_context_reply(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    node_prog::prog_type pType;

    message::unpack_partial_message(*request->msg, message::NODE_CONTEXT_REPLY, pType);
    assert(node_prog::programs.find(pType) != node_prog::programs.end());
    node_prog::programs[pType]->unpack_context_reply_db(std::move(request->msg));
    delete request;
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> ::
    unpack_context_reply_db(std::unique_ptr<message::message> msg)
{
    vc::vclock req_vclock;
    node_prog::prog_type pType;
    uint64_t req_id, vt_id;
    std::pair<uint64_t, uint64_t> lookup_pair;
    std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>> contexts_to_add; // TODO extra copy, later unpack into cache_response object itself
    message::unpack_message(*msg, message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, req_vclock, lookup_pair, contexts_to_add);

    S->node_prog_running_states_mutex.lock();
    struct fetch_state<ParamsType, NodeStateType> *fstate = (struct fetch_state<ParamsType, NodeStateType> *) S->node_prog_running_states.at(lookup_pair);
    S->node_prog_running_states_mutex.unlock();

    fstate->counter_mutex.lock();
    auto& existing_context = fstate->prog_state.cache_value->context;
    existing_context.insert(existing_context.end(), contexts_to_add.begin(), contexts_to_add.end()); // XXX avoid copy here
    fstate->replies_left--;
    if (fstate->replies_left == 0){
        assert(fstate->prog_state.cache_value); // cache value should exist
        node_prog_loop<ParamsType, NodeStateType>(enclosed_node_prog_func, fstate->prog_state);
        fstate->counter_mutex.unlock();
        //remove from map
        S->node_prog_running_states_mutex.lock();
        size_t num_erased = S->node_prog_running_states.erase(lookup_pair);
        S->node_prog_running_states_mutex.unlock();
        assert(num_erased == 1);
        UNUSED(num_erased);
        delete fstate; // XXX did we delete prog_state, cache_value?
    }
    else { // wait for more replies
        fstate->counter_mutex.unlock();
    }
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_run_db(std::unique_ptr<message::message> msg)
{
    node_prog::node_prog_running_state<ParamsType, NodeStateType> np;

    // unpack the node program
    try {
        message::unpack_message(*msg, message::NODE_PROG, np.prog_type_recvd, np.global_req, np.vt_id, np.req_vclock, np.req_id, np.start_node_params);
        assert(np.req_vclock->clock.size() == NUM_VTS);
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
    node_prog_loop<ParamsType, NodeStateType>(enclosed_node_prog_func, np);
/*
    // TODO needs work
    if (global_req) {
        assert(start_node_params.size() == 1);

        std::vector<uint64_t> handles_to_send_to;
        S->update_mutex.lock();
        for (auto& n : S->nodes) {
            bool creat_before = order::compare_two_vts(n.second->get_del_time(), req_vclock) != 0;
            bool del_after = order::compare_two_vts(req_vclock, n.second->get_creat_time()) != 0;
            if (creat_before && del_after) {
                handles_to_send_to.emplace_back(n.first);
            }
        }
        S->update_mutex.unlock();
        ParamsType& params_copy = std::get<1>(start_node_params[0]); // send this all over
        assert(handles_to_send_to.size() > 0);
        this_node.handle = handles_to_send_to[0];
        //modify_triangle_params((void *) &params_copy, handles_to_send_to.size(), this_node);
        global_req = false; // for batched messages to execute normally
        uint64_t idx = 0;
        size_t batch_size = handles_to_send_to.size() / (NUM_THREADS-1);
        db::thread::unstarted_thread *thr;
        db::graph_request *request;
        std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> next_batch;
        while (idx < handles_to_send_to.size()) {
            next_batch.emplace_back(std::make_tuple(handles_to_send_to[idx], params_copy, db::element::remote_node()));
            if (next_batch.size() % batch_size == 0) {
                message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, next_batch);
                request = new db::graph_request(message::NODE_PROG, std::move(msg));
                thr = new db::thread::unstarted_thread(req_id, req_vclock, unpack_node_program, request);
                S->add_read_request(vt_id, thr);
                msg.reset(new message::message());
                next_batch.clear();
            }
            idx++;
        }
        if (next_batch.size() > 0) { // get leftovers
            message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, next_batch);
            request = new db::graph_request(message::NODE_PROG, std::move(msg));
            thr = new db::thread::unstarted_thread(req_id, req_vclock, unpack_node_program, request);
            S->add_read_request(vt_id, thr);
        }
        return;
    }
    */
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message>, uint64_t, int)
{ }

// delete all in-edges for a permanently deleted node
inline void
update_deleted_node(void *node)
{
    uint64_t *node_handle = (uint64_t*)node;
    std::unordered_set<uint64_t> nbrs;
    db::element::node *n;
    db::element::edge *e;
    S->edge_map_mutex.lock();
    if (S->edge_map.find(*node_handle) != S->edge_map.end()) {
        nbrs = std::move(S->edge_map[*node_handle]);
        S->edge_map.erase(*node_handle);
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
            if (e->nbr.handle == *node_handle) {
                delete e;
                to_del.emplace_back(x.first);
                found = true;
            }
        }
        assert(found);
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
migrate_node_step1(db::element::node *n, std::vector<uint64_t> &shard_node_count)
{
    // find arg max migr score
    uint64_t max_pos = shard_id - SHARD_ID_INCR; // don't migrate if all equal
    std::vector<uint32_t> max_indices(1, max_pos);
    for (uint32_t j = 0; j < NUM_SHARDS; j++) {
        if (j == (shard_id - SHARD_ID_INCR)) {
            continue;
        }
        if (n->migr_score[max_pos] < n->migr_score[j]) {
            max_pos = j;
            max_indices.clear();
            max_indices.emplace_back(j);
        } else if (n->migr_score[max_pos] == n->migr_score[j]) {
            max_indices.emplace_back(j);
        }
        n->msg_count[j] = 0;
    }
    uint64_t migr_loc = get_balanced_assignment(shard_node_count, max_indices) + SHARD_ID_INCR;
    if (migr_loc > shard_id) {
        n->already_migr = true;
    }
    
    // no migration to self
    if (migr_loc == shard_id) {
        return false;
    }

    // begin migration
    S->migration_mutex.lock();
    S->current_migr = true;
    for (uint64_t &x: S->nop_count) {
        x = 0;
    }
    S->migration_mutex.unlock();

    // mark node as "moved"
    n->state = db::element::node::mode::MOVED;
    n->new_loc = migr_loc;
    S->migr_node = n->get_handle();
    S->migr_shard = migr_loc;

    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &e: n->out_edges) {
        uint64_t node = e.second->nbr.handle;
        assert(S->edge_map.find(node) != S->edge_map.end());
        auto &node_set = S->edge_map[node];
        node_set.erase(S->migr_node);
        if (node_set.empty()) {
            S->edge_map.erase(node);
        }
    }
    S->edge_map_mutex.unlock();
    S->release_node(n);

    // update Hyperdex map for this node
    S->update_node_mapping(S->migr_node, migr_loc);

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
    for (uint64_t idx = 0; idx < NUM_VTS; idx++) {
        S->target_prog_id[idx] = 0;
    }
    S->migration_mutex.unlock();

    n = S->acquire_node(S->migr_node);
    assert(n != NULL);
    message::prepare_message(msg, message::MIGRATE_SEND_NODE, S->migr_node, shard_id, *n);
    S->release_node(n);
    S->send(S->migr_shard, msg.buf);
}

// receive and place node which has been migrated to this shard
// apply buffered reads and writes to node
// update nbrs of migrated nbrs
void
migrate_node_step2_resp(std::unique_ptr<message::message> msg)
{
    // unpack and place node
    uint64_t from_loc;
    uint64_t node_handle;
    db::element::node *n;

    // create a new node, unpack the message
    vc::vclock dummy_clock;
    message::unpack_message(*msg, message::MIGRATE_SEND_NODE, node_handle);
    n = S->create_node(node_handle, dummy_clock, true); // node will be acquired on return
    try {
        message::unpack_message(*msg, message::MIGRATE_SEND_NODE, node_handle, from_loc, *n);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        return;
    }
    
    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &e: n->out_edges) {
        uint64_t node = e.second->nbr.handle;
        S->edge_map[node].emplace(node_handle);
    }
    S->edge_map_mutex.unlock();

    S->migration_mutex.lock();
    // apply buffered writes
    if (S->deferred_writes.find(node_handle) != S->deferred_writes.end()) {
        for (auto &dw: S->deferred_writes[node_handle]) {
            switch (dw.type) {
                case message::NODE_DELETE_REQ:
                    S->delete_node_nonlocking(n, dw.vclk);
                    break;

                case message::EDGE_CREATE_REQ:
                    S->create_edge_nonlocking(n, dw.edge, dw.remote_node, dw.remote_loc, dw.vclk);
                    break;

                case message::EDGE_DELETE_REQ:
                    S->delete_edge_nonlocking(n, dw.edge, dw.vclk);
                    break;

                default:
                    WDEBUG << "unexpected type" << std::endl;
            }
        }
        S->deferred_writes.erase(node_handle);
    }

    // update nbrs
    for (uint64_t upd_shard = SHARD_ID_INCR; upd_shard < SHARD_ID_INCR + NUM_SHARDS; upd_shard++) {
        if (upd_shard == shard_id) {
            continue;
        }
        message::prepare_message(*msg, message::MIGRATED_NBR_UPDATE, node_handle, from_loc, shard_id);
        S->send(upd_shard, msg->buf);
    }
    n->state = db::element::node::mode::STABLE;

    // release node for new reads and writes
    S->release_node(n);

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
        message::unpack_partial_message(*m, message::NODE_PROG, pType);
        WDEBUG << "APPLYING BUFREAD for node " << node_handle << std::endl;
        assert(node_prog::programs.find(pType) != node_prog::programs.end());
        node_prog::programs[pType]->unpack_and_run_db(std::move(m));
    }
}

// check if all nbrs updated, if so call step3
// caution: assuming caller holds S->migration_mutex
bool
check_step3()
{
    bool init_step3 = S->migr_edge_acks.all(); // all shards must have responded for edge updates
    for (int i = 0; i < NUM_VTS && init_step3; i++) {
        init_step3 = (S->target_prog_id[i] <= S->max_done_id[i]);
    }
    if (init_step3) {
        S->migr_edge_acks.reset();
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
check_migr_node(db::element::node *n)
{
    if (n == NULL || order::compare_two_clocks(n->get_del_time().clock, S->max_clk.clock) != 2 ||
        n->state == db::element::node::mode::MOVED ||
        n->already_migr) {
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

inline void
cldg_migration_wrapper(std::vector<uint64_t> &shard_node_count)
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
        db::element::edge *e;
        // get aggregate msg counts per shard
        //std::vector<uint64_t> msg_count(NUM_SHARDS, 0);
        for (auto &e_iter: n->out_edges) {
            e = e_iter.second;
            //msg_count[e->nbr.loc - SHARD_ID_INCR] += e->msg_count;
            n->msg_count[e->nbr.loc - SHARD_ID_INCR] += e->msg_count;
        }
        // EWMA update to msg count
        //for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        //    double new_val = 0.4 * n->msg_count[i] + 0.6 * msg_count[i];
        //    n->msg_count[i] = new_val;
        //}
        // update migration score based on CLDG
        for (int j = 0; j < NUM_SHARDS; j++) {
            double penalty = 1.0 - ((double)shard_node_count[j])/SHARD_CAP;
            n->migr_score[j] = n->msg_count[j] * penalty;
        }

        if (migrate_node_step1(n, shard_node_count)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        shard_daemon_end();
    }
}

inline void
ldg_migration_wrapper(std::vector<uint64_t> &shard_node_count)
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
            n->migr_score[e->nbr.loc - SHARD_ID_INCR] += 1;
        }
        for (int j = 0; j < NUM_SHARDS; j++) {
            n->migr_score[j] *= (1 - ((double)shard_node_count[j])/SHARD_CAP);
        }

        if (migrate_node_step1(n, shard_node_count)) {
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        shard_daemon_end();
    }
}

// stream list of nodes, decide where to migrate each node
// graph partitioning logic here
void
migration_wrapper()
{
    S->migration_mutex.lock();
    std::vector<uint64_t> shard_node_count = S->shard_node_count;
    S->migration_mutex.unlock();

#ifdef WEAVER_CLDG
    cldg_migration_wrapper(shard_node_count);
#else
    ldg_migration_wrapper(shard_node_count);
#endif
}

// method to sort pairs based on second coordinate
bool agg_count_compare(std::pair<uint64_t, uint32_t> p1, std::pair<uint64_t, uint32_t> p2)
{
    return (p1.second > p2.second);
}

// sort nodes in order of number of requests propagated
// and (implicitly) pass sorted deque to migration wrapper
void
shard_daemon_begin()
{
    S->update_mutex.lock();
    S->ldg_nodes = S->node_list;
    S->update_mutex.unlock();

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
#else
    S->ldg_iter = S->ldg_nodes.begin();
#endif

    migration_wrapper();
}

// pass migration token to next shard
void
shard_daemon_end()
{
    message::message msg;
    S->migration_mutex.lock();
    S->migr_token = false;
    message::prepare_message(msg, message::MIGRATION_TOKEN, --S->migr_token_hops, S->migr_vt);
    S->migration_mutex.unlock();
    uint64_t next_id; 
    if ((shard_id + 1 - SHARD_ID_INCR) >= NUM_SHARDS) {
        next_id = SHARD_ID_INCR;
    } else {
        next_id = shard_id + 1;
    }
    S->send(next_id, msg.buf);
}

// server msg recv loop for the shard server
void
msgrecv_loop()
{
    busybee_returncode ret;
    uint64_t sender, vt_id, req_id;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg(new message::message());
    db::thread::unstarted_thread *thr;
    db::graph_request *request;
    node_prog::prog_type pType;
    vc::vclock vclk;
    vc::qtimestamp_t qts;

    while (true) {
        if ((ret = S->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            WDEBUG << "msg recv error: " << ret << " at shard " << S->shard_id << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        rec_msg->change_type(mtype);
        sender -= ID_INCR;
        vclk.clock.clear();
        qts.clear();

        switch (mtype)
        {
            case message::TX_INIT:
                message::unpack_partial_message(*rec_msg, message::TX_INIT, vt_id, vclk, qts);
                assert(qts.size() == NUM_SHARDS);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts[shard_id-SHARD_ID_INCR], vclk, unpack_tx_request, request);
                S->add_write_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_PROG:
                bool global_req;
                message::unpack_partial_message(*rec_msg, message::NODE_PROG, pType, global_req, vt_id, vclk, req_id);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_node_program, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_CONTEXT_FETCH:
                message::unpack_partial_message(*rec_msg, message::NODE_CONTEXT_FETCH, pType, req_id, vt_id, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_and_fetch_context, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_CONTEXT_REPLY:
                message::unpack_partial_message(*rec_msg, message::NODE_CONTEXT_REPLY, pType, req_id, vt_id, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_context_reply, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::VT_NOP: {
                db::nop_data *nop_arg = new db::nop_data();
                message::unpack_message(*rec_msg, mtype, vt_id, nop_arg->vclk, qts, req_id,
                    nop_arg->done_reqs, nop_arg->max_done_id, nop_arg->max_done_clk,
                    nop_arg->outstanding_progs, nop_arg->shard_node_count);
                assert(nop_arg->vclk.clock.size() == NUM_VTS);
                assert(nop_arg->max_done_clk.size() == NUM_VTS);
                nop_arg->vt_id = vt_id;
                nop_arg->req_id = req_id;
                thr = new db::thread::unstarted_thread(qts[shard_id-SHARD_ID_INCR], nop_arg->vclk, nop, (void*)nop_arg);
                S->add_write_request(vt_id, thr);
                rec_msg.reset(new message::message());
                break;
            }

            case message::PERMANENTLY_DELETED_NODE: {
                uint64_t *node = (uint64_t*)malloc(sizeof(uint64_t));
                message::unpack_message(*rec_msg, mtype, *node);
                thr = new db::thread::unstarted_thread(0, S->zero_clk, update_deleted_node, (void*)node);
                S->add_read_request(rand() % NUM_VTS, thr);
                rec_msg.reset(new message::message());
                break;
            }

            case message::MIGRATE_SEND_NODE:
            case message::MIGRATED_NBR_UPDATE:
            case message::MIGRATED_NBR_ACK:
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, S->zero_clk, unpack_migrate_request, request);
                S->add_read_request(rand() % NUM_VTS, thr);
                rec_msg.reset(new message::message());
                break;

            case message::MIGRATION_TOKEN:
                S->migration_mutex.lock();
                message::unpack_message(*rec_msg, mtype, S->migr_token_hops, S->migr_vt);
                S->migr_token = true;
                S->migrated = false;
                S->migration_mutex.unlock();
                break;

            case message::LOADED_GRAPH: {
                uint64_t load_time;
                message::unpack_message(*rec_msg, message::LOADED_GRAPH, load_time);
                S->graph_load_mutex.lock();
                if (load_time > S->max_load_time) {
                    S->max_load_time = load_time;
                }
                if (++S->load_count == NUM_SHARDS) {
                    WDEBUG << "Loaded graph on all shards, time taken = " << (S->max_load_time/MEGA) << " ms." << std::endl;
                } else {
                    WDEBUG << "Loaded graph on " << S->load_count << " shards, current time "
                            << (S->max_load_time/MEGA) << "ms." << std::endl;
                }
                S->graph_load_mutex.unlock();
                break;
            }

            case message::MSG_COUNT: {
                message::unpack_message(*rec_msg, message::MSG_COUNT, vt_id);
                S->update_mutex.lock();
                message::prepare_message(*rec_msg, message::MSG_COUNT, shard_id, S->msg_count);
                S->msg_count = 0;
                S->update_mutex.unlock();
                S->send(vt_id, rec_msg->buf);
                break;
            }

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                WDEBUG << "unexpected msg type " << mtype << std::endl;
        }
    }
}

int
main(int argc, char *argv[])
{
    signal(SIGINT, end_program);
    if (argc != 2 && argc != 4) {
        WDEBUG << "Usage: " << argv[0] << " <myid> [<graph_file_format> <graph_file_name>]" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    shard_id = id;
    S = new db::shard(id);
    if (argc == 4) {
        // bulk loading
        db::graph_file_format format = db::SNAP;
        if (strcmp(argv[2], "tsv") == 0) {
            format = db::TSV;
        } else if (strcmp(argv[2], "snap") == 0) {
            format = db::SNAP;
        } else if (strcmp(argv[2], "weaver") == 0) {
            format = db::WEAVER;
        } else {
            WDEBUG << "Invalid graph file format" << std::endl;
        }
        init_nodes = false;
        start_load = 0;
        std::thread nmap_thr(init_nmap);

        timespec ts;
        uint64_t load_time = wclock::get_time_elapsed(ts);
        load_graph(format, argv[3]);
        nmap_thr.join();
        load_time = wclock::get_time_elapsed(ts) - load_time;
        message::message msg;
        message::prepare_message(msg, message::LOADED_GRAPH, load_time);
        S->send(SHARD_ID_INCR, msg.buf);
    }
    std::cout << "Weaver: shard instance " << S->shard_id << std::endl;

    msgrecv_loop();

    return 0;
}
