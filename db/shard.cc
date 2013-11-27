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
void migrated_nbr_ack();
void migrate_node_step1(uint64_t node_handle, uint64_t shard);
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
    WDEBUG << "Ending program, param = " << param << ", kronos num calls " << order::call_times->size() << std::endl;
    std::ofstream ktime("kronos_time.rec");
    for (auto x: *order::call_times) {
        ktime << x << std::endl;
    }
    ktime.close();
    exit(0);
}


inline void
create_node(vc::vclock &t_creat, uint64_t node_handle)
{
    S->create_node(node_handle, t_creat, false);
}

inline void
create_edge(vc::vclock &t_creat, uint64_t edge_handle, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    S->create_edge(edge_handle, n1, n2, loc2, t_creat);
}

inline void
delete_node(vc::vclock &t_del, uint64_t node_handle)
{
    S->delete_node(node_handle, t_del);
}

inline void
delete_edge(vc::vclock &t_del, uint64_t edge_handle, uint64_t node_handle)
{
    WDEBUG << "Deleting edge " << edge_handle << " on node " << node_handle << std::endl;
    S->delete_edge(edge_handle, node_handle, t_del);
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

// parse the string 'line' as '<unsigned int> <unsigned int> '
// there can be arbitrary whitespace between the two ints, and after the second int
// store the two parsed ints in 'n1' and 'n2'
// if there is overflow or unexpected char is encountered, return n1 = n2 = 0
inline void
parse_two_uint64(std::string &line, uint64_t &n1, uint64_t &n2)
{
    size_t i = 0;
    bool bad = false; // overflow or unexpected char

    parse_single_uint64(line, i, n1, bad);
    if (bad || i == line.length()) {
        n1 = 0;
        n2 = 0;
        WDEBUG << "Parsing error" << std::endl;
        return;
    }

    while (line[i] == ' '
        || line[i] == '\r'
        || line[i] == '\n'
        || line[i] == '\t') {
        ++i;
    }

    parse_single_uint64(line, i, n2, bad);
    if (bad) {
        n1 = 0;
        n2 = 0;
        WDEBUG << "Parsing error" << std::endl;
    }
}

// initial bulk graph loading method
// 'format' stores the format of the graph file (currently only SNAP format supported - basically edge list)
// 'graph_file' stores the full path filename of the graph file
inline void
load_graph(db::graph_file_format format, const char *graph_file)
{
    std::ifstream file;
    uint64_t node0, node1;
    std::string line, str_node;
    std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>> graph;

    file.open(graph_file, std::ifstream::in);
    if (!file) {
        WDEBUG << "File not found" << std::endl;
        return;
    }
    
    // read, validate, and create graph
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
                    uint64_t loc0 = ((node0 % NUM_SHARDS) + SHARD_ID_INCR);
                    uint64_t loc1 = ((node1 % NUM_SHARDS) + SHARD_ID_INCR);
                    db::element::node *n;
                    if (loc0 == shard_id) {
                        uint64_t edge_handle = max_node_handle + (edge_count++);
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

        default:
            WDEBUG << "Unknown graph file format " << std::endl;
            return;
    }
    file.close();

    WDEBUG << "Loaded graph at shard " << shard_id << " with " << S->num_nodes()
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
            node_mapper.put_mappings(node_map, true);
            init_mutex.lock();
            init_node_maps.pop_front();
        }
    }
    init_mutex.unlock();
    WDEBUG << "Done init nmap thread, exiting now" << std::endl;
}

// called on a separate thread during bulk graph loading process
// issues Hyperdex calls to store the edge map
//inline void
//init_emap()
//{
//    nmap::nmap_stub node_mapper;
//    uint64_t edge_count = 0;
//    timespec ts;
//    uint64_t time;
//    init_mutex.lock();
//    start_load++;
//    start_load_cv.signal();
//    while (!init_edges || !init_edge_maps.empty()) {
//        if (init_edge_maps.empty()) {
//            init_cv.wait();
//        } else {
//            auto &edge_map = init_edge_maps.front();
//            WDEBUG << "EMAP init edge map at shard " << shard_id << ", map size = " << edge_map.size() << std::endl;
//            edge_count += edge_map.size();
//            init_mutex.unlock();
//            time = wclock::get_time_elapsed(ts);
//            node_mapper.put_mappings(edge_map, false);
//            time = wclock::get_time_elapsed(ts) - time;
//            init_mutex.lock();
//            WDEBUG << "EMAP done init edge map at shard " << shard_id << ", time taken = " << time/MEGA << " ms." << std::endl;
//            init_edge_maps.pop_front();
//        }
//    }
//    init_mutex.unlock();
//    WDEBUG << "Done init emap thread, edge processed = " << edge_count << " at shard " << shard_id << std::endl;
//}

void
migrated_nbr_update(std::unique_ptr<message::message> msg)
{
    uint64_t node, old_loc, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, node, old_loc, new_loc);
    S->update_migrated_nbr(node, old_loc, new_loc);
}

void
migrated_nbr_ack(uint64_t from_loc, std::vector<uint64_t> &target_req_id)
{
    S->migration_mutex.lock();
    for (int i = 0; i < NUM_VTS; i++) {
        if (target_req_id[i] > S->target_prog_id[i]) {
            S->target_prog_id[i] = target_req_id[i];
        }
    }
    S->migr_edge_acks[from_loc - SHARD_ID_INCR] = true;
    S->migration_mutex.unlock();
}

void
unpack_migrate_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    uint64_t from_loc;
    std::vector<uint64_t> done_ids;

    switch (request->type) {
        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(std::move(request->msg));
            break;

        case message::MIGRATE_SEND_NODE:
            migrate_node_step2_resp(std::move(request->msg));
            break;

        case message::MIGRATED_NBR_ACK:
            message::unpack_message(*request->msg, request->type, from_loc, done_ids);
            migrated_nbr_ack(from_loc, done_ids);
            break;

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

    // apply all writes
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                create_node(vclk, upd->handle);
                break;

            case transaction::EDGE_CREATE_REQ:
                create_edge(vclk, upd->handle, upd->elem1, upd->elem2, upd->loc2);
                break;

            case transaction::NODE_DELETE_REQ:
                delete_node(vclk, upd->elem1);
                break;

            case transaction::EDGE_DELETE_REQ:
                delete_edge(vclk, upd->elem1, upd->elem2);
                break;

            default:
                WDEBUG << "unknown type" << std::endl;
        }
    }

    // increment qts for next writes
    S->record_completed_transaction(vt_id, tx_id, tx.writes.size());
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
    db::nop_data *nop_arg = (db::nop_data*)noparg;
    bool check_move_migr, check_init_migr, check_migr_step3;
    
    // increment qts
    S->record_completed_transaction(nop_arg->vt_id, nop_arg->req_id);
    
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
        if (S->migr_chance++ > 2) {
            S->migrated = true;
            check_init_migr = true;
            S->migr_chance = 0;
        }
    }

    // node max done id for migrated node clean up
    assert(S->max_done_id[nop_arg->vt_id] <= nop_arg->max_done_id);
    S->max_done_id[nop_arg->vt_id] = nop_arg->max_done_id;
    check_migr_step3 = check_step3();
    
    //std::cout << "Check move migr " << check_move_migr
    //          << ", check init migr " << check_init_migr
    //          << ", check migr step3 " << check_migr_step3 << std::endl;
    // atmost one check should be true
    assert(!(check_move_migr && check_init_migr)
        && !(check_init_migr && check_migr_step3)
        && !(check_move_migr && check_migr_step3));

    S->migration_mutex.unlock();

    // call appropriate function based on check
    if (check_move_migr) {
        migrate_node_step2_req();
    } else if (check_init_migr) {
        shard_daemon_begin();
    } else if (check_migr_step3) {
        migrate_node_step3();
    }

    // ack to VT
    message::message msg;
    message::prepare_message(msg, message::VT_NOP_ACK, shard_id);
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

//template <typename CacheValueType>
void add_cache_value(node_prog::prog_type pType, db::element::node* node, std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
        std::shared_ptr<std::vector<db::element::remote_node>> watch_set, uint64_t cache_key, vc::vclock& req_vclock)
{
    node->cache.add_cache_value(pType, cache_value, watch_set, cache_key, req_vclock);
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

        assert(creat_before_cur);
        assert(del_after_cached);

        if (creat_after_cached && creat_before_cur && !del_before_cur){
            context.edges_added.push_back(*e);
            WDEBUG << "&&&&&&&&&&&&&&&&&&&1" << std::endl;
        }
        if (creat_after_cached){
            WDEBUG << "&&&&&&&&&&&&&&&&&&&0" << std::endl;
        }
        if (del_after_cached && del_before_cur) {
            context.edges_deleted.push_back(*e);
            WDEBUG << "&&&&&&&&&&&&&&&&&&&2" << std::endl;
        }
    }
}

inline void
fetch_node_cache_context(uint64_t loc, uint64_t handle, std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>>& toFill,
        vc::vclock& cache_entry_time, vc::vclock& req_vclock)
{
    db::element::node *node = S->acquire_node(handle);
    assert(node != NULL); // TODO could be garbage collected, or migrated but not completed?
    if (node->state == db::element::node::mode::IN_TRANSIT
            || node->state == db::element::node::mode::MOVED) {
        WDEBUG << "cache dont support this yet" << std::endl;
    } else { // node exists
        toFill.emplace_back();
        toFill.back().first.loc = loc;
        toFill.back().first.handle = handle;
        fill_node_cache_context(toFill.back().second, *node, cache_entry_time, req_vclock);
    }
    S->release_node(node);
}

void
unpack_and_fetch_context(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    std::vector<uint64_t> handles;
    vc::vclock req_vclock, cache_entry_time;
    uint64_t vt_id, req_id, lookup_uid, from_shard;

    message::unpack_message(*request->msg, message::NODE_CONTEXT_FETCH, vt_id, req_vclock, cache_entry_time, req_id, lookup_uid, handles, from_shard);
    std::vector<std::pair<db::element::remote_node, db::caching::node_cache_context>> contexts;

    WDEBUG << "fetching cache contexts on remote shard" << std::endl;
    for (uint64_t handle : handles){
        fetch_node_cache_context(S->shard_id, handle, contexts, cache_entry_time, req_vclock);
    }
    message::message m;
    message::prepare_message(m, message::NODE_CONTEXT_REPLY, vt_id, req_vclock, req_id, lookup_uid, contexts);
    S->send(from_shard, m.buf);
    delete request;
}

/* precondition: node_to_check is locked when called
   returns true if it has updated cached value or there is no valid one and the node prog loop should continue
   returns false and frees node if it needs to fetch context on other shards
 */
template <typename ParamsType, typename NodeStateType>
inline bool cache_lookup(db::element::node* node_to_check, uint64_t cache_key,
        std::vector<std::pair<db::element::remote_node, ParamsType>> (*func)(uint64_t, 
            db::element::node&, 
            db::element::remote_node&, 
            ParamsType&,
            std::function<NodeStateType&()>,
            vc::vclock &,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            db::caching::cache_response*),
        node_prog::prog_type prog_type_recvd, bool global_req, uint64_t vt_id, vc::vclock& req_vclock, uint64_t req_id,
        std::tuple<uint64_t, ParamsType, db::element::remote_node>& node_params,
        db::caching::cache_response*& cache_value)
{
    assert(node_to_check != NULL);
    assert(cache_value == NULL);
    WDEBUG << "cache search for " << cache_key << " on node " << node_to_check->get_handle() << std::endl;
    if (node_to_check->cache.cache.count(cache_key) == 0){
        WDEBUG << "no cache entry exists" << std::endl;
        return true;
    } else {
        auto entry = node_to_check->cache.cache.at(cache_key);
        std::shared_ptr<node_prog::Cache_Value_Base>& cval = std::get<0>(entry);
        vc::vclock& cache_entry_time = std::get<1>(entry);
        std::shared_ptr<std::vector<db::element::remote_node>>& watch_set = std::get<2>(entry);

        int64_t cmp_1 = order::compare_two_vts(cache_entry_time, req_vclock);
        assert(cmp_1 != 2);
        if (cmp_1 == 1){
            WDEBUG << "cached value is newer, no cached value for this prog" << std::endl;
            return true;
        }
        assert(cmp_1 == 0);

        cache_value = new db::caching::cache_response();
        cache_value->value = cval;
        if (watch_set->empty()){
            //something about invalidate
            return true;
        }

        S->release_node(node_to_check);

        // map from loc to list of handles on that shard we need context from for this request
        std::unordered_map<uint64_t, std::vector<uint64_t>> contexts_to_fetch; 
        for (db::element::remote_node& watch_node : *watch_set)
        {
            if (watch_node.loc == S->shard_id) { 
                fetch_node_cache_context(watch_node.loc, watch_node.handle, cache_value->context, cache_entry_time, req_vclock);
            } else { // on another shard
                contexts_to_fetch[watch_node.loc].emplace_back(watch_node.handle);
            } 
        }
        if (contexts_to_fetch.empty()){
            WDEBUG << "returning context right away" << std::endl;
            return true;
        }

        WDEBUG << "caching waiting for rest of context to be fetched" << std::endl;
        // add waiting stuff to shard global structure

        uint64_t lookup_uid = node_to_check->cache.gen_uid();
        // map from node_handle, lookup_id to node_prog_running_state

        for (auto& shard_list_pair : contexts_to_fetch){
            std::unique_ptr<message::message> m(new message::message()); // XXX can we re-use messages?
            message::prepare_message(*m, message::NODE_CONTEXT_FETCH, vt_id, req_vclock, cache_entry_time, req_id, lookup_uid, shard_list_pair.second, S->shard_id);
            S->send(shard_list_pair.first, m->buf);
        }

        return false;
    } 
}

template <typename ParamsType, typename NodeStateType>
class node_prog_running_state {
    std::vector<std::pair<db::element::remote_node, ParamsType>> (*func)(uint64_t, 
            db::element::node&, 
            db::element::remote_node&, 
            ParamsType&,
            std::function<NodeStateType&()>,
            vc::vclock &,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            db::caching::cache_response*);
        node_prog::prog_type prog_type_recvd;
        bool global_req;
        uint64_t vt_id;
        vc::vclock& req_vclock;
        uint64_t req_id;
        std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>& start_node_params;
        db::caching::cache_response *cache_value;
};

template <typename ParamsType, typename NodeStateType>
inline void node_prog_loop(std::vector<std::pair<db::element::remote_node, ParamsType>> (*func)(uint64_t, 
            db::element::node&, 
            db::element::remote_node&, 
            ParamsType&,
            std::function<NodeStateType&()>,
            vc::vclock &,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            db::caching::cache_response*),
        node_prog::prog_type prog_type_recvd, bool global_req, uint64_t vt_id, vc::vclock& req_vclock, uint64_t req_id,
        std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>& start_node_params,
        db::caching::cache_response* cache_value)
{
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

    while (!start_node_params.empty() && !done_request) {
        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            ParamsType& params = std::get<1>(handle_params);
            this_node.handle = node_handle;
            // TODO maybe use a try-lock later so forward progress can continue on other nodes in list
            db::element::node *node = S->acquire_node(node_handle);
            if (node == NULL || order::compare_two_vts(node->get_del_time(), req_vclock)==0) { // TODO: TIMESTAMP
                if (node != NULL) {
                    S->release_node(node);
                } else {
                    // node is being migrated here, but not yet completed
                    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> buf_node_params;
                    buf_node_params.emplace_back(handle_params);
                    std::unique_ptr<message::message> m(new message::message());
                    message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, buf_node_params);
                    S->migration_mutex.lock();
                    if (S->deferred_reads.find(node_handle) == S->deferred_reads.end()) {
                        S->deferred_reads.emplace(node_handle, std::vector<std::unique_ptr<message::message>>());
                    }
                    S->deferred_reads.at(node_handle).emplace_back(std::move(m));
                    WDEBUG << "Buffering read for node " << node_handle << std::endl;
                    S->migration_mutex.unlock();
                }
            } else if (node->state == db::element::node::mode::IN_TRANSIT
                    || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node program
                //WDEBUG << "Forwarding node prog for node " << node_handle << std::endl;
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                fwd_node_params.emplace_back(handle_params);
                std::unique_ptr<message::message> m(new message::message());
                message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, fwd_node_params);
                uint64_t new_loc = node->new_loc;
                S->release_node(node);
                S->send(new_loc, m->buf);
            } else { // node does exist
                //XXX assert(node->state == db::element::node::mode::STABLE);

                if (params.search_cache()){
                    WDEBUG << "OMG WE GOT SEARCH CACHE" << std::endl;
                    if (cache_value == NULL) {
                        bool run_prog_now = cache_lookup(node, params.cache_key(), func, prog_type_recvd, global_req, vt_id, req_vclock, req_id, handle_params, cache_value); 
                        // go to next node while we fetch cache context for this one, cache_lookup releases node if false
                        if (!run_prog_now) {
                            continue;
                        }
                    } // else cache came passed in
                } else {
                    assert(cache_value == NULL); // cache value should only be passed if first prog wanted it
                }

                // bind cache getter and putter function variables to functions
                std::shared_ptr<NodeStateType> state = get_node_state<NodeStateType>(prog_type_recvd,
                        req_id, node_handle);
                node_state_getter = std::bind(return_state<NodeStateType>,
                        prog_type_recvd, req_id, node_handle, state);

                if (S->check_done_request(req_id)) {
                    done_request = true;
                    S->release_node(node);
                    break;
                }
                // call node program
                using namespace std::placeholders;
                add_cache_func = std::bind(add_cache_value, prog_type_recvd, node, _1, _2, _3, req_vclock); // 1 is cache value, 2 is watch set, 3 is key

                auto next_node_params = func(req_id, *node, this_node,
                        params, // actual parameters for this node program
                        node_state_getter, req_vclock, add_cache_func, cache_value);
               // WDEBUG << "1 got new params count" << next_node_params.size() << std::endl;
                // batch the newly generated node programs for onward propagation
                S->msg_count_mutex.lock();
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t loc = res.first.loc;
                    if (loc == vt_id) {
                        // signal to send back to vector timestamper that issued request
                        // TODO mark done
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        std::unique_ptr<message::message> m(new message::message());
                        message::prepare_message(*m, message::NODE_PROG_RETURN, prog_type_recvd, req_id, temppair);
                        S->send(vt_id, m->buf);
                    } else {
                        batched_node_progs[loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        S->agg_msg_count[node_handle]++;
                        S->request_count[loc]++; // increment count of msges sent to loc
                    }
                }
                S->msg_count_mutex.unlock();
                S->release_node(node);

                // Only per hop batching now
                for (uint64_t next_loc = SHARD_ID_INCR; next_loc < NUM_SHARDS + SHARD_ID_INCR; next_loc++) {
                    if ((batched_node_progs.find(next_loc) != batched_node_progs.end() && !batched_node_progs[next_loc].empty())
                        && next_loc != S->shard_id) {
                        std::unique_ptr<message::message> m(new message::message());
                        message::prepare_message(*m, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, batched_node_progs[next_loc]);
                        S->send(next_loc, m->buf);
                        batched_node_progs[next_loc].clear();
                    }
                }
            }
            if (cache_value != NULL){
                WDEBUG << "2 cache value deleted" << std::endl;
                delete cache_value; // we can only have cached value for first one 
                cache_value = NULL; // needed?
            }
        }
        start_node_params = std::move(batched_node_progs[S->shard_id]);

        if (S->check_done_request(req_id)) {
            done_request = true;
        }
    }
}

void
unpack_node_program(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    node_prog::prog_type pType;

    message::unpack_message(*request->msg, message::NODE_PROG, pType);
    node_prog::programs.at(pType)->unpack_and_run_db(std::move(request->msg));
    delete request;
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_run_db(std::unique_ptr<message::message> msg)
{
    // tuple of (node handle, node prog params, prev node)
    typedef std::tuple<uint64_t, ParamsType, db::element::remote_node> node_params_t;
    // unpack some start params from msg:
    std::vector<node_params_t> start_node_params;
    vc::vclock req_vclock;
    uint64_t vt_id, req_id;
    prog_type prog_type_recvd;
    bool global_req;

    bool done_request = false;

    // unpack the node program
    try {
        message::unpack_message(*msg, message::NODE_PROG, prog_type_recvd, global_req, vt_id, req_vclock, req_id, start_node_params);
        assert(req_vclock.clock.size() == NUM_VTS);
    } catch (std::bad_alloc& ba) {
        WDEBUG << "bad_alloc caught " << ba.what() << std::endl;
        assert(false);
        return;
    }
    
    // update max prog id
    S->migration_mutex.lock();
    if (S->max_prog_id[vt_id] < req_id) {
        S->max_prog_id[vt_id] = req_id;
    }
    S->migration_mutex.unlock();

    // check if request completed
    if (S->check_done_request(req_id)) {
        done_request = true;
        return;
    }

    node_prog_loop<ParamsType, NodeStateType>(enclosed_node_prog_func, prog_type_recvd, global_req, vt_id, req_vclock, req_id, start_node_params, NULL);
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

// mark node as "in transit" so that subsequent requests are queued up
// send migration information to coordinator mapper
void
migrate_node_step1(uint64_t node_handle, uint64_t shard)
{
    db::element::node *n;
    n = S->acquire_node(node_handle);
    S->migration_mutex.lock();
    if (n->updated) {
        S->release_node(n);
        S->migration_mutex.unlock();
        WDEBUG << "canceling migration for node " << node_handle << " at shard " << shard_id << std::endl;
        migration_wrapper();
    } else {
        S->current_migr = true;
        for (uint64_t &x: S->nop_count) {
            x = 0;
        }
        // mark node as "in transit"
        n->state = db::element::node::mode::IN_TRANSIT;
        n->new_loc = shard;
        S->prev_migr_node = S->migr_node;
        S->migr_node = node_handle;
        S->migr_shard = shard;
        // updating edge map
        S->edge_map_mutex.lock();
        std::unordered_set<uint64_t> seen_nodes;
        for (auto &e: n->out_edges) {
            uint64_t node = e.second->nbr.handle;
            uint64_t loc = e.second->nbr.loc;
            assert(S->edge_map.find(loc) != S->edge_map.end());
            assert(S->edge_map[loc].find(node) != S->edge_map[loc].end());
            auto &node_set = S->edge_map[loc][node];
            if (seen_nodes.find(node_handle) == seen_nodes.end()) {
                assert(node_set.find(node_handle) != node_set.end());
            }
            seen_nodes.emplace(node_handle);
            node_set.erase(node_handle);
            if (node_set.empty()) {
                S->edge_map[loc].erase(node);
            }
        }
        S->edge_map_mutex.unlock();
        S->release_node(n);
        S->update_node_mapping(node_handle, shard);
        S->migration_mutex.unlock();
    }
}

// pack node in big message and send to new location
void
migrate_node_step2_req()
{
    db::element::node *n;
    message::message msg;
    n = S->acquire_node(S->migr_node);
    assert(n != NULL);
    S->migration_mutex.lock();
    S->current_migr = false;
    message::prepare_message(msg, message::MIGRATE_SEND_NODE, S->migr_node, shard_id, *n);
    S->release_node(n);
    for (uint64_t idx = 0; idx < NUM_VTS; idx++) {
        S->target_prog_id[idx] = 0;
    }
    S->migration_mutex.unlock();
    S->send(S->migr_shard, msg.buf);
    WDEBUG << "Migrating node " << S->migr_node << " to shard " << S->migr_shard << std::endl;
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
    n->prev_loc = from_loc; // record shard from which we just migrated this node
    n->prev_locs.at(shard_id - SHARD_ID_INCR) = 1; // mark this shard as one of the previous locations
    
    // updating edge map
    S->edge_map_mutex.lock();
    for (auto &e: n->out_edges) {
        uint64_t node = e.second->nbr.handle;
        uint64_t loc = e.second->nbr.loc;
        S->edge_map[loc][node].emplace(node_handle);
    }
    S->edge_map_mutex.unlock();

    S->migration_mutex.lock();
    // apply buffered writes
    if (S->deferred_writes.find(node_handle) != S->deferred_writes.end()) {
        for (auto &def_wr: S->deferred_writes.at(node_handle)) {
            auto &write = def_wr.request;
            switch (def_wr.type) {
                case message::NODE_DELETE_REQ:
                    assert(write.del_node.node == node_handle);
                    S->delete_node_nonlocking(n, def_wr.vclk);
                    break;

                case message::EDGE_CREATE_REQ:
                    assert(write.cr_edge.n1 == node_handle);
                    S->create_edge_nonlocking(n, write.cr_edge.edge, write.cr_edge.n2,
                            write.cr_edge.loc2, def_wr.vclk);
                    break;

                case message::EDGE_DELETE_REQ:
                    assert(write.del_edge.node == node_handle);
                    S->delete_edge_nonlocking(n, write.del_edge.edge, def_wr.vclk);
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
        deferred_reads = std::move(S->deferred_reads.at(node_handle));
        S->deferred_reads.erase(node_handle);
    }
    S->migration_mutex.unlock();

    // update local nbrs
    S->update_migrated_nbr(node_handle, from_loc, shard_id);

    // apply buffered reads
    for (auto &m: deferred_reads) {
        node_prog::prog_type pType;
        message::unpack_message(*m, message::NODE_PROG, pType);
        WDEBUG << "APPLYING BUFREAD for node " << node_handle << std::endl;
        node_prog::programs.at(pType)->unpack_and_run_db(std::move(m));
    }
}

// check if all nbrs updated, if so call step3
// caution: assuming caller holds S->migration_mutex
bool
check_step3()
{
    bool init_step3 = true;
    for (int i = 0; i < NUM_SHARDS; i++) {
        if (!S->migr_edge_acks[i]) {
            init_step3 = false;
            break;
        }
    }
    for (int i = 0; i < NUM_VTS && init_step3; i++) {
        // XXX check following condition
        init_step3 = init_step3 && (S->target_prog_id[i] <= S->max_done_id[i]);
    }
    if (init_step3) {
      for (uint64_t idx = 0; idx < NUM_SHARDS; idx++) {
          S->migr_edge_acks[idx] = false;
      }
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

void
migration_wrapper()
{
    bool no_migr = true;
    while (!S->sorted_nodes.empty()) {
        db::element::node *n;
        uint64_t max_pos, migr_pos;
        uint64_t migr_node = S->sorted_nodes.front().first;
        n = S->acquire_node(migr_node);
        if (n == NULL || order::compare_two_clocks(n->get_del_time().clock, S->max_clk.clock) != 2 ||
            n->state == db::element::node::mode::IN_TRANSIT ||
            n->state == db::element::node::mode::MOVED) {
            if (n != NULL) {
                S->release_node(n);
            }
            S->sorted_nodes.pop_front();
            continue;
        }
        n->updated = false;
        db::element::edge *e;
        for (auto &e_iter: n->out_edges) {
            e = e_iter.second;
            n->msg_count.at(e->nbr.loc-SHARD_ID_INCR) += e->msg_count;
            e->msg_count = 0;
        }
        for (int j = 0; j < NUM_SHARDS; j++) {
            n->agg_msg_count.at(j) += (uint64_t)(0.8 * (double)(n->msg_count.at(j)));
        }
        max_pos = 0;
        for (uint64_t j = 0; j < n->agg_msg_count.size(); j++) {
            if (n->agg_msg_count.at(max_pos) < n->agg_msg_count.at(j)) {
                max_pos = j;
            }
        }
        migr_pos = max_pos;
        migr_pos += SHARD_ID_INCR; // fixing index
        for (uint32_t &cnt: n->msg_count) {
            cnt = 0;
        }
        S->release_node(n);
        S->sorted_nodes.pop_front();
        // no migration to self
        if (migr_pos != shard_id) {
            //WDEBUG << "migrating node " << migr_node << " to " << migr_pos << std::endl;
            migrate_node_step1(migr_node, migr_pos);
            no_migr = false;
            break;
        }
    }
    if (no_migr) {
        shard_daemon_end();
    }
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
                message::unpack_message(*rec_msg, message::TX_INIT, vt_id, vclk, qts);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, unpack_tx_request, request);
                S->add_write_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_PROG:
                bool global_req;
                message::unpack_message(*rec_msg, message::NODE_PROG, pType, global_req, vt_id, vclk, req_id);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_node_program, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_CONTEXT_FETCH:
                message::unpack_message(*rec_msg, message::NODE_CONTEXT_FETCH, vt_id, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_and_fetch_context, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::NODE_CONTEXT_REPLY:
                message::unpack_message(*rec_msg, message::NODE_CONTEXT_REPLY, vt_id, vclk);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_and_fetch_context, request);
                S->add_read_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
                break;

            case message::VT_NOP: {
                db::nop_data *nop_arg = new db::nop_data();
                message::unpack_message(*rec_msg, mtype, vt_id, vclk, qts, req_id, nop_arg->done_reqs, nop_arg->max_done_id);
                nop_arg->vt_id = vt_id;
                nop_arg->req_id = req_id;
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, nop, (void*)nop_arg);
                S->add_write_request(vt_id, thr);
                rec_msg.reset(new message::message());
                assert(vclk.clock.size() == NUM_VTS);
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
                WDEBUG << "Now obtained migration token at shard " << shard_id << std::endl;
                S->migration_mutex.lock();
                S->migr_token = true;
                S->migrated = false;
                S->migration_mutex.unlock();
                WDEBUG << "Ended obtaining token" << std::endl;
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

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                WDEBUG << "unexpected msg type " << mtype << std::endl;
        }
    }
}

bool agg_count_compare(std::pair<uint64_t, uint32_t> p1, std::pair<uint64_t, uint32_t> p2)
{
    return (p1.second > p2.second);
}

// sort nodes in order of number of requests propagated
// and (implicitly) pass sorted deque to migration wrapper
void
shard_daemon_begin()
{
    WDEBUG << "Starting shard daemon" << std::endl;
    S->msg_count_mutex.lock();
    auto agg_msg_count = std::move(S->agg_msg_count);
    assert(S->agg_msg_count.empty());
    S->msg_count_mutex.unlock();
    std::deque<std::pair<uint64_t, uint32_t>> sn;
    for (auto &p: agg_msg_count) {
        sn.emplace_back(p);
    }
    std::sort(sn.begin(), sn.end(), agg_count_compare);
    WDEBUG << "sorted nodes size " << sn.size() << std::endl;
    WDEBUG << "total num nodes " << S->num_nodes() << std::endl;
    S->sorted_nodes = std::move(sn);
    migration_wrapper();
}

void
shard_daemon_end()
{
    message::message msg;
    message::prepare_message(msg, message::MIGRATION_TOKEN);
    S->migration_mutex.lock();
    S->migr_token = false;
    S->migration_mutex.unlock();
    uint64_t next_id; 
    if ((shard_id + 1 - SHARD_ID_INCR) >= NUM_SHARDS) {
        next_id = SHARD_ID_INCR;
    } else {
        next_id = shard_id + 1;
    }
    S->send(next_id, msg.buf);
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
        db::graph_file_format format = db::SNAP;
        if (strcmp(argv[2], "tsv") == 0) {
            format = db::TSV;
        } else if (strcmp(argv[2], "snap") == 0) {
            format = db::SNAP;
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
