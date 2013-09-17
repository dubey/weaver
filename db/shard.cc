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
#include <signal.h>
#include <e/buffer.h>
#include "busybee_constants.h"

#include "common/event_order.h"
#include "common/weaver_constants.h"
#include "common/message_graph_elem.h"
#include "shard.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"

// global static variables
static uint64_t shard_id;
static db::shard *S;
db::shard *db::thread::pool::S = NULL; // reinitialized in graph constructor

void migrate_node_step1(uint64_t node_handle, uint64_t shard);
void migrate_node_step2_req();
void initiate_nbr_update(std::pair<const uint64_t, db::element::edge*> &nbr,
    uint64_t migr_node, std::vector<std::pair<uint64_t, uint64_t>> &local_nbr_updates,
    uint64_t &pending_edge_updates, message::message &msg);
void migrate_node_step2_resp(std::unique_ptr<message::message> msg);
void migrate_node_step3(uint64_t node_handle);
void migration_wrapper();
void shard_daemon_begin();
void shard_daemon_end();


inline void
create_node(vc::vclock &t_creat, uint64_t node_handle)
{
    S->create_node(node_handle, t_creat, false);
}

inline uint64_t
create_edge(vc::vclock &t_creat, uint64_t edge_handle, uint64_t n1, uint64_t n2, uint64_t loc2)
{
    return S->create_edge(edge_handle, n1, n2, loc2, t_creat);
}

inline uint64_t
delete_node(vc::vclock &t_del, uint64_t node_handle)
{
    return S->delete_node(node_handle, t_del);
}

inline uint64_t
delete_edge(vc::vclock &t_del, uint64_t edge_handle, uint64_t node_handle)
{
    return S->delete_edge(edge_handle, node_handle, t_del);
}

inline uint64_t
create_reverse_edge(vc::vclock &vclk, uint64_t edge_handle, uint64_t local_node, uint64_t remote_node, uint64_t remote_loc)
{
    return S->create_reverse_edge(edge_handle, local_node, remote_node, remote_loc, vclk);
}

void
migrated_nbr_update(std::unique_ptr<message::message> msg)
{
    uint64_t local_node, remote_node, edge, new_loc;
    message::unpack_message(*msg, message::MIGRATED_NBR_UPDATE, local_node, edge, remote_node, new_loc);
    S->update_migrated_nbr(local_node, edge, remote_node, new_loc);
}

void
unpack_update_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    uint64_t handle, elem1, elem2, loc2, ret;

    switch (request->type) {
        case message::REVERSE_EDGE_CREATE:
            DEBUG << "reverse edge create" << std::endl;
            message::unpack_message(*request->msg, request->type, vclk, handle, elem1, elem2, loc2);
            ret = create_reverse_edge(vclk, handle, elem1, elem2, loc2);
            break;

        case message::MIGRATED_NBR_UPDATE:
            migrated_nbr_update(std::move(request->msg));
            break;

        case message::MIGRATE_SEND_NODE:
            migrate_node_step2_resp(std::move(request->msg));
            break;

        case message::MIGRATE_DONE:
            message::unpack_message(*request->msg, request->type, handle);
            migrate_node_step3(handle);
            break;

        default:
            DEBUG << "unknown type" << std::endl;
    }
    //if (ret == 0) {
    //    // update successful
    //} else {
    //    // node being migrated, tx needs to be forwarded
    //    // TODO
    //}
    delete request;
}

void
unpack_tx_request(void *req)
{
    db::graph_request *request = (db::graph_request*)req;
    uint64_t vt_id, tx_id, ret;
    vc::vclock vclk;
    vc::qtimestamp_t qts;
    transaction::pending_tx tx;
    bool ack = true;
    message::unpack_message(*request->msg, message::TX_INIT, vt_id, vclk, qts, tx_id, tx.writes);
    ret = 0;
    for (auto upd: tx.writes) {
        switch (upd->type) {
            case transaction::NODE_CREATE_REQ:
                DEBUG << "unpacked node create" << std::endl;
                create_node(vclk, upd->handle);
                S->record_completed_transaction(vt_id, tx_id); // TODO: only do this once per transaction
                DEBUG << "node create vt_id " << vt_id << std::endl;
                DEBUG << "done node create" << std::endl;
                ret = 0;
                break;

            case transaction::EDGE_CREATE_REQ:
                ret = create_edge(vclk, upd->handle, upd->elem1, upd->elem2, upd->loc2);
                S->record_completed_transaction(vt_id, tx_id);
                DEBUG << "edge create vt_id " << vt_id << std::endl;
                break;

            case transaction::NODE_DELETE_REQ:
                ret = delete_node(vclk, upd->elem1);
                S->record_completed_transaction(vt_id, tx_id);
                DEBUG << "node delete vt_id " << vt_id << std::endl;
                break;

            case transaction::EDGE_DELETE_REQ:
                ret = delete_edge(vclk, upd->elem1, upd->elem2);
                S->record_completed_transaction(vt_id, tx_id);
                DEBUG << "edge delete vt_id " << vt_id << std::endl;
                break;

            default:
                DEBUG << "unknown type" << std::endl;
        }
        DEBUG << "done tx subpart\n";
        //if (ret == 0) {
        //    // tx subpart successful
        //} else {
        //    // node being migrated, tx needs to be forwarded
        //    // TODO also need to maintain DS for tracking when to ack transaction, in case of migration
        //    ack = false;
        //}
    }
    delete request;
    if (ack) {
        // send tx confirmation to coordinator
        message::message conf_msg;
        message::prepare_message(conf_msg, message::TX_DONE, tx_id);
        S->send(vt_id, conf_msg.buf);
    }
}

// process nop
// migration-related checks, and possibly initiating migration
inline void
nop(void *noparg)
{
    std::pair<uint64_t, uint64_t> *nop_arg = (std::pair<uint64_t, uint64_t>*)noparg;
    //DEBUG << "nop vt_id " << nop_arg->first << ", qts " << nop_arg->second << std::endl;
    S->record_completed_transaction(nop_arg->first, nop_arg->second);
    message::message msg;
    message::prepare_message(msg, message::VT_NOP_ACK);
    S->send(nop_arg->first, msg.buf);
    // increment nop count, trigger migration step 2 after check
    bool move_migr_node = true;
    bool initiate_migration = false;
    S->migration_mutex.lock();
    if (S->current_migr) {
        S->nop_count.at(nop_arg->first)++;
        for (uint64_t &x: S->nop_count) {
            move_migr_node = move_migr_node && (x == 2);
        }
    } else {
        move_migr_node = false;
    }
    if (!S->migrated && S->migr_token) {
        if (S->migr_chance++ > 2) {
            S->migrated = true;
            initiate_migration = true;
            S->migr_chance = 0;
            DEBUG << "Going to start migration at shard " << shard_id << std::endl;
        } else {
            DEBUG << "Migr chance cntr = " << S->migr_chance << std::endl;
        }
    }
    S->migration_mutex.unlock();
    free(nop_arg);
    assert(!(move_migr_node && initiate_migration));
    if (move_migr_node) {
        migrate_node_step2_req();
    } else if (initiate_migration) {
        shard_daemon_begin();
    }
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
                std::dynamic_pointer_cast<node_prog::Packable_Deletable>(newState));
        return *newState;
    }
}

void
unpack_node_program(void *req)
{
    db::graph_request *request = (db::graph_request *) req;
    node_prog::prog_type pType;

    DEBUG << "node program got of priority queue to run!" << std::endl;
    message::unpack_message(*request->msg, message::NODE_PROG, pType);
    node_prog::programs.at(pType)->unpack_and_run_db(std::move(request->msg));
    delete request;
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_run_db(std::unique_ptr<message::message> msg)
{
    DEBUG << "node program running in templated function!" << std::endl;
    // unpack some start params from msg:
    std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> start_node_params;
    uint64_t vt_id;
    uint64_t req_id;
    vc::vclock req_vclock;
    //std::vector<uint64_t> vclocks; //needed to pass to next message
    prog_type prog_type_recvd;
    bool done_request = false;
    /*
    // map from loc to send back to the handle that was deleted, the params it was given, and the handle to send back to
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, uint64_t>>> batched_deleted_nodes; 
    */

    // map from location to send to next to tuple of handle and params to send to next, and node that sent them
    // these are the node programs that will be propagated onwards
    std::unordered_map<uint64_t, std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>>> batched_node_progs;
    db::element::remote_node this_node(S->shard_id, 0);
    uint64_t node_handle, init_num_prog;

    // unpack the node program
    try {
        message::unpack_message(*msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, start_node_params);
        DEBUG << "node program unpacked" << std::endl;
        assert(req_vclock.clock.size() == NUM_VTS);
        init_num_prog = start_node_params.size();
        //, dirty_cache_ids, invalid_cache_ids, batched_deleted_nodes[G->myid]);
        /*
#ifdef __WEAVER_DEBUG__
if (batched_deleted_nodes[G->myid].size() == 1 && std::get<0>(batched_deleted_nodes[G->myid].at(0)) == MAX_TIME) {
        //DEBUG << "Unpacking forwarded request in unpack_and_run for node "
        //    << std::get<0>(start_node_params.at(0)) << std::endl;
        batched_deleted_nodes[G->myid].clear();
        }
#endif
         */
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << '\n';
        return;
    }

    // node state and cache functions
    std::function<NodeStateType&()> node_state_getter;

    // check if request completed
    if (S->check_done_request(req_id)) {
        done_request = true;
    }
    while ((!start_node_params.empty() /*|| !batched_deleted_nodes[G->myid].empty()*/) && !done_request) {
        DEBUG << "node program main loop" << std::endl;
        /*
        // deleted nodes loop TODO
        */

        for (auto &handle_params : start_node_params) {
            node_handle = std::get<0>(handle_params);
            ParamsType params = std::get<1>(handle_params);
            this_node.handle = node_handle;
            // TODO maybe use a try-lock later so forward progress can continue on other nodes in list
            db::element::node *node = S->acquire_node(node_handle);
            DEBUG << "node acquired!" << std::endl;
            if (node == NULL || order::compare_two_vts(node->get_del_time(), req_vclock)==0) { // TODO: TIMESTAMP
                DEBUG << "migrating node" << std::endl;
                if (node != NULL) {
                    S->release_node(node);
                } else {
                    // node is being migrated here, but not yet completed
                    assert(init_num_prog == 1);
                    S->migration_mutex.lock();
                    if (S->deferred_reads.find(node_handle) == S->deferred_reads.end()) {
                        S->deferred_reads.emplace(node_handle, std::vector<std::unique_ptr<message::message>>());
                    }
                    S->deferred_reads.at(node_handle).emplace_back(std::move(msg));
                    S->migration_mutex.unlock();
                }

                /*
                db::element::remote_node parent = std::get<2>(handle_params);
                batched_deleted_nodes[parent.loc].emplace_back(std::make_tuple(node_handle, params, parent.handle));
                */
                DEBUG << "Node " << node_handle << " deleted, cur request num " << req_id << std::endl;
            } else if (node->state == db::element::node::mode::IN_TRANSIT
                    || node->state == db::element::node::mode::MOVED) {
                // queueing/forwarding node program
                std::vector<std::tuple<uint64_t, ParamsType, db::element::remote_node>> fwd_node_params;
                /*
                std::vector<std::tuple<uint64_t, ParamsType, uint64_t>> dummy_deleted_nodes; 
#ifdef __WEAVER_DEBUG__
                dummy_deleted_nodes.emplace_back(std::make_tuple(MAX_TIME, ParamsType(), MAX_TIME));
#endif
                */
                fwd_node_params.emplace_back(handle_params);
                message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, fwd_node_params);
                uint64_t new_loc = node->new_loc;
                S->release_node(node);
                S->send(new_loc, msg->buf);
            } else { // node does exist
                DEBUG << "getting state" << std::endl;
                //XXX assert(node->state == db::element::node::mode::STABLE);
                // bind cache getter and putter function variables to functions
                std::shared_ptr<NodeStateType> state = get_node_state<NodeStateType>(prog_type_recvd,
                        req_id, node_handle);
                node_state_getter = std::bind(return_state<NodeStateType>,
                        prog_type_recvd, req_id, node_handle, state);
                /*
                cache_value_putter = std::bind(put_cache_value<CacheValueType>,
                        prog_type_recvd, req_id, node_handle, node, &dirty_cache_ids);
                cached_values_getter = std::bind(get_cached_values<CacheValueType>,
                        prog_type_recvd, req_id, node_handle, &dirty_cache_ids, std::ref(invalid_cache_ids));
                */

                if (S->check_done_request(req_id)) {
                    done_request = true;
                    S->release_node(node);
                    break;
                }
                // call node program
                auto next_node_params = enclosed_node_prog_func(req_id, *node, this_node,
                        params, // actual parameters for this node program
                        node_state_getter, req_vclock);
                /*
                        cache_value_putter,
                        cached_values_getter);
                        */
                // batch the newly generated node programs for onward propagation
                for (std::pair<db::element::remote_node, ParamsType> &res : next_node_params) {
                    uint64_t loc = res.first.loc;
                    DEBUG << "Node prog has to be sent to loc " << loc << std::endl;
                    if (loc == vt_id) {
                        // signal to send back to vector timestamper that issued request
                        // TODO mark done
                        // XXX get rid of pair, without pair it is not working for some reason
                        std::pair<uint64_t, ParamsType> temppair = std::make_pair(1337, res.second);
                        message::prepare_message(*msg, message::NODE_PROG_RETURN, req_id, temppair);
                        S->send(vt_id, msg->buf);
                    } else {
                        DEBUG << "forwarding node prog to shard " << loc << std::endl;
                        batched_node_progs[loc].emplace_back(res.first.handle, std::move(res.second), this_node);
                        if (!MSG_BATCHING && (loc != S->shard_id)) {
                            message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, batched_node_progs[loc]);
                            batched_node_progs[loc].clear();
                            //batched_deleted_nodes[loc].clear();
                            S->send(loc, msg->buf);
                        }
                        S->msg_count_mutex.lock();
                        S->agg_msg_count[node_handle]++;
                        S->request_count[loc-1]++; // increment count of msges sent to loc
                        S->msg_count_mutex.unlock();
                    }
                }
                S->release_node(node);
            }
            if (MSG_BATCHING) {
                for (uint64_t next_loc = SHARD_ID_INCR; next_loc < NUM_SHARDS + SHARD_ID_INCR; next_loc++) {
                    if (((!batched_node_progs[next_loc].empty() && batched_node_progs[next_loc].size()>BATCH_MSG_SIZE)
                         /*|| (!batched_deleted_nodes[next_loc].empty())*/)
                        && next_loc != S->shard_id) {
                        message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, batched_node_progs[next_loc]);
                        S->send(next_loc, msg->buf);
                        batched_node_progs[next_loc].clear();
                        //batched_deleted_nodes[next_loc].clear();
                    }
                }
            }
        }
        start_node_params = std::move(batched_node_progs[S->shard_id]);
        if (S->check_done_request(req_id)) {
            done_request = true;
        }
    }

    // propagate all remaining node progs
    for (uint64_t next_loc = SHARD_ID_INCR; next_loc < NUM_SHARDS + SHARD_ID_INCR && !done_request; next_loc++) {
        if (((!batched_node_progs[next_loc].empty())
          /*|| (!batched_deleted_nodes[next_loc].empty())*/)
            && next_loc != S->shard_id) {
            message::prepare_message(*msg, message::NODE_PROG, prog_type_recvd, vt_id, req_vclock, req_id, batched_node_progs[next_loc]);
            S->send(next_loc, msg->buf);
            batched_node_progs[next_loc].clear();
            //batched_deleted_nodes[next_loc].clear();
        }
    }
}

template <typename ParamsType, typename NodeStateType>
void node_prog :: particular_node_program<ParamsType, NodeStateType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID)
{
    UNUSED(msg);
    UNUSED(clientID);
}

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
        DEBUG << "canceling migration" << std::endl;
        migration_wrapper();
    } else {
        S->current_migr = true;
        for (uint64_t &x: S->nop_count) {
            x = 0;
        }
        // mark node as "in transit"
        n->state = db::element::node::mode::IN_TRANSIT;
        n->new_loc = shard;
        // pack entire node info in a ginormous message and send to new loc
        S->prev_migr_node = S->migr_node; // TODO corner case may prevent deletion
        S->migr_node = node_handle;
        S->migr_shard = shard;
        // TODO send new loc information to coordinator map
        //message::prepare_message(msg, message::COORD_NODE_MIGRATE, S->mrequest.cur_node, n->new_loc);
        //S->send(COORD_ID, msg.buf);
        S->release_node(n);
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
    S->send(S->migr_shard, msg.buf);
    S->migration_mutex.unlock();
    S->release_node(n);
}

void
initiate_nbr_update(std::pair<const uint64_t, db::element::edge*> &nbr,
    uint64_t migr_node, std::vector<std::pair<uint64_t, uint64_t>> &local_nbr_updates,
    uint64_t &pending_edge_updates, message::message &msg)
{
    if (nbr.second->nbr.loc == shard_id) {
        local_nbr_updates.emplace_back(std::make_pair(nbr.second->nbr.handle, nbr.first));
    } else {
        // remote node, need to send msg
        message::prepare_message(msg, message::MIGRATED_NBR_UPDATE,
            nbr.second->nbr.handle, nbr.first, migr_node, shard_id);
        S->send(nbr.second->nbr.loc, msg.buf);
        pending_edge_updates++;
    }
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
    S->create_node(node_handle, dummy_clock, true);
    n = S->acquire_node(node_handle);
    std::vector<uint64_t>().swap(n->agg_msg_count);
    try {
        message::unpack_message(*msg, message::MIGRATE_SEND_NODE, node_handle, from_loc, *n);
    } catch (std::bad_alloc& ba) {
        DEBUG << "bad_alloc caught " << ba.what() << std::endl;
        return;
    }
    n->prev_loc = from_loc; // record shard from which we just migrated this node
    n->prev_locs.at(shard_id - SHARD_ID_INCR) = 1; // mark this shard as one of the previous locations

    S->migration_mutex.lock();
    // apply buffered writes
    if (S->deferred_writes.find(node_handle) != S->deferred_writes.end()) {
        for (auto &def_wr: S->deferred_writes.at(node_handle)) {
            switch (def_wr.type) {
                case message::NODE_DELETE_REQ:
                    delete_node(def_wr.vclk, def_wr.request.del_node.node);
                    break;

                case message::EDGE_DELETE_REQ:
                    delete_edge(def_wr.vclk, def_wr.request.del_edge.edge, def_wr.request.del_edge.node);
                    break;

                case message::EDGE_CREATE_REQ:
                    create_edge(def_wr.vclk, def_wr.request.cr_edge.edge, def_wr.request.cr_edge.n1, def_wr.request.cr_edge.n2, def_wr.request.cr_edge.loc2);
                    break;

                default:
                    DEBUG << "unexpected type" << std::endl;
            }
        }
        S->deferred_writes.erase(node_handle);
    }

    // apply buffered reads
    if (S->deferred_reads.find(node_handle) != S->deferred_reads.end()) {
        for (auto &m: S->deferred_reads.at(node_handle)) {
            node_prog::prog_type pType;
            message::unpack_message(*m, message::NODE_PROG, pType);
            node_prog::programs.at(pType)->unpack_and_run_db(std::move(m));
        }
        S->deferred_reads.erase(node_handle);
    }

    S->migration_mutex.unlock();

    // update nbrs
    uint64_t pending_edge_updates = 0;
    std::vector<std::pair<uint64_t, uint64_t>> local_nbr_updates;
    for (auto &nbr: n->in_edges) {
        initiate_nbr_update(nbr, node_handle, local_nbr_updates, pending_edge_updates, *msg);
    }
    for (auto &nbr: n->out_edges) {
        initiate_nbr_update(nbr, node_handle, local_nbr_updates, pending_edge_updates, *msg);
    }
    n->state = db::element::node::mode::STABLE;
    S->release_node(n);
    for (auto &u: local_nbr_updates) {
        S->update_migrated_nbr(u.first, u.second, node_handle, shard_id);
    }

    // ack to prev loc
    message::prepare_message(*msg, message::MIGRATE_DONE, node_handle);
    S->send(from_loc, msg->buf);
}

// successfully migrated node to new location, continue migration process
void
migrate_node_step3(uint64_t node_handle)
{
    assert(node_handle == S->migr_node);
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
        for (auto &e_iter: n->in_edges) {
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
            DEBUG << "migrating node " << migr_node << " to " << migr_pos << std::endl;
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
    std::pair<uint64_t, uint64_t> *nop_arg;

    while (true) {
        if ((ret = S->bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "msg recv error: " << ret << " at shard " << S->shard_id << std::endl;
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
                DEBUG << "got tx_init" << std::endl;
                message::unpack_message(*rec_msg, message::TX_INIT, vt_id, vclk, qts);
                DEBUG << "unpacked message" << std::endl;
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, unpack_tx_request, request);
                DEBUG << "going to add request" << std::endl;
                S->add_write_request(vt_id, thr);
                DEBUG << "added request to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::REVERSE_EDGE_CREATE:
                message::unpack_message(*rec_msg, mtype, vclk);
                DEBUG << "unpacked reverse edge create message" << std::endl;
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, vclk, unpack_update_request, request);
                S->add_write_request(0, thr);
                DEBUG << "added request to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::NODE_PROG:
                DEBUG << "got node_prog" << std::endl;
                message::unpack_message(*rec_msg, message::NODE_PROG, pType, vt_id, vclk, req_id);
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(req_id, vclk, unpack_node_program, request);
                DEBUG << "going to add node prog to queue for vt " << vt_id << std::endl;
                S->add_read_request(vt_id, thr);
                DEBUG << "added node prog to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::VT_NOP:
                message::unpack_message(*rec_msg, mtype, vt_id, vclk, qts, req_id);
                //DEBUG << "unpacked message" << std::endl;
                nop_arg = (std::pair<uint64_t, uint64_t>*)malloc(sizeof(std::pair<uint64_t, uint64_t>));
                nop_arg->first = vt_id;
                nop_arg->second = req_id;
                //DEBUG << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$after unpacking nop, vt_id = " << vt_id << std::endl;
                thr = new db::thread::unstarted_thread(qts.at(shard_id-SHARD_ID_INCR), vclk, nop, (void*)nop_arg);
                S->add_write_request(vt_id, thr);
                //DEBUG << "added request to threadpool" << std::endl;
                rec_msg.reset(new message::message());
                break;

            case message::MIGRATE_SEND_NODE:
            case message::MIGRATE_DONE:
            case message::MIGRATED_NBR_UPDATE:
                request = new db::graph_request(mtype, std::move(rec_msg));
                thr = new db::thread::unstarted_thread(0, S->zero_clk, unpack_update_request, request);
                S->add_write_request(vt_id, thr);
                rec_msg.reset(new message::message());
                break;

            case message::MIGRATION_TOKEN:
                DEBUG << "Now obtained migration token at shard " << shard_id << std::endl;
                S->migration_mutex.lock();
                S->migr_token = true;
                S->migrated = false;
                S->migration_mutex.unlock();
                DEBUG << "Ended obtaining token" << std::endl;
                break;

            case message::EXIT_WEAVER:
                exit(0);
                
            default:
                DEBUG << "unexpected msg type " << mtype << std::endl;
        }
        assert(vclk.clock.size() == NUM_VTS);
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
    DEBUG << "Starting shard daemon" << std::endl;
    S->msg_count_mutex.lock();
    auto agg_msg_count = std::move(S->agg_msg_count);
    assert(S->agg_msg_count.empty());
    S->msg_count_mutex.unlock();
    std::deque<std::pair<uint64_t, uint32_t>> sn;
    for (auto &p: agg_msg_count) {
        sn.emplace_back(p);
    }
    std::sort(sn.begin(), sn.end(), agg_count_compare);
    S->sorted_nodes = std::move(sn);
    DEBUG << "sorted nodes size " << sn.size() << std::endl;
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
    uint64_t next_id = (shard_id + 1 - SHARD_ID_INCR) > NUM_SHARDS ? SHARD_ID_INCR : (shard_id + 1);
    S->send(next_id, msg.buf);
}

int
main(int argc, char* argv[])
{
    // TODO signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <myid>" << std::endl;
        return -1;
    }
    uint64_t id = atoi(argv[1]);
    shard_id = id;
    S = new db::shard(id);
    std::cout << "Weaver: shard instance " << S->shard_id << std::endl;

    // TODO migration methods init

    msgrecv_loop();

    return 0;
}
