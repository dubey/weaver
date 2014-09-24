/*
 * ===============================================================
 *    Description:  Graph state corresponding to the partition
 *                  stored on this shard server.
 *
 *        Created:  07/25/2013 12:46:05 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_shard_h_
#define weaver_db_shard_h_

#include <set>
#include <map>
#include <vector>
#include <unordered_map>
#include <po6/threads/mutex.h>
#include <po6/net/location.h>
#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>

#include "node_prog/base_classes.h"
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/cache_constants.h"
#include "common/ids.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/comm_wrapper.h"
#include "common/event_order.h"
#include "common/configuration.h"
#include "common/server_manager_link_wrapper.h"
#include "common/bool_vector.h"
#include "common/utils.h"
#include "db/shard_constants.h"
#include "db/element.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/queue_manager.h"
#include "db/deferred_write.h"
#include "db/del_obj.h"
#include "db/hyper_stub.h"

namespace db
{
    enum graph_file_format
    {
        // edge list
        TSV,
        // edge list, ignore comment lines beginning with "#"
        // first line must be a comment with number of nodes, e.g. "#42"
        SNAP,
        // list of node ids with corresponding shard ids, then edge list
        // first line must be of format "#<num_nodes>", e.g. "#42"
        // each edge followed by list of props (list of key-value pairs)
        WEAVER
    };

    // graph partition state and associated data structures
    class shard
    {
        public:
            shard(uint64_t serverid, po6::net::location &loc);
            void init(uint64_t shardid);

            // Messaging infrastructure
            common::comm_wrapper comm;

            // Server manager
            po6::threads::mutex config_mutex, exit_mutex;
            server_manager_link_wrapper sm_stub;
            configuration config, prev_config;
            bool active_backup, first_config, pause_bb;
            po6::threads::cond backup_cond, first_config_cond;
            void reconfigure();
            void update_members_new_config();
            bool to_exit;

            // Consistency
        public:
            queue_manager qm;
            std::vector<order::oracle*> time_oracles;
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void record_completed_tx(vc::vclock &tx_clk);
            element::node* acquire_node(const node_handle_t &node_handle);
            element::node* acquire_node_write(const node_handle_t &node, uint64_t vt_id, uint64_t qts);
            element::node* acquire_node_nonlocking(const node_handle_t &node_handle);
            void release_node_write(element::node *n);
            void release_node(element::node *n, bool migr_node);

            // Graph state
            po6::threads::mutex edge_map_mutex;
            po6::threads::mutex node_map_mutexes[NUM_NODE_MAPS];
            uint64_t shard_id;
            server_id serv_id;
            std::unordered_map<node_handle_t, element::node*> nodes[NUM_NODE_MAPS]; // node handle -> ptr to node object
            std::unordered_map<node_handle_t, // node handle n ->
                std::unordered_set<node_handle_t>> edge_map; // in-neighbors of n
        public:
            element::node* create_node(const node_handle_t &node_handle,
                vc::vclock &vclk,
                bool migrate,
                bool init_load);
            void delete_node_nonlocking(element::node *n,
                vc::vclock &tdel);
            void delete_node(const node_handle_t &node_handle,
                vc::vclock &vclk,
                uint64_t qts);
            void create_edge_nonlocking(element::node *n,
                const edge_handle_t &handle,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vc::vclock &vclk,
                bool init_load);
            void create_edge(const edge_handle_t &handle,
                const node_handle_t &local_node,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vc::vclock &vclk,
                uint64_t qts);
            void delete_edge_nonlocking(element::node *n,
                const edge_handle_t &edge_handle,
                vc::vclock &tdel);
            void delete_edge(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                vc::vclock &vclk,
                uint64_t qts);
            // properties
            void set_node_property_nonlocking(element::node *n,
                std::string &key, std::string &value,
                vc::vclock &vclk);
            void set_node_property(const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vc::vclock &vclk,
                uint64_t qts);
            void set_edge_property_nonlocking(element::node *n,
                const edge_handle_t &edge_handle,
                std::string &key, std::string &value,
                vc::vclock &vclk);
            void set_edge_property(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vc::vclock &vclk,
                uint64_t qts);
            uint64_t get_node_count();
            bool node_exists_nonlocking(const node_handle_t &node_handle);

            // Initial graph loading
            po6::threads::mutex graph_load_mutex;
            uint64_t max_load_time, bulk_load_num_shards;
            uint32_t load_count;
            void bulk_load_persistent();

            // Permanent deletion
        public:
            po6::threads::mutex perm_del_mutex;
            std::deque<del_obj*> perm_del_queue;
            void delete_migrated_node(const node_handle_t &migr_node);
            void permanent_delete_loop(uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle);
        private:
            void permanent_node_delete(element::node *n);

            // Migration
        public:
            po6::threads::mutex migration_mutex;
            std::unordered_set<node_handle_t> node_list; // list of node ids currently on this shard
            bool current_migr, migr_updating_nbrs, migr_token, migrated;
            node_handle_t migr_node;
            uint64_t migr_chance, migr_shard, migr_token_hops, migr_num_shards, migr_vt;
#ifdef WEAVER_CLDG
            std::unordered_map<node_handle_t, uint32_t> agg_msg_count;
            std::vector<std::pair<node_handle_t, uint32_t>> cldg_nodes;
            std::vector<std::pair<node_handle_t, uint32_t>>::iterator cldg_iter;
            po6::threads::mutex msg_count_mutex;
#endif
#ifdef WEAVER_NEW_CLDG
            std::unordered_map<node_handle_t, uint32_t> agg_msg_count;
            std::vector<std::pair<node_handle_t, uint32_t>> cldg_nodes;
            std::vector<std::pair<node_handle_t, uint32_t>>::iterator cldg_iter;
            po6::threads::mutex msg_count_mutex;
#endif
            std::unordered_set<node_handle_t> ldg_nodes;
            std::unordered_set<node_handle_t>::iterator ldg_iter;
            std::vector<uint64_t> shard_node_count;
            std::unordered_map<node_handle_t, def_write_lst> deferred_writes; // for migrating nodes
            std::unordered_map<node_handle_t, std::vector<std::unique_ptr<message::message>>> deferred_reads; // for migrating nodes
            std::vector<uint64_t> nop_count;
            vc::vclock max_clk // to compare against for checking if node is deleted
                , zero_clk; // all zero clock for migration thread in queue
            void update_migrated_nbr_nonlocking(element::node *n, const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc);
            void update_migrated_nbr(const node_handle_t &node, uint64_t old_loc, uint64_t new_loc);
            void update_node_mapping(const node_handle_t &node, uint64_t shard);
            std::vector<uint64_t> max_prog_id // max prog id seen from each vector timestamper
                , target_prog_id
                , max_done_id; // max id done from each VT
            std::vector<vc::vclock_t> max_done_clk; // vclk of cumulative last node program completed
            std::vector<bool> migr_edge_acks;

            // node programs
        private:
            po6::threads::mutex node_prog_state_mutex;
            std::unordered_set<uint64_t> done_ids; // request ids that have finished
            std::unordered_map<uint64_t, std::vector<node_handle_t>> outstanding_prog_states; // maps request_id to list of nodes that have created prog state for that req id
            void delete_prog_states(uint64_t req_id, std::vector<node_handle_t> &node_handles);
        public:
            void mark_nodes_using_state(uint64_t req_id, std::vector<node_handle_t> &node_handles);
            void add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests);
            bool check_done_request(uint64_t req_id);

            std::unordered_map<std::tuple<cache_key_t, uint64_t, node_handle_t>, void *> node_prog_running_states; // used for fetching cache contexts
            po6::threads::mutex node_prog_running_states_mutex;

            po6::threads::mutex watch_set_lookups_mutex;
            uint64_t watch_set_lookups;
            uint64_t watch_set_nops;
            uint64_t watch_set_piggybacks;

            // Fault tolerance
        public:
            std::vector<hyper_stub*> hstub;
            void restore_backup();
    };

    inline
    shard :: shard(uint64_t serverid, po6::net::location &loc)
        : comm(loc, NUM_SHARD_THREADS, SHARD_MSGRECV_TIMEOUT)
        , sm_stub(server_id(serverid), comm.get_loc())
        , active_backup(false)
        , first_config(false)
        , pause_bb(false)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , to_exit(false)
        , shard_id(UINT64_MAX)
        , serv_id(serverid)
        , current_migr(false)
        , migr_updating_nbrs(false)
        , migr_token(false)
        , migrated(false)
        , migr_chance(0)
        , nop_count(NumVts, 0)
        , max_clk(UINT64_MAX, UINT64_MAX)
        , zero_clk(0, 0)
        , max_prog_id(NumVts, 0)
        , target_prog_id(NumVts, 0)
        , max_done_id(NumVts, 0)
        , max_done_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , watch_set_lookups(0)
        , watch_set_nops(0)
        , watch_set_piggybacks(0)
    {
    }

    // initialize: msging layer
    //           , chronos client
    //           , hyperdex stub
    // caution: assume holding config_mutex
    inline void
    shard :: init(uint64_t shardid)
    {
        shard_id = shardid;
        for (int i = 0; i < NUM_SHARD_THREADS; i++) {
            hstub.push_back(new hyper_stub(shard_id));
            time_oracles.push_back(new order::oracle());
        }
    }

    // reconfigure shard according to new cluster configuration
    // caution: assume holding config_mutex
    inline void
    shard :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure" << std::endl;

        comm.reconfigure(config, pause_bb);
        update_members_new_config();
    }

    inline void
    shard :: update_members_new_config()
    {
        std::vector<server> servers = config.get_servers();

        // get num shards
        std::unordered_set<uint64_t> shard_set;
        for (const server &srv: servers) {
            if (srv.type == server::SHARD
             && srv.state != server::ASSIGNED) {
                shard_set.emplace(srv.virtual_id);
            }
        }
        uint64_t num_shards = shard_set.size();

        // resize migration ds
        migration_mutex.lock();
        shard_node_count.resize(num_shards, 0);
        if (migr_updating_nbrs) {
            // currently sent out request to update nbrs
            // set all new members to true
            migr_edge_acks.resize(num_shards, true);
        } else {
            migr_edge_acks.resize(num_shards, false);
        }
        migration_mutex.unlock();

        // update config constants
        update_config_constants(num_shards);

        // activate if backup
        uint64_t vid = config.get_virtual_id(serv_id);
        if (vid != UINT64_MAX) {
            active_backup = true;
            backup_cond.signal();
        }

        // reset qts if a VTS died
        //std::vector<server> delta = prev_config.delta(config);
        //for (const server &srv: delta) {
        //    if (srv.type == server::VT) {
        //        server::state_t prev_state = prev_config.get_state(srv.id);
        //        if ((prev_state == server::AVAILABLE || prev_state == server::ASSIGNED)
        //         && (srv.state != server::AVAILABLE && srv.state != server::ASSIGNED)) {
        //            uint64_t vt_id = srv.virtual_id;
        //            // reset qts for vt_id
        //            qm.reset(vt_id, config.version());
        //            WDEBUG << "reset qts for vt " << vt_id << std::endl;
        //        }
        //    }
        //}

        // drop reads
        //qm.clear_queued_reads();
    }

    inline void
    shard :: bulk_load_persistent()
    {
        std::vector<std::thread> threads;
        WDEBUG << "hstub.size " << hstub.size() << ", NUM_SHARD_THREADS " << NUM_SHARD_THREADS << std::endl;
        for (uint64_t i = 0; i < hstub.size(); i++) {
            threads.emplace_back(std::thread(&hyper_stub::bulk_load, hstub[i], (int)i, nodes));
        }
        for (uint64_t i = 0; i < hstub.size(); i++) {
            threads[i].join();
        }
    }

    // Consistency methods
    inline void
    shard :: increment_qts(uint64_t vt_id, uint64_t incr)
    {
        qm.increment_qts(vt_id, incr);
    }

    inline void
    shard :: record_completed_tx(vc::vclock &tx_clk)
    {
        qm.record_completed_tx(tx_clk);
    }

    // find the node corresponding to given id
    // lock and return the node
    // return NULL if node does not exist (possibly permanently deleted)
    inline element::node*
    shard :: acquire_node(const node_handle_t &node_handle)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        element::node *n = NULL;
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            n = node_iter->second;
            n->waiters++;
            while (n->in_use) {
                n->cv.wait();
            }
            n->waiters--;
            n->in_use = true;
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    inline element::node*
    shard :: acquire_node_write(const node_handle_t &node_handle, uint64_t vt_id, uint64_t qts)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        element::node *n = NULL;
        auto comp = std::make_pair(vt_id, qts);
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            n = node_iter->second;
            n->waiters++;
            // first wait for node to become free
            while (n->in_use) {
                n->cv.wait();
            }
            // check if write exists in queue---in case we are recovering from failure
            bool exists = false;
            for (auto &p: n->tx_queue) {
                if (p == comp) {
                    exists = true;
                    break;
                }
            }
            if (exists && n->tx_queue.front() != comp) {
                // write exists in queue, but cannot be executed right now
                while (n->in_use || n->tx_queue.front() != comp) {
                    n->cv.wait();
                }
                n->tx_queue.pop_front();
            } else if (exists) {
                // write exists in queue and is the first
                n->tx_queue.pop_front();
            }
            n->waiters--;
            n->in_use = true;
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    inline element::node*
    shard :: acquire_node_nonlocking(const node_handle_t &node_handle)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        element::node *n = NULL;
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            n = node_iter->second;
        }
        return n;
    }

    inline void
    shard :: release_node_write(element::node *n)
    {
        release_node(n, false);
    }

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(element::node *n, bool migr_done=false)
    {
        uint64_t map_idx = hash_node_handle(n->get_handle()) % NUM_NODE_MAPS;

        node_map_mutexes[map_idx].lock();
        n->in_use = false;
        if (migr_done) {
            n->migr_cv.broadcast();
        }
        if (n->waiters > 0) {
            n->cv.signal();
            node_map_mutexes[map_idx].unlock();
        } else if (n->permanently_deleted) {
            const node_handle_t &node_handle = n->get_handle();
            nodes[map_idx].erase(node_handle);
            node_map_mutexes[map_idx].unlock();

            migration_mutex.lock();
            node_list.erase(node_handle);
            shard_node_count[shard_id - ShardIdIncr]--;
            migration_mutex.unlock();

#ifdef WEAVER_CLDG
            msg_count_mutex.lock();
            agg_msg_count.erase(node_handle);
            msg_count_mutex.unlock();
#endif
#ifdef WEAVER_NEW_CLDG
            msg_count_mutex.lock();
            agg_msg_count.erase(node_handle);
            msg_count_mutex.unlock();
#endif

            permanent_node_delete(n);
        } else {
            node_map_mutexes[map_idx].unlock();
        }
        n = NULL;
    }


    // Graph state update methods

    inline element::node*
    shard :: create_node(const node_handle_t &node_handle,
        vc::vclock &vclk,
        bool migrate,
        bool init_load=false)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
        element::node *new_node = new element::node(node_handle, vclk, node_map_mutexes+map_idx);
        new_node->last_upd_clk = vclk;
        new_node->restore_clk = vclk.clock;

        if (!init_load) {
            node_map_mutexes[map_idx].lock();
        }

        bool success = nodes[map_idx].emplace(node_handle, new_node).second;
        assert(success);
        UNUSED(success);

        if (!init_load) {
            node_map_mutexes[map_idx].unlock();
        }

        if (!init_load) {
            migration_mutex.lock();
        }
        node_list.emplace(node_handle);
        shard_node_count[shard_id - ShardIdIncr]++;
        if (!init_load) {
            migration_mutex.unlock();
        }

        if (!migrate) {
            new_node->state = element::node::mode::STABLE;
#ifdef WEAVER_CLDG
            new_node->msg_count.resize(get_num_shards(), 0);
#endif
            if (!init_load) {
                release_node(new_node);
            } else {
                new_node->in_use = false;
            }
        }
        return new_node;
    }

    inline void
    shard :: delete_node_nonlocking(element::node *n,
        vc::vclock &tdel)
    {
        n->base.update_del_time(tdel);
        n->updated = true;
    }

    inline void
    shard :: delete_node(const node_handle_t &node_handle,
        vc::vclock &tdel,
        uint64_t qts)
    {
        element::node *n = acquire_node_write(node_handle, tdel.vt_id, qts);
        if (n == NULL) {
            // node is being migrated
            migration_mutex.lock();
            deferred_writes[node_handle].emplace_back(deferred_write(transaction::NODE_DELETE_REQ, tdel));
            migration_mutex.unlock();
        } else {
            delete_node_nonlocking(n, tdel);
            release_node_write(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace_back(new del_obj(transaction::NODE_DELETE_REQ, tdel, node_handle));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge_nonlocking(element::node *n,
        const edge_handle_t &handle,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vc::vclock &vclk,
        bool init_load=false)
    {
        element::edge *new_edge = new element::edge(handle, vclk, remote_loc, remote_node);
        n->add_edge(new_edge);
        n->updated = true;

        // update edge map
        if (!init_load) {
            edge_map_mutex.lock();
        }
        edge_map[remote_node].emplace(n->get_handle());
        if (!init_load) {
            edge_map_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge(const edge_handle_t &handle,
        const node_handle_t &local_node,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vc::vclock &vclk,
        uint64_t qts)
    {
        element::node *n = acquire_node_write(local_node, vclk.vt_id, qts);
        if (n == NULL) {
            // node is being migrated
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[local_node];
            dwl.emplace_back(deferred_write(transaction::EDGE_CREATE_REQ, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge_handle = handle;
            dw.remote_node = remote_node;
            dw.remote_loc = remote_loc; 
            migration_mutex.unlock();
        } else {
            assert(n->get_handle() == local_node);
            create_edge_nonlocking(n, handle, remote_node, remote_loc, vclk);
            release_node_write(n);
        }
    }

    inline void
    shard :: delete_edge_nonlocking(element::node *n,
        const edge_handle_t &edge_handle,
        vc::vclock &tdel)
    {
        // already_exec check for fault tolerance
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        element::edge *e = out_edge_iter->second;
        e->base.update_del_time(tdel);
        n->updated = true;
        n->dependent_del++;

        // update edge map
        const node_handle_t &remote = e->nbr.handle;
        edge_map_mutex.lock();
        auto edge_map_iter = edge_map.find(remote);
        assert(edge_map_iter != edge_map.end());
        auto &node_set = edge_map_iter->second;
        node_set.erase(n->get_handle());
        if (node_set.empty()) {
            edge_map.erase(remote);
        }
        edge_map_mutex.unlock();
    }

    inline void
    shard :: delete_edge(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        vc::vclock &tdel,
        uint64_t qts)
    {
        element::node *n = acquire_node_write(node_handle, tdel.vt_id, qts);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_handle];
            dwl.emplace_back(deferred_write(transaction::EDGE_DELETE_REQ, tdel));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge_handle = edge_handle;
            migration_mutex.unlock();
        } else {
            delete_edge_nonlocking(n, edge_handle, tdel);
            release_node_write(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace_back(new del_obj(transaction::EDGE_DELETE_REQ, tdel, node_handle, edge_handle));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: set_node_property_nonlocking(element::node *n,
        std::string &key, std::string &value,
        vc::vclock &vclk)
    {
        n->base.add_property(key, value, vclk);
    }

    inline void
    shard :: set_node_property(const node_handle_t &node_handle,
        std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
        vc::vclock &vclk,
        uint64_t qts)
    {
        element::node *n = acquire_node_write(node_handle, vclk.vt_id, qts);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_handle];
            dwl.emplace_back(deferred_write(transaction::NODE_SET_PROPERTY, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.key = std::move(key);
            dw.value = std::move(value);
            migration_mutex.unlock();
        } else {
            set_node_property_nonlocking(n, *key, *value, vclk);
            release_node_write(n);
        }
    }

    inline void
    shard :: set_edge_property_nonlocking(element::node *n,
        const edge_handle_t &edge_handle,
        std::string &key, std::string &value,
        vc::vclock &vclk)
    {
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        element::edge *e = out_edge_iter->second;
        e->base.add_property(key, value, vclk);
    }

    inline void
    shard :: set_edge_property(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
        vc::vclock &vclk,
        uint64_t qts)
    {
        element::node *n = acquire_node_write(node_handle, vclk.vt_id, qts);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_handle];
            dwl.emplace_back(deferred_write(transaction::EDGE_SET_PROPERTY, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge_handle = edge_handle;
            dw.key = std::move(key);
            dw.value = std::move(value);
            migration_mutex.unlock();
        } else {
            set_edge_property_nonlocking(n, edge_handle, *key, *value, vclk);
            release_node_write(n);
        }
    }

    // return true if node already created
    inline bool
    shard :: node_exists_nonlocking(const node_handle_t &node_handle)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
        return (nodes[map_idx].find(node_handle) != nodes[map_idx].end());
    }

    // permanent deletion

    inline void
    shard :: delete_migrated_node(const node_handle_t &migr_node)
    {
        element::node *n;
        n = acquire_node(migr_node);
        n->permanently_deleted = true;
        // deleting edges now so as to prevent sending messages to neighbors for permanent edge deletion
        // rest of deletion happens in release_node()
        for (auto &e: n->out_edges) {
            delete e.second;
        }
        n->out_edges.clear();
        release_node(n);
    }

    bool
    perm_del_less(const del_obj* const o1, const del_obj* const o2)
    {
        const vc::vclock_t &clk1 = o1->vclk.clock;
        const vc::vclock_t &clk2 = o2->vclk.clock;
        assert(clk1.size() == ClkSz);
        assert(clk2.size() == ClkSz);
        for (uint64_t i = 0; i < ClkSz; i++) {
            if (clk1[i] < clk2[i]) {
                return true;
            }
        }
        return false;
    }

    inline void
    shard :: permanent_delete_loop(uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle)
    {
        element::node *n;
        del_obj *dobj = nullptr;

        perm_del_mutex.lock();

        std::sort(perm_del_queue.begin(), perm_del_queue.end(), perm_del_less);

        while (true) {
            bool to_del = !perm_del_queue.empty();
            if (to_del) {
                dobj = perm_del_queue.front(); // element with smallest vector clock
                if (!outstanding_progs) {
                    dobj->no_outstanding_progs[vt_id] = true;
                }
                // if all VTs have no outstanding node progs, then everything can be permanently deleted
                if (!weaver_util::all(dobj->no_outstanding_progs)) {
                    for (uint64_t i = 0; (i < NumVts) && to_del; i++) {
                        if (max_done_clk[i].size() < ClkSz) {
                            to_del = false;
                            break;
                        }
                        std::vector<vc::vclock_t*> compare(1, &max_done_clk[i]);
                        to_del = order::oracle::happens_before_no_kronos(dobj->vclk.clock, compare);
                    }
                }
            }
            if (!to_del) {
                break;
            }

            // now dobj will be deleted
            switch (dobj->type) {
                case transaction::NODE_DELETE_REQ:
                    n = acquire_node(dobj->node);
                    if (n != NULL) {
                        n->permanently_deleted = true;
                        for (auto &e: n->out_edges) {
                            const node_handle_t &node = e.second->nbr.handle;
                            auto edge_map_iter = edge_map.find(node);
                            assert(edge_map_iter != edge_map.end());
                            auto &node_set = edge_map_iter->second;
                            node_set.erase(dobj->node);
                            if (node_set.empty()) {
                                edge_map.erase(node);
                            }
                        }
                        release_node(n);
                    }
                    break;

                case transaction::EDGE_DELETE_REQ:
                    n = acquire_node(dobj->node);
                    if (n != NULL) {
                        auto out_edge_iter = n->out_edges.find(dobj->edge);
                        assert(out_edge_iter != n->out_edges.end());
                        if (n->last_perm_deletion == nullptr
                         || time_oracle->compare_two_vts(*n->last_perm_deletion,
                                out_edge_iter->second->base.get_del_time()) == 0) {
                            n->last_perm_deletion.reset(new vc::vclock(std::move(out_edge_iter->second->base.get_del_time())));
                        }
                        delete out_edge_iter->second;
                        n->out_edges.erase(dobj->edge);
                        release_node(n);
                    }
                    break;

                default:
                    WDEBUG << "invalid type " << dobj->type << " in deleted object" << std::endl;
            }

            // we know dobj will be initialized here
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
            assert(dobj != nullptr);
            delete dobj;
#pragma GCC diagnostic pop

            perm_del_queue.pop_front();
        }

        if (!outstanding_progs) {
            for (del_obj* const o: perm_del_queue) {
                o->no_outstanding_progs[vt_id] = true;
            }
        }

        perm_del_mutex.unlock();
    }

    inline void
    shard :: permanent_node_delete(element::node *n)
    {
        uint64_t num_shards = get_num_shards();
        message::message msg;
        assert(n->waiters == 0);
        assert(!n->in_use);
        // send msg to each shard to delete incoming edges
        // this happens lazily, and there could be dangling edges
        // users should explicitly delete edges before nodes if the program requires
        // this loop isn't executed in case of deletion of migrated nodes
        if (n->state != element::node::mode::MOVED) {
            for (uint64_t shard = ShardIdIncr; shard < ShardIdIncr + num_shards; shard++) {
                msg.prepare_message(message::PERMANENTLY_DELETED_NODE, n->get_handle());
                comm.send(shard, msg.buf);
            }
            for (auto &e: n->out_edges) {
                delete e.second;
            }
            n->out_edges.clear();
        }
        delete n;
    }


    // migration methods

    inline void
    shard :: update_migrated_nbr_nonlocking(element::node *n, const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc)
    {
        bool found = false;
        element::edge *e;
        for (auto &x: n->out_edges) {
            e = x.second;
            if (e->nbr.handle == migr_node && e->nbr.loc == old_loc) {
                e->nbr.loc = new_loc;
                found = true;
            }
        }
        assert(found);
    }

    inline void
    shard :: update_migrated_nbr(const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc)
    {
        std::unordered_set<node_handle_t> nbrs;
        element::node *n;
        edge_map_mutex.lock();
        nbrs = edge_map[migr_node];
        edge_map_mutex.unlock();
        for (const node_handle_t &nbr: nbrs) {
            n = acquire_node(nbr);
            update_migrated_nbr_nonlocking(n, migr_node, old_loc, new_loc);
            release_node(n);
        }
        migration_mutex.lock();
        if (old_loc != shard_id) {
            message::message msg;
            msg.prepare_message(message::MIGRATED_NBR_ACK, shard_id, max_prog_id,
                    shard_node_count[shard_id-ShardIdIncr]);
            comm.send(old_loc, msg.buf);
        } else {
            for (uint64_t i = 0; i < NumVts; i++) {
                if (target_prog_id[i] < max_prog_id[i]) {
                    target_prog_id[i] = max_prog_id[i];
                }
            }
            migr_edge_acks[shard_id - ShardIdIncr] = true;
        }
        migration_mutex.unlock();
    }

    inline void
    shard :: update_node_mapping(const node_handle_t &handle, uint64_t shard)
    {
        hstub.back()->update_mapping(handle, shard);
    }

    // node program

    inline void
    shard :: delete_prog_states(uint64_t req_id, std::vector<node_handle_t> &node_handles)
    {
        for (node_handle_t node_handle: node_handles) {
            db::element::node *node = acquire_node(node_handle);

            // check that node not migrated or permanently deleted
            if (node != NULL) {
                bool found = false;
                for (auto &state_map: node->prog_states) {
                    auto state_iter = state_map.find(req_id);
                    if (state_iter != state_map.end()) {
                        assert(state_map.erase(req_id) > 0);
                    }
                    found = true;
                    break;
                }
                assert(found);

                release_node(node);
            }
        }
    }

    inline void
    shard :: mark_nodes_using_state(uint64_t req_id, std::vector<node_handle_t> &node_handles)
    {
        node_prog_state_mutex.lock();
        bool done_request = (done_ids.find(req_id) != done_ids.end());
        if (!done_request) {
            auto state_list_iter = outstanding_prog_states.find(req_id);
            if (state_list_iter == outstanding_prog_states.end()) {
                outstanding_prog_states.emplace(req_id, std::move(node_handles));
            } else {
                std::vector<node_handle_t> &add_to = state_list_iter->second;
                add_to.reserve(add_to.size() + node_handles.size());
                add_to.insert(add_to.end(), node_handles.begin(), node_handles.end());
            }
            node_prog_state_mutex.unlock();
        } else { // request is finished, just delete things
            node_prog_state_mutex.unlock();
            delete_prog_states(req_id, node_handles);
        }
    }

    inline void
    shard :: add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests)
    {
        if (completed_requests.size() == 0) {
            return;
        }
        std::vector<uint64_t> completed_request_ids;
        for (auto &p: completed_requests) {
            uint64_t rid = p.first;
            completed_request_ids.push_back(rid);
        }

        std::vector<std::pair<uint64_t, std::vector<node_handle_t>>> to_delete;

        node_prog_state_mutex.lock();
        done_ids.insert(completed_request_ids.begin(), completed_request_ids.end());

        for (auto &p: completed_requests) {
            uint64_t req_id = p.first;
            auto node_list_iter = outstanding_prog_states.find(req_id);
            if (node_list_iter != outstanding_prog_states.end()) {
                to_delete.emplace_back(std::make_pair(req_id, std::move(node_list_iter->second)));
                int num_deleted = outstanding_prog_states.erase(req_id);
                assert(num_deleted == 1);
            }
        }
        node_prog_state_mutex.unlock();

        for (auto &p: to_delete) {
            delete_prog_states(p.first, p.second);
        }
    }

    inline bool
    shard :: check_done_request(uint64_t req_id)
    {
        node_prog_state_mutex.lock();
        bool done = (done_ids.find(req_id) != done_ids.end());
        node_prog_state_mutex.unlock();
        return done;
    }


    // Fault tolerance

    // restore state when backup becomes primary due to failure
    inline void
    shard :: restore_backup()
    {
        hstub.back()->restore_backup(nodes, edge_map, node_map_mutexes);
    }
}

#endif
