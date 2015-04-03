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
#include "common/cache_constants.h"
#include "common/config_constants.h"
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
#include "db/types.h"
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
        WEAVER,
        // xml based format for graphs. see http://graphml.graphdrawing.org/
        GRAPHML
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
            void node_wait_and_mark_busy(node*);
            node* acquire_node_latest(const node_handle_t &node_handle);
            node* acquire_node_specific(const node_handle_t &node_handle, const vc::vclock *tcreat, const vc::vclock *tdel);
            node* acquire_node_version(const node_handle_t &node_handle, const vc::vclock &vclk, order::oracle*);
            node* acquire_node_write(const node_handle_t &node, uint64_t vt_id, uint64_t qts);
            node* bulk_load_acquire_node_nonlocking(const node_handle_t &node_handle, uint64_t map_idx);
            void release_node_write(node *n);
            void release_node(node *n, bool migr_node);

            // Graph state
            //XXX po6::threads::mutex edge_map_mutex;
            po6::threads::mutex node_map_mutexes[NUM_NODE_MAPS];
            uint64_t shard_id;
            server_id serv_id;
            // node handle -> ptr to node object
            db::data_map<std::vector<node*>> nodes[NUM_NODE_MAPS];
            std::unordered_map<node_handle_t, // node handle n ->
                std::unordered_set<node_version_t, node_version_hash>> edge_map; // in-neighbors of n
        public:
            node* create_node(const node_handle_t &node_handle,
                vc::vclock &vclk,
                bool migrate);
            node* create_node_bulk_load(const node_handle_t &node_handle,
                uint64_t map_idx,
                vc::vclock &vclk);
            void delete_node_nonlocking(node *n,
                vc::vclock &tdel);
            void delete_node(const node_handle_t &node_handle,
                vc::vclock &vclk,
                uint64_t qts);
            void create_edge_nonlocking(node *n,
                const edge_handle_t &handle,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vc::vclock &vclk);
            void create_edge(const edge_handle_t &handle,
                const node_handle_t &local_node,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vc::vclock &vclk,
                uint64_t qts);
            void create_edge_bulk_load(node *n,
                const edge_handle_t &handle,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vc::vclock &vclk);
            void delete_edge_nonlocking(node *n,
                const edge_handle_t &edge_handle,
                vc::vclock &tdel);
            void delete_edge(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                vc::vclock &vclk,
                uint64_t qts);
            // properties
            void set_node_property_nonlocking(node *n,
                std::string &key, std::string &value,
                vc::vclock &vclk);
            void set_node_property(const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vc::vclock &vclk,
                uint64_t qts);
            void set_edge_property_nonlocking(node *n,
                const edge_handle_t &edge_handle,
                std::string &key, std::string &value,
                vc::vclock &vclk);
            void set_edge_property(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vc::vclock &vclk,
                uint64_t qts);
            void set_edge_property_bulk_load(node *n,
                const edge_handle_t &edge_handle,
                std::string &key, std::string &value,
                vc::vclock &vclk);
            void add_node_alias_nonlocking(node *n,
                node_handle_t &alias);
            void add_node_alias(const node_handle_t &node_handle,
                node_handle_t &alias,
                vc::vclock &vclk,
                uint64_t qts);
            uint64_t get_node_count();
            bool bulk_load_node_exists_nonlocking(const node_handle_t &node_handle, uint64_t map_idx);
            node_handle_t node_exists_cache[NUM_NODE_MAPS];

            // Initial graph loading
            po6::threads::mutex graph_load_mutex;
            uint64_t max_load_time, bulk_load_num_shards;
            uint32_t load_count;
            void bulk_load_persistent(int tid);

            // Permanent deletion
        public:
            po6::threads::mutex perm_del_mutex;
            std::deque<del_obj*> perm_del_queue;
            void delete_migrated_node(const node_handle_t &migr_node);
            // XXX void remove_from_edge_map(const node_handle_t &remote_node, const node_version_t &local_node);
            void permanent_delete_loop(uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle);
        private:
            void permanent_node_delete(node *n);

            // Migration
        public:
            po6::threads::mutex migration_mutex;
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
            void update_migrated_nbr_nonlocking(node *n, const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc);
            void update_migrated_nbr(const node_handle_t &node, uint64_t old_loc, uint64_t new_loc);
            void update_node_mapping(const node_handle_t &node, uint64_t shard);
            std::vector<vc::vclock_t> max_seen_clk // largest clock seen from each vector timestamper
                , target_prog_clk
                , max_done_clk; // largest clock of completed node prog for each VT
            //std::vector<vc::vclock_t> max_done_clk; // vclk of cumulative last node program completed
            std::vector<bool> migr_edge_acks;

            // node programs
        private:
            po6::threads::mutex node_prog_state_mutex;
            std::unordered_set<uint64_t> done_prog_ids; // request ids that have finished
            std::unordered_map<uint64_t, std::vector<node_version_t>> outstanding_prog_states; // maps request_id to list of nodes that have created prog state for that req id
            void clear_all_state(const std::unordered_map<uint64_t, std::vector<node_version_t>> &outstanding_prog_states);
            void delete_prog_states(uint64_t req_id, std::vector<node_version_t> &node_handles);
        public:
            void mark_nodes_using_state(uint64_t req_id, std::vector<node_version_t> &node_handles);
            void add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests);
            bool check_done_request(uint64_t req_id, vc::vclock &clk);

            std::unordered_map<std::tuple<cache_key_t, uint64_t, node_handle_t>, void *> node_prog_running_states; // used for fetching cache contexts
            po6::threads::mutex node_prog_running_states_mutex;

            po6::threads::mutex watch_set_lookups_mutex;
            uint64_t watch_set_lookups;
            uint64_t watch_set_nops;
            uint64_t watch_set_piggybacks;

            // fault tolerance
        public:
            std::vector<hyper_stub*> hstub;
            uint64_t min_prog_epoch; // protected by node_prog_state_mutex
            std::unordered_set<uint64_t> done_tx_ids;
            po6::threads::mutex done_tx_mtx;
            bool check_done_tx(uint64_t tx_id);
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
        , max_seen_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , target_prog_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , max_done_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , watch_set_lookups(0)
        , watch_set_nops(0)
        , watch_set_piggybacks(0)
    {
        for (uint64_t i = 0; i < NUM_NODE_MAPS; i++) {
            nodes[i].set_deleted_key("");
        }
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

#ifdef weaver_benchmark_
        // kill if a server died
        for (const server &srv: prev_config.delta(config)) {
            if (srv.type == server::VT || srv.type == server::SHARD) {
                server::state_t prev_state = prev_config.get_state(srv.id);
                if ((prev_state == server::AVAILABLE || prev_state == server::ASSIGNED)
                 && (srv.state != server::AVAILABLE && srv.state != server::ASSIGNED)) {
                    WDEBUG << server::to_string(srv.type) << " " << srv.virtual_id << " died, exiting now" << std::endl;
                    exit(-1);
                }
            }
        }
#endif
        // reset qts if a VTS died
        std::vector<server> delta = prev_config.delta(config);
        bool clear_queued = false;
        std::unordered_map<uint64_t, std::vector<node_version_t>> clear_map;
        for (const server &srv: delta) {
            if (srv.type == server::VT) {
                server::state_t prev_state = prev_config.get_state(srv.id);

                if ((prev_state == server::AVAILABLE || prev_state == server::ASSIGNED)
                 && (srv.state != server::AVAILABLE && srv.state != server::ASSIGNED)) {
                    uint64_t vt_id = srv.virtual_id;
                    // reset qts for vt_id
                    qm.reset(vt_id, config.version());
                    WDEBUG << "reset qts for vt " << vt_id << std::endl;

                    clear_queued = true;
                }
            } else if (srv.type == server::SHARD) {
                server::type_t prev_type = prev_config.get_type(srv.id);

                if (prev_type == server::BACKUP_SHARD) {
                    node_prog_state_mutex.lock();
                    min_prog_epoch = config.version();
                    clear_map = std::move(outstanding_prog_states);
                    node_prog_state_mutex.unlock();

                    clear_queued = true;
                }
            }
        }

        if (clear_queued) {
            // drop reads
            qm.clear_queued_reads();
            clear_all_state(clear_map);
        }
    }

    inline void
    shard :: bulk_load_persistent(int tid)
    {
        hstub[tid]->memory_efficient_bulk_load(tid, nodes);
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

    inline void
    shard :: node_wait_and_mark_busy(node *n)
    {
        n->waiters++;
        while (n->in_use) {
            n->cv.wait();
        }
        n->waiters--;
        n->in_use = true;
    }

    // nodes is map<handle, vector<node*>>
    // multiple nodes with same handle can exist because the node was deleted and then recreated
    // find the current node corresponding to given id
    // lock and return the node
    // return nullptr if node does not exist (possibly permanently deleted)
    inline node*
    shard :: acquire_node_latest(const node_handle_t &node_handle)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end() && !node_iter->second.empty()) {
            n = node_iter->second.back();
            node_wait_and_mark_busy(n);
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    // get node with handle node_handle that has either create time == tcreat or delete time == tdel
    inline node*
    shard :: acquire_node_specific(const node_handle_t &node_handle, const vc::vclock *tcreat, const vc::vclock *tdel)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            for (node *n_ver: node_iter->second) {
                node_wait_and_mark_busy(n_ver);

                if ((tcreat != nullptr && n_ver->base.get_creat_time() == *tcreat)
                 || (tdel != nullptr && n_ver->base.get_del_time() && *n_ver->base.get_del_time() == *tdel)) {
                    n = n_ver;
                    break;
                } else {
                    n_ver->in_use = false;
                    if (n_ver->waiters > 0) {
                        n_ver->cv.signal();
                    }
                }
            }
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    // get node that existed at time vclk, i.e. create time < vclk < delete time
    // strict inequality because vector clocks are unique.  no two clocks have exact same value
    inline node*
    shard :: acquire_node_version(const node_handle_t &node_handle, const vc::vclock &vclk, order::oracle *time_oracle)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            for (node *n_ver: node_iter->second) {
                node_wait_and_mark_busy(n_ver);

                if (time_oracle->clock_creat_before_del_after(vclk, n_ver->base.get_creat_time(), n_ver->base.get_del_time())) {
                    n = n_ver;
                    break;
                } else {
                    n_ver->in_use = false;
                    if (n_ver->waiters > 0) {
                        n_ver->cv.signal();
                    }
                }
            }
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    inline node*
    shard :: acquire_node_write(const node_handle_t &node_handle, uint64_t vt_id, uint64_t qts)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;

        node *n = nullptr;
        auto comp = std::make_pair(vt_id, qts);
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end() && !node_iter->second.empty()) {
            n = node_iter->second.back();
            n->waiters++;

            // first wait for node to become free
            while (n->in_use) {
                n->cv.wait();
            }

            // assert write exists in queue
            bool exists = false;
            for (auto &p: n->tx_queue) {
                if (p == comp) {
                    exists = true;
                    break;
                }
            }
            assert(exists);

            if (n->tx_queue.front() != comp) {
                // write exists in queue, but cannot be executed right now
                while (n->in_use || n->tx_queue.front() != comp) {
                    n->cv.wait();
                }
                n->tx_queue.pop_front();
            } else {
                // write exists in queue and is the first
                n->tx_queue.pop_front();
            }

            n->waiters--;
            n->in_use = true;
        }

        node_map_mutexes[map_idx].unlock();

        return n;
    }

    inline node*
    shard :: bulk_load_acquire_node_nonlocking(const node_handle_t &node_handle, uint64_t map_idx)
    {
        if (bulk_load_node_exists_nonlocking(node_handle, map_idx)) {
            return nodes[map_idx][node_handle][0];
        } else {
            return nullptr;
        }
        //node *n = nullptr;
        //auto node_iter = nodes[map_idx].find(node_handle);
        //if (node_iter != nodes[map_idx].end() && !node_iter->second.empty()) {
        //    n = node_iter->second.back();
        //}
        //return n;
    }

    inline void
    shard :: release_node_write(node *n)
    {
        release_node(n, false);
    }

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(node *n, bool migr_done=false)
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
            auto map_iter = nodes[map_idx].find(node_handle);
            assert(map_iter != nodes[map_idx].end());
            auto node_iter = map_iter->second.begin();
            for (; node_iter != map_iter->second.end(); node_iter++) {
                if (n == *node_iter) {
                    break;
                }
            }
            assert(node_iter != map_iter->second.end());
            map_iter->second.erase(node_iter);
            if (map_iter->second.empty()) {
                nodes[map_idx].erase(node_handle);
            }
            node_map_mutexes[map_idx].unlock();

            migration_mutex.lock();
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
        n = nullptr;
    }


    // Graph state update methods

    inline node*
    shard :: create_node(const node_handle_t &node_handle,
        vc::vclock &vclk,
        bool migrate)
    {
        uint64_t map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
        node *new_node = new node(node_handle, shard_id, vclk, node_map_mutexes+map_idx);

        node_map_mutexes[map_idx].lock();
        auto map_iter = nodes[map_idx].find(node_handle);
        if (map_iter == nodes[map_idx].end()) {
            nodes[map_idx][node_handle] = std::vector<node*>(1, new_node);
        } else {
            map_iter->second.emplace_back(new_node);
        }
        node_exists_cache[map_idx] = node_handle;
        node_map_mutexes[map_idx].unlock();

        migration_mutex.lock();
        shard_node_count[shard_id - ShardIdIncr]++;
        migration_mutex.unlock();

        if (!migrate) {
            new_node->state = node::mode::STABLE;
#ifdef WEAVER_CLDG
            new_node->msg_count.resize(get_num_shards(), 0);
#endif
            release_node(new_node);
        }
        return new_node;
    }

    inline node*
    shard :: create_node_bulk_load(const node_handle_t &node_handle,
        uint64_t map_idx,
        vc::vclock &vclk)
    {
        node *new_node = new node(node_handle, shard_id, vclk, node_map_mutexes+map_idx);

        nodes[map_idx][node_handle] = std::vector<node*>(1, new_node);
        node_exists_cache[map_idx] = node_handle;

        shard_node_count[shard_id - ShardIdIncr]++;

        new_node->state = node::mode::STABLE;
#ifdef WEAVER_CLDG
        new_node->msg_count.resize(get_num_shards(), 0);
#endif
        new_node->in_use = false;
        return new_node;
    }

    inline void
    shard :: delete_node_nonlocking(node *n,
        vc::vclock &tdel)
    {
        n->base.update_del_time(tdel);
    }

    inline void
    shard :: delete_node(const node_handle_t &node_handle,
        vc::vclock &tdel,
        uint64_t qts)
    {
        node *n = acquire_node_write(node_handle, tdel.vt_id, qts);
        if (n == nullptr) {
            // node is being migrated
            migration_mutex.lock();
            deferred_writes[node_handle].emplace_back(deferred_write(transaction::NODE_DELETE_REQ, tdel));
            migration_mutex.unlock();
        } else {
            delete_node_nonlocking(n, tdel);
            del_obj *dobj = new del_obj(transaction::NODE_DELETE_REQ, tdel, n->base.get_creat_time(), node_handle, "");
            release_node_write(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace_back(dobj);
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge_nonlocking(node *n,
        const edge_handle_t &handle,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vc::vclock &vclk)
    {
        edge *new_edge = new edge(handle, vclk, remote_loc, remote_node);
        n->add_edge(new_edge);

        // XXX update edge map
        //if (!init_load) {
        //    edge_map_mutex.lock();
        //}
        //edge_map[remote_node].emplace(std::make_pair(n->get_handle(), vclk));
        //if (!init_load) {
        //    edge_map_mutex.unlock();
        //}
    }

    inline void
    shard :: create_edge(const edge_handle_t &handle,
        const node_handle_t &local_node,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vc::vclock &vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(local_node, vclk.vt_id, qts);
        if (n == nullptr) {
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
    shard :: create_edge_bulk_load(node *n,
        const edge_handle_t &handle,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vc::vclock &vclk)
    {
        edge *new_edge = new edge(handle, vclk, remote_loc, remote_node);
        n->add_edge_unique(new_edge);
    }

    inline void
    shard :: delete_edge_nonlocking(node *n,
        const edge_handle_t &edge_handle,
        vc::vclock &tdel)
    {
        // already_exec check for fault tolerance
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        assert(!out_edge_iter->second.empty());
        edge *e = out_edge_iter->second.back();
        assert(!e->base.get_del_time());
        e->base.update_del_time(tdel);
    }

    inline void
    shard :: delete_edge(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        vc::vclock &tdel,
        uint64_t qts)
    {
        node *n = acquire_node_write(node_handle, tdel.vt_id, qts);
        if (n == nullptr) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_handle];
            dwl.emplace_back(deferred_write(transaction::EDGE_DELETE_REQ, tdel));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge_handle = edge_handle;
            migration_mutex.unlock();
        } else {
            delete_edge_nonlocking(n, edge_handle, tdel);
            del_obj *dobj = new del_obj(transaction::EDGE_DELETE_REQ, tdel, n->base.get_creat_time(), node_handle, edge_handle);
            release_node_write(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace_back(dobj);
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: set_node_property_nonlocking(node *n,
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
        node *n = acquire_node_write(node_handle, vclk.vt_id, qts);
        if (n == nullptr) {
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
    shard :: set_edge_property_nonlocking(node *n,
        const edge_handle_t &edge_handle,
        std::string &key, std::string &value,
        vc::vclock &vclk)
    {
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        assert(!out_edge_iter->second.empty());
        edge *e = out_edge_iter->second.back();
        assert(!e->base.get_del_time());
        e->base.add_property(key, value, vclk);
    }

    inline void
    shard :: set_edge_property(const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
        vc::vclock &vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(node_handle, vclk.vt_id, qts);
        if (n == nullptr) {
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

    inline void
    shard :: set_edge_property_bulk_load(node *n,
        const edge_handle_t &edge_handle,
        std::string &key, std::string &value,
        vc::vclock &vclk)
    {
        edge *e = n->out_edges[edge_handle].back();
        e->base.add_property(key, value, vclk);
    }

    void
    shard :: add_node_alias_nonlocking(node *n, node_handle_t &alias)
    {
        n->add_alias(alias);
    }

    void
    shard :: add_node_alias(const node_handle_t &node_handle, node_handle_t &alias,
        vc::vclock &vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(node_handle, vclk.vt_id, qts);
        if (n == nullptr) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_handle];
            dwl.emplace_back(deferred_write(transaction::ADD_AUX_INDEX, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.alias = alias;
            migration_mutex.unlock();
        } else {
            add_node_alias_nonlocking(n, alias);
            release_node_write(n);
        }
    }

    // return true if node already created
    inline bool
    shard :: bulk_load_node_exists_nonlocking(const node_handle_t &node_handle, uint64_t map_idx)
    {
        if (node_exists_cache[map_idx] == node_handle) {
            return true;
        }
        if (nodes[map_idx].find(node_handle) != nodes[map_idx].end()) {
            node_exists_cache[map_idx] = node_handle;
            return true;
        } else {
            return false;
        }
    }

    // permanent deletion

    inline void
    shard :: delete_migrated_node(const node_handle_t &migr_node)
    {
        node *n;
        n = acquire_node_latest(migr_node);
        n->permanently_deleted = true;
        // deleting edges now so as to prevent sending messages to neighbors for permanent edge deletion
        // rest of deletion happens in release_node()
        for (auto &x: n->out_edges) {
            for (db::edge *e: x.second) {
                delete e;
            }
        }
        n->out_edges.clear();
        release_node(n);
    }

    bool
    perm_del_less(const del_obj* const o1, const del_obj* const o2)
    {
        const vc::vclock_t &clk1 = o1->tdel.clock;
        const vc::vclock_t &clk2 = o2->tdel.clock;
        assert(clk1.size() == ClkSz);
        assert(clk2.size() == ClkSz);
        for (uint64_t i = 0; i < ClkSz; i++) {
            if (clk1[i] < clk2[i]) {
                return true;
            }
        }
        return false;
    }

    // assuming caller holds edge_map_mutex
    // XXX
    //inline void
    //shard :: remove_from_edge_map(const node_handle_t &remote_node, const node_version_t &local_node)
    //{
    //    auto edge_map_iter = edge_map.find(remote_node);
    //    assert(edge_map_iter != edge_map.end());
    //    auto &node_version_set = edge_map_iter->second;
    //    node_version_set.erase(local_node);
    //    if (node_version_set.empty()) {
    //        edge_map.erase(remote_node);
    //    }
    //}

    inline void
    shard :: permanent_delete_loop(uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle)
    {
        node *n;
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
                        to_del = order::oracle::happens_before_no_kronos(dobj->tdel.clock, max_done_clk[i]);
                    }
                }
            }
            if (!to_del) {
                break;
            }

            // now dobj will be deleted
            switch (dobj->type) {
                case transaction::NODE_DELETE_REQ:
                    n = acquire_node_specific(dobj->node, &dobj->version, nullptr);
                    if (n != nullptr) {
                        n->permanently_deleted = true;
                        for (auto &x: n->out_edges) {
                            for (db::edge *e: x.second) {
                                // XXX const node_handle_t &remote_node = e->nbr.handle;
                                node_version_t local_node = std::make_pair(dobj->node, e->base.get_creat_time());
                                // XXX remove_from_edge_map(remote_node, local_node);
                            }
                        }
                        release_node(n);
                    }
                    break;

                case transaction::EDGE_DELETE_REQ:
                    n = acquire_node_specific(dobj->node, &dobj->version, nullptr);
                    if (n != nullptr) {
                        auto map_iter = n->out_edges.find(dobj->edge);
                        assert(map_iter != n->out_edges.end());

                        edge *e = nullptr;
                        auto edge_iter = map_iter->second.begin();
                        for (; edge_iter != map_iter->second.end(); edge_iter++) {
                            const std::unique_ptr<vc::vclock> &tdel = (*edge_iter)->base.get_del_time();
                            if (tdel && *tdel == dobj->tdel) {
                                e = *edge_iter;
                                break;
                            }
                        }
                        assert(e);

                        if (n->last_perm_deletion == nullptr
                         || time_oracle->compare_two_vts(*n->last_perm_deletion, *e->base.get_del_time()) == 0) {
                            n->last_perm_deletion.reset(new vc::vclock(*e->base.get_del_time()));
                        }

                        delete e;
                        map_iter->second.erase(edge_iter);
                        if (map_iter->second.empty()) {
                            n->out_edges.erase(dobj->edge);
                        }
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
    shard :: permanent_node_delete(node *n)
    {
        message::message msg;
        assert(n->waiters == 0);
        assert(!n->in_use);
        // this code isn't executed in case of deletion of migrated nodes
        if (n->state != node::mode::MOVED) {
            for (auto &x: n->out_edges) {
                for (db::edge *e: x.second) {
                    delete e;
                }
            }
            n->out_edges.clear();
        }
        delete n;
    }


    // migration methods

    inline void
    shard :: update_migrated_nbr_nonlocking(node *n, const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc)
    {
        for (auto &x: n->out_edges) {
            for (db::edge *e: x.second) {
                if (e->nbr.handle == migr_node && e->nbr.loc == old_loc) {
                    e->nbr.loc = new_loc;
                }
            }
        }
    }

    inline void
    shard :: update_migrated_nbr(const node_handle_t &, uint64_t, uint64_t) { }
    //XXX shard :: update_migrated_nbr(const node_handle_t &migr_node, uint64_t old_loc, uint64_t new_loc)
    //{
    //    std::unordered_set<node_version_t, node_version_hash> nbrs;
    //    node *n;
    //    edge_map_mutex.lock();
    //    auto find_iter = edge_map.find(migr_node);
    //    if (find_iter != edge_map.end()) {
    //        nbrs = find_iter->second;
    //    }
    //    edge_map_mutex.unlock();

    //    for (const node_version_t &nv: nbrs) {
    //        n = acquire_node_specific(nv.first, &nv.second, nullptr);
    //        update_migrated_nbr_nonlocking(n, migr_node, old_loc, new_loc);
    //        release_node(n);
    //    }
    //    migration_mutex.lock();
    //    if (old_loc != shard_id) {
    //        message::message msg;
    //        msg.prepare_message(message::MIGRATED_NBR_ACK, shard_id, max_seen_clk, shard_node_count[shard_id-ShardIdIncr]);
    //        comm.send(old_loc, msg.buf);
    //    } else {
    //        for (uint64_t i = 0; i < NumVts; i++) {
    //            if (order::oracle::happens_before_no_kronos(target_prog_clk[i], max_seen_clk[i])) {
    //                target_prog_clk[i] = max_seen_clk[i];
    //            }
    //        }
    //        migr_edge_acks[shard_id - ShardIdIncr] = true;
    //    }
    //    migration_mutex.unlock();
    //}

    inline void
    shard :: update_node_mapping(const node_handle_t &handle, uint64_t shard)
    {
        hstub.back()->update_mapping(handle, shard);
    }

    // node program

    inline void
    shard :: clear_all_state(const std::unordered_map<uint64_t, std::vector<node_version_t>> &prog_states)
    {
        std::unordered_map<node_handle_t, std::vector<vc::vclock>> cleared;

        for (auto &p: prog_states) {
            for (const node_version_t &nv: p.second) {
                bool found = false;

                std::vector<vc::vclock> &clk_vec = cleared[nv.first];
                for (const vc::vclock &vclk: clk_vec) {
                    if (vclk == nv.second) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    node *n = acquire_node_specific(nv.first, &nv.second, nullptr);
                    n->prog_states.clear();
                    release_node(n);
                    clk_vec.emplace_back(nv.second);
                }
            }
        }
    }

    inline void
    shard :: delete_prog_states(uint64_t req_id, std::vector<node_version_t> &state_nodes)
    {
        for (const node_version_t &nv: state_nodes) {
            node *n = acquire_node_specific(nv.first, &nv.second, nullptr);

            // check that node not migrated or permanently deleted
            if (n != nullptr) {
                bool found = false;
                for (auto &state_pair: n->prog_states) {
                    auto &state_map = state_pair.second;
                    auto state_iter = state_map.find(req_id);
                    if (state_iter != state_map.end()) {
                        assert(state_map.erase(req_id) > 0);
                    }
                    found = true;
                    break;
                }
                assert(found);

                release_node(n);
            }
        }
    }

    inline void
    shard :: mark_nodes_using_state(uint64_t req_id, std::vector<node_version_t> &state_nodes)
    {
        node_prog_state_mutex.lock();
        bool done_request = (done_prog_ids.find(req_id) != done_prog_ids.end());
        if (!done_request) {
            auto state_list_iter = outstanding_prog_states.find(req_id);
            if (state_list_iter == outstanding_prog_states.end()) {
                outstanding_prog_states.emplace(req_id, std::move(state_nodes));
            } else {
                std::vector<node_version_t> &add_to = state_list_iter->second;
                add_to.reserve(add_to.size() + state_nodes.size());
                add_to.insert(add_to.end(), state_nodes.begin(), state_nodes.end());
            }
            node_prog_state_mutex.unlock();
        } else { // request is finished, just delete things
            node_prog_state_mutex.unlock();
            delete_prog_states(req_id, state_nodes);
        }
    }

    inline void
    shard :: add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests)
    {
        if (completed_requests.empty()) {
            return;
        }
        std::vector<uint64_t> completed_request_ids;
        for (auto &p: completed_requests) {
            completed_request_ids.push_back(p.first);
        }

        std::vector<std::pair<uint64_t, std::vector<node_version_t>>> to_delete;

        node_prog_state_mutex.lock();
        done_prog_ids.insert(completed_request_ids.begin(), completed_request_ids.end());

        for (auto &p: completed_requests) {
            uint64_t req_id = p.first;
            auto node_list_iter = outstanding_prog_states.find(req_id);
            if (node_list_iter != outstanding_prog_states.end()) {
                to_delete.emplace_back(std::make_pair(req_id, std::move(node_list_iter->second)));
                outstanding_prog_states.erase(req_id);
            }
        }
        node_prog_state_mutex.unlock();

        for (auto &p: to_delete) {
            delete_prog_states(p.first, p.second);
        }
    }

    inline bool
    shard :: check_done_request(uint64_t req_id, vc::vclock &clk)
    {
        node_prog_state_mutex.lock();
        bool done = (clk.get_epoch() < min_prog_epoch) || (done_prog_ids.find(req_id) != done_prog_ids.end());
        node_prog_state_mutex.unlock();
        return done;
    }


    // Fault tolerance

    inline bool
    shard :: check_done_tx(uint64_t tx_id)
    {
        bool found = false;

        done_tx_mtx.lock();
        if (done_tx_ids.find(tx_id) != done_tx_ids.end()) {
            found = true;
        } else {
            done_tx_ids.emplace(tx_id);
        }
        done_tx_mtx.unlock();

        return found;
    }

    // restore state when backup becomes primary due to failure
    inline void
    shard :: restore_backup()
    {
        hstub.back()->restore_backup(nodes, /*XXX edge_map,*/ node_map_mutexes);
    }
}

#endif
