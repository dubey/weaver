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
#include <sys/types.h>
#include <sys/sysinfo.h>
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
#include "db/utils.h"
#include "db/types.h"
#include "db/element.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/queue_manager.h"
#include "db/deferred_write.h"
#include "db/del_obj.h"
#include "db/node_entry.h"
#include "db/hyper_stub.h"
#include "db/async_nodeprog_state.h"

bool
available_memory()
{
    struct sysinfo mem_info;
    sysinfo(&mem_info);

    float total = mem_info.totalram;
    float avail = mem_info.freeram;
    float used  = total - avail;

    return (used/total < MaxMemory);
}

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
            shard(uint64_t serverid, std::shared_ptr<po6::net::location> loc);
            void init(uint64_t shardid);

            // Server manager
            po6::threads::mutex config_mutex, exit_mutex;
            server_manager_link_wrapper sm_stub;
            configuration config, prev_config;
            bool active_backup, first_config, pause_bb;
            po6::threads::cond backup_cond, first_config_cond;
            void reconfigure();
            void update_members_new_config();
            bool to_exit;

            // Messaging infrastructure
            common::comm_wrapper comm;

            // Consistency
            queue_manager qm;
            std::vector<order::oracle*> time_oracles;
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void record_completed_tx(vc::vclock &tx_clk);
            void node_wait_and_mark_busy(node*);
            void new_node_entry(uint64_t map_idx, std::shared_ptr<db::node_entry> new_entry);
            void post_node_recovery(bool recovery_success,
                                    db::node *n,
                                    uint64_t map_idx,
                                    std::shared_ptr<node_entry> entry);
            db::data_map<std::shared_ptr<node_entry>>::iterator node_present(uint64_t tid, uint64_t map_idx, const node_handle_t&);
            bool async_node_present(uint64_t tid, uint64_t map_idx, const node_handle_t&, db::data_map<std::shared_ptr<node_entry>>::iterator&);
            node* acquire_node_latest(uint64_t tid, const node_handle_t &node_handle);
            node* acquire_node_specific(uint64_t tid, const node_handle_t &node_handle, const vclock_ptr_t tcreat, const vclock_ptr_t tdel);
            node* acquire_node_version(uint64_t tid, const node_handle_t &node_handle, const vc::vclock &vclk, order::oracle*);
            node* acquire_node_write(uint64_t tid, const node_handle_t &node, uint64_t vt_id, uint64_t qts);
            node* acquire_node_nodeprog(uint64_t tid,
                                        const node_handle_t &node_handle,
                                        const vc::vclock &vclk,
                                        order::oracle*,
                                        node_prog::prog_type p_type,
                                        std::shared_ptr<void> prog_state,
                                        bool &recover);
            node* finish_acquire_node_nodeprog(db::data_map<std::shared_ptr<node_entry>>::iterator&,
                                               const vc::vclock &prog_clk,
                                               order::oracle *time_oracle);
            bool loop_recover_node(int tid, order::oracle*, async_nodeprog_state&);
            void save_evicted_node_state(node *n, uint64_t map_idx);
            void evict_all(uint64_t map_idx);
            void node_evict(node*, uint64_t map_idx);
            void choose_node_to_evict(uint64_t map_idx, std::shared_ptr<node_entry> cur_entry);
            void release_node_write(node *n);
            void release_node(node *n, bool migr_node);

            // Graph state
            //XXX po6::threads::mutex edge_map_mutex;
            po6::threads::mutex node_map_mutexes[NUM_NODE_MAPS];
            uint64_t shard_id;
            server_id serv_id;
            // node handle -> ptr to node object
            db::data_map<std::shared_ptr<node_entry>> nodes[NUM_NODE_MAPS];
            std::shared_ptr<node_entry> node_queue_clock_hand[NUM_NODE_MAPS];
            std::shared_ptr<node_entry> node_queue_last[NUM_NODE_MAPS];
            db::data_map<evicted_node_state> evicted_nodes_states[NUM_NODE_MAPS];
            uint32_t nodes_in_memory[NUM_NODE_MAPS];
            std::unordered_map<node_handle_t, // node handle n ->
                std::unordered_set<node_version_t, node_version_hash>> edge_map; // in-neighbors of n
            node* create_node(const node_handle_t &node_handle,
                vclock_ptr_t vclk,
                bool migrate);
            node* create_node_bulk_load(const node_handle_t &node_handle,
                uint64_t map_idx,
                vclock_ptr_t vclk);
            bool add_node_to_nodemap_bulk_load(node *n, uint64_t map_idx, uint64_t block_index);
            bool node_exists_bulk_load(const node_handle_t &node_handle);
            void delete_node_nonlocking(node *n,
                vclock_ptr_t tdel);
            void delete_node(uint64_t tid, const node_handle_t &node_handle,
                vclock_ptr_t vclk,
                uint64_t qts);
            void create_edge_nonlocking(node *n,
                const edge_handle_t &handle,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vclock_ptr_t vclk);
            void create_edge(uint64_t tid, const edge_handle_t &handle,
                const node_handle_t &local_node,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vclock_ptr_t vclk,
                uint64_t qts);
            edge* create_edge_bulk_load(const edge_handle_t &handle,
                const node_handle_t &remote_node, uint64_t remote_loc,
                vclock_ptr_t vclk);
            bool add_edge_to_node_bulk_load(edge *e, const node_handle_t &node, uint64_t map_idx);
            void delete_edge_nonlocking(node *n,
                const edge_handle_t &edge_handle,
                vclock_ptr_t tdel);
            void delete_edge(uint64_t tid, const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                vclock_ptr_t vclk,
                uint64_t qts);
            // properties
            void set_node_property_nonlocking(node *n,
                std::string &key, std::string &value,
                vclock_ptr_t vclk);
            void set_node_property(uint64_t tid, const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vclock_ptr_t vclk,
                uint64_t qts);
            void set_node_property_bulk_load(node *n,
                std::string &key, std::string &value,
                vclock_ptr_t vclk);
            void set_edge_property_nonlocking(node *n,
                const edge_handle_t &edge_handle,
                std::string &key, std::string &value,
                vclock_ptr_t vclk);
            void set_edge_property(uint64_t tid, const edge_handle_t &edge_handle, const node_handle_t &node_handle,
                std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
                vclock_ptr_t vclk,
                uint64_t qts);
            void set_edge_property_bulk_load(edge *e,
                std::string &key, std::string &value,
                vclock_ptr_t vclk);
            void add_node_alias_nonlocking(node *n,
                node_handle_t &alias);
            void add_node_alias(uint64_t tid, const node_handle_t &node_handle,
                node_handle_t &alias,
                vclock_ptr_t vclk,
                uint64_t qts);
            void add_node_alias_bulk_load(node *n,
                node_handle_t &alias);
            uint64_t get_node_count();
            bool bulk_load_node_exists(const node_handle_t &node_handle, uint64_t map_idx);

            // Initial graph loading
            po6::threads::mutex graph_load_mutex;
            uint64_t max_load_time, bulk_load_num_shards;
            uint32_t load_count;
            std::vector<uint64_t> graph_load_time;
            void bulk_load_persistent(hyper_stub &hs, bool call_hdex);
            void bulk_load_put_node(hyper_stub &hs,
                                    node *n,
                                    bool in_mem);
            void bulk_load_put_edge(hyper_stub &hs,
                                    edge *e,
                                    const node_handle_t &node_handle,
                                    uint64_t edge_id,
                                    bool node_in_mem);
            void bulk_load_flush_map(hyper_stub &hs);

            // Permanent deletion
        public:
            po6::threads::mutex perm_del_mutex;
            std::deque<del_obj*> perm_del_queue;
            std::vector<vc::vclock_t> permdel_done_clk;
            void delete_migrated_node(uint64_t tid, const node_handle_t &migr_node);
            // XXX void remove_from_edge_map(const node_handle_t &remote_node, const node_version_t &local_node);
            void permanent_delete_loop(uint64_t tid, uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle);
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
            void update_node_mapping(uint64_t tid, const node_handle_t &node, uint64_t shard);
            std::vector<vc::vclock_t> max_seen_clk // largest clock seen from each vector timestamper
                , target_prog_clk
                , migr_done_clk; // largest clock of completed node prog for each VT
            std::vector<bool> migr_edge_acks;

            // node programs
        private:
            po6::threads::mutex node_prog_state_mutex;
            using out_prog_map_t = std::unordered_map<uint64_t, std::pair<vc::vclock, std::vector<node_version_t>>>;
            out_prog_map_t outstanding_prog_states; // maps request_id to list of nodes that have created prog state for that req id
            std::vector<vc::vclock_t> prog_done_clk; // largest clock of cumulative completed node prog for each VT
            std::unordered_map<node_handle_t, async_nodeprog_state> async_get_prog_states[NUM_NODE_MAPS];
            std::unordered_map<uint64_t, std::pair<vc::vclock, uint64_t>> m_prog_node_recover_counts;
            void clear_all_state(uint64_t tid, const out_prog_map_t &outstanding_prog_states);
            void delete_prog_states(uint64_t tid, uint64_t req_id, std::vector<node_version_t> &node_handles);
        public:
            void record_node_recovery(uint64_t prog_id, const vc::vclock&);
            void mark_nodes_using_state(uint64_t req_id, const vc::vclock &clk, std::vector<node_version_t> &node_handles);
            void done_prog_clk(const vc::vclock_t *prog_clk, uint64_t vt_id);
            void done_permdel_clk(const vc::vclock_t *permdel_clk, uint64_t vt_id);
            std::unordered_map<uint64_t, uint64_t> cleanup_prog_states(uint64_t tid);
            bool check_done_prog(vc::vclock &clk);

            std::unordered_map<std::tuple<cache_key_t, uint64_t, node_handle_t>, void *> node_prog_running_states; // used for fetching cache contexts
            po6::threads::mutex node_prog_running_states_mutex;

            po6::threads::mutex watch_set_lookups_mutex;
            uint64_t watch_set_lookups;
            uint64_t watch_set_nops;
            uint64_t watch_set_piggybacks;

            // fault tolerance
        private:
            std::vector<hyper_stub*> hstub;
            uint64_t min_prog_epoch; // protected by node_prog_state_mutex
            std::unordered_set<uint64_t> done_tx_ids;
            po6::threads::mutex done_tx_mtx;
        public:
            int hstub_fd(uint64_t tid);
            bool check_done_tx(uint64_t tx_id);
            void cleanup_done_txs(const std::vector<uint64_t> &clean_txs);
            void restore_backup();

            // debug
            po6::threads::mutex nodeprog_msg_mtx;
            std::unordered_map<uint64_t, uint64_t> nodeprog_msg_counts;
    };

    inline
    shard :: shard(uint64_t serverid, std::shared_ptr<po6::net::location> loc)
        : sm_stub(server_id(serverid), loc)
        , active_backup(false)
        , first_config(false)
        , pause_bb(false)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , to_exit(false)
#ifdef weaver_async_node_recovery_
        , comm(loc, NUM_SHARD_THREADS, SHARD_MSGRECV_TIMEOUT, &sm_stub)
#else
        , comm(loc, NUM_SHARD_THREADS, -1, &sm_stub)
#endif
        , shard_id(UINT64_MAX)
        , serv_id(serverid)
        , max_load_time(0)
        , load_count(0)
        , permdel_done_clk(NumVts, vc::vclock_t(ClkSz, 0))
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
        , migr_done_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , prog_done_clk(NumVts, vc::vclock_t(ClkSz, 0))
        , watch_set_lookups(0)
        , watch_set_nops(0)
        , watch_set_piggybacks(0)
        , min_prog_epoch(0)
    {
        for (uint64_t i = 0; i < NUM_NODE_MAPS; i++) {
            nodes[i].set_deleted_key("");
            nodes_in_memory[i] = 0;
            evicted_nodes_states[i].set_deleted_key("");
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
            hstub.push_back(new hyper_stub(shard_id, -1));
            time_oracles.push_back(new order::oracle());
        }
        hstub.push_back(new hyper_stub(shard_id, -1)); // for server manager thread
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
        out_prog_map_t clear_map;
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
            clear_all_state(NUM_SHARD_THREADS, clear_map);
        }
    }

    inline void
    shard :: bulk_load_persistent(db::hyper_stub &hs, bool call_hdex)
    {
        if (call_hdex) {
            hs.loop_async_calls(true);
            hs.flush_all_edge_ids();
            hs.loop_async_calls(true);
        }
        hs.done_bulk_load();
    }

    inline void
    shard :: bulk_load_put_node(db::hyper_stub &hs,
                                db::node *n,
                                bool in_mem)
    {
        assert(hs.put_node_no_loop(n));

        if (!in_mem) {
            permanent_node_delete(n);
        }
    }

    inline void
    shard :: bulk_load_put_edge(db::hyper_stub &hs,
                                db::edge *e,
                                const node_handle_t &node_handle,
                                uint64_t edge_id,
                                bool node_in_mem)
    {
        assert(hs.put_edge_no_loop(node_handle, e, edge_id, !node_in_mem));
    }

    inline void
    shard :: bulk_load_flush_map(db::hyper_stub &hs)
    {
        hs.loop_async_calls(false);
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

    inline void
    shard :: new_node_entry(uint64_t map_idx, std::shared_ptr<db::node_entry> new_entry)
    {
        if (node_queue_last[map_idx]) {
            std::shared_ptr<db::node_entry> last = node_queue_last[map_idx];
            std::shared_ptr<db::node_entry> first = last->next;
            new_entry->next = first;
            new_entry->prev = last;
            first->prev = new_entry;
            last->next  = new_entry;
        } else {
            node_queue_clock_hand[map_idx] = new_entry;
            new_entry->next = new_entry;
            new_entry->prev = new_entry;
        }
        node_queue_last[map_idx] = new_entry;
    }

    inline void
    shard :: post_node_recovery(bool recovery_success,
                                db::node *n,
                                uint64_t map_idx,
                                std::shared_ptr<node_entry> entry)
    {
        if (recovery_success) {
            db::data_map<db::evicted_node_state> &node_state_map = evicted_nodes_states[map_idx];
            auto node_state_iter = node_state_map.find(n->get_handle());
            if (node_state_iter != node_state_map.end()) {
                evicted_node_state &s = node_state_iter->second;
                n->tx_queue = std::move(s.tx_queue);
                if (s.last_perm_deletion.vt_id != UINT64_MAX) {
                    n->last_perm_deletion.reset(new vc::vclock((s.last_perm_deletion)));
                }
                n->prog_states = std::move(s.prog_states);
                node_state_map.erase(n->get_handle());
            }

            entry->nodes.emplace_back(n);
            //if (++nodes_in_memory[map_idx] > NodesPerMap) {
            if (!available_memory()) {
                // evict a node when this node is released
                n->to_evict = true;
            }
            entry->present = true;

            new_node_entry(map_idx, entry);
        } else {
            permanent_node_delete(n);
        }
    }

    inline db::data_map<std::shared_ptr<node_entry>>::iterator
    shard :: node_present(uint64_t tid, uint64_t map_idx, const node_handle_t &node_handle)
    {
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            std::shared_ptr<node_entry> entry_ptr = node_iter->second;
            node_entry &entry = *entry_ptr;
            if (!entry.present) {
                // fetch node from HyperDex
                vclock_ptr_t dummy_clk;
                node *n = new node(node_handle, UINT64_MAX, dummy_clk, node_map_mutexes+map_idx);
                bool recovery_success = hstub[tid]->recover_node(*n);
                post_node_recovery(recovery_success,
                                   n,
                                   map_idx,
                                   entry_ptr);
            }

            if (entry.present) {
                entry.used = true;
            }
        }

        return node_iter;
    }

    // return value indicates if node found and in memory
    inline bool
    shard :: async_node_present(uint64_t tid,
                                uint64_t map_idx,
                                const node_handle_t &node_handle,
                                db::data_map<std::shared_ptr<node_entry>>::iterator &node_iter)
    {
        node_iter = nodes[map_idx].find(node_handle);
        if (node_iter != nodes[map_idx].end()) {
            node_entry &entry = *node_iter->second;
            if (!entry.present) {
                // fetch node from HyperDex
                vclock_ptr_t dummy_clk;
                node *n = new node(node_handle, UINT64_MAX, dummy_clk, node_map_mutexes+map_idx);
                hstub[tid]->get_node_no_loop(n);
                return false;
            } else {
                entry.used = true;
                return true;
            }
        } else {
            return false;
        }
    }

    // nodes is map<handle, vector<node*>>
    // multiple nodes with same handle can exist because the node was deleted and then recreated
    // find the current node corresponding to given id
    // lock and return the node
    // return nullptr if node does not exist (possibly permanently deleted)
    inline node*
    shard :: acquire_node_latest(uint64_t tid, const node_handle_t &node_handle)
    {
        uint64_t map_idx = get_map_idx(node_handle);

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = node_present(tid, map_idx, node_handle);
        if (node_iter != nodes[map_idx].end() && !node_iter->second->nodes.empty()) {
            n = node_iter->second->nodes.back();
            node_wait_and_mark_busy(n);
        }
        node_map_mutexes[map_idx].unlock();

        return n;
    }

    // get node with handle node_handle that has either create time == tcreat or delete time == tdel
    inline node*
    shard :: acquire_node_specific(uint64_t tid, const node_handle_t &node_handle, const vclock_ptr_t tcreat, const vclock_ptr_t tdel)
    {
        uint64_t map_idx = get_map_idx(node_handle);

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = node_present(tid, map_idx, node_handle);
        if (node_iter != nodes[map_idx].end()) {
            for (node *n_ver: node_iter->second->nodes) {
                node_wait_and_mark_busy(n_ver);

                if ((tcreat && *n_ver->base.get_creat_time() == *tcreat)
                 || (tdel && n_ver->base.get_del_time() && *n_ver->base.get_del_time() == *tdel)) {
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
    shard :: acquire_node_version(uint64_t tid, const node_handle_t &node_handle, const vc::vclock &vclk, order::oracle *time_oracle)
    {
        uint64_t map_idx = get_map_idx(node_handle);

        node *n = nullptr;
        node_map_mutexes[map_idx].lock();
        auto node_iter = node_present(tid, map_idx, node_handle);
        if (node_iter != nodes[map_idx].end()) {
            for (node *n_ver: node_iter->second->nodes) {
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
    shard :: acquire_node_write(uint64_t tid, const node_handle_t &node_handle, uint64_t vt_id, uint64_t qts)
    {
        uint64_t map_idx = get_map_idx(node_handle);

        node *n = nullptr;
        auto comp = std::make_pair(vt_id, qts);
        node_map_mutexes[map_idx].lock();
        auto node_iter = node_present(tid, map_idx, node_handle);
        if (node_iter != nodes[map_idx].end() && !node_iter->second->nodes.empty()) {
            n = node_iter->second->nodes.back();
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

    // get node that existed at time vclk, i.e. create time < vclk < delete time
    // strict inequality because vector clocks are unique.  no two clocks have exact same value
    // async get node for better performance
    inline node*
    shard :: acquire_node_nodeprog(uint64_t tid,
                                   const node_handle_t &node_handle,
                                   const vc::vclock &vclk,
                                   order::oracle *time_oracle,
                                   node_prog::prog_type p_type,
                                   std::shared_ptr<void> prog_state,
                                   bool &recover)
    {
        uint64_t map_idx = get_map_idx(node_handle);
        node *n = nullptr;
        recover = false;

        node_map_mutexes[map_idx].lock();

        auto node_iter = nodes[map_idx].end();
#ifdef weaver_async_node_recovery_
        bool found = async_node_present(tid, map_idx, node_handle, node_iter);
#else
        node_iter = node_present(tid, map_idx, node_handle);
        bool found = (node_iter != nodes[map_idx].end());
#endif
        bool node_exists = (node_iter != nodes[map_idx].end());

        if (found) {
            n = finish_acquire_node_nodeprog(node_iter, vclk, time_oracle);
        } else if (node_exists) {
            // node exists but currently not in memory
            // save prog state until node is recovered from HyperDex
            async_nodeprog_state delayed_prog;
            delayed_prog.type  = p_type;
            delayed_prog.clk   = vclk;
            delayed_prog.state = prog_state;
            async_get_prog_states[map_idx][node_handle] = delayed_prog;
            recover = true;
        }

        node_map_mutexes[map_idx].unlock();

        return n;
    }

    inline node*
    shard :: finish_acquire_node_nodeprog(db::data_map<std::shared_ptr<node_entry>>::iterator &node_iter,
                                          const vc::vclock &prog_clk,
                                          order::oracle *time_oracle)
    {
        db::node *n = nullptr;
        for (node *n_ver: node_iter->second->nodes) {
            node_wait_and_mark_busy(n_ver);

            if (time_oracle->clock_creat_before_del_after(prog_clk, n_ver->base.get_creat_time(), n_ver->base.get_del_time())) {
                n = n_ver;
                break;
            } else {
                n_ver->in_use = false;
                if (n_ver->waiters > 0) {
                    n_ver->cv.signal();
                }
            }
        }

        return n;
    }

    inline bool
    shard :: loop_recover_node(int tid,
                               order::oracle *time_oracle,
                               async_nodeprog_state &delayed_prog)
    {
        db::node *n = nullptr;
        bool success = hstub[tid]->loop_get_node(&n);

        if (n == nullptr) {
            // still haven't recovered node entirely
            return false;
        }

        if (!success) {
            // error in loop call
           return false;
        }

        uint64_t map_idx = get_map_idx(n->get_handle());
        node_map_mutexes[map_idx].lock();

        auto node_iter = nodes[map_idx].find(n->get_handle());
        assert(node_iter != nodes[map_idx].end());

        post_node_recovery(success,
                           n,
                           map_idx,
                           node_iter->second);

        node_entry &entry = *node_iter->second;
        entry.used = true;

        auto progstate_map  = async_get_prog_states[map_idx];
        auto progstate_iter = progstate_map.find(n->get_handle());
        assert(progstate_iter != progstate_map.end());
        delayed_prog = progstate_iter->second;
        n = finish_acquire_node_nodeprog(node_iter,
                                         delayed_prog.clk,
                                         time_oracle);

        node_map_mutexes[map_idx].unlock();

        delayed_prog.n = n;

        return true;
    }

    inline void
    shard :: release_node_write(node *n)
    {
        release_node(n, false);
    }

    inline void
    shard :: permanent_node_delete(node *n)
    {
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

    inline void
    shard :: save_evicted_node_state(node *n, uint64_t map_idx)
    {
        if (!n->empty_evicted_node_state()) {
            db::data_map<db::evicted_node_state> &node_state_map = evicted_nodes_states[map_idx];
            assert(node_state_map.find(n->get_handle()) == node_state_map.end());
            evicted_node_state &s = node_state_map[n->get_handle()];
            s.tx_queue = std::move(n->tx_queue);
            if (n->last_perm_deletion) {
                s.last_perm_deletion = *n->last_perm_deletion;
            }
            s.prog_states = std::move(n->prog_states);
        }
    }

    // evict all nodes in nodes[map_idx]
    // while bulk loading when a map is flushed to persistent storage
    inline void
    shard :: evict_all(uint64_t map_idx)
    {
        for (auto &p: nodes[map_idx]) {
            node_entry &entry = *p.second;
            if (entry.present) {
                permanent_node_delete(entry.nodes.front());
                entry.nodes.clear();
                entry.present = false;
                entry.next = nullptr;
                entry.prev = nullptr;
            }
        }
        node_queue_clock_hand[map_idx] = nullptr;
        node_queue_last[map_idx] = nullptr;
    }

    inline void
    shard :: node_evict(node *n, uint64_t map_idx)
    {
        const node_handle_t &node_handle = n->get_handle();
        //WDEBUG << "evicting node " << node_handle << std::endl;
        auto map_iter = nodes[map_idx].find(node_handle);
        assert(map_iter != nodes[map_idx].end());

        auto entry_ptr = map_iter->second;
        assert(entry_ptr->prev != entry_ptr); // should never evict last node, NodesPerMap >= 1

        if (entry_ptr == node_queue_clock_hand[map_idx]) {
            node_queue_clock_hand[map_idx] = entry_ptr->next;
        }
        if (entry_ptr == node_queue_last[map_idx]) {
            node_queue_last[map_idx] = entry_ptr->prev;
        }

        entry_ptr->prev->next = entry_ptr->next;
        entry_ptr->next->prev = entry_ptr->prev;
        entry_ptr->prev = nullptr;
        entry_ptr->next = nullptr;

        node_entry &entry = *entry_ptr;
        auto node_iter = entry.nodes.begin();
        for (; node_iter != entry.nodes.end(); node_iter++) {
            if (n == *node_iter) {
                break;
            }
        }
        assert(node_iter != entry.nodes.end());
        entry.nodes.erase(node_iter);
        entry.present = false;

        save_evicted_node_state(n, map_idx);
        permanent_node_delete(n);
    }

    inline void
    shard :: choose_node_to_evict(uint64_t map_idx, std::shared_ptr<db::node_entry> cur_entry)
    {
        while (true) {
            auto entry = node_queue_clock_hand[map_idx];
            node_queue_clock_hand[map_idx] = node_queue_clock_hand[map_idx]->next;

            if (entry == node_queue_clock_hand[map_idx]) {
                // single node in this node_map
                break;
            }

            if (entry != cur_entry && entry->present) {
                if (entry->used) {
                    entry->used = false;
                } else {
                    node *evict_node = entry->nodes.back();

                    evict_node->waiters++;
                    while (evict_node->in_use) {
                        evict_node->cv.wait();
                    }
                    evict_node->waiters--;

                    if (!evict_node->evicted && !evict_node->permanently_deleted) {
                        --nodes_in_memory[map_idx]; // eventually this node will be evicted, proactively reduce count to prevent multiple evictions
                        if (evict_node->waiters > 0) {
                            evict_node->evicted = true;
                            evict_node->cv.signal();
                        } else {
                            node_evict(evict_node, map_idx);
                        }
                        break;
                    }
                }
            }
        }
    }

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(node *n, bool migr_done=false)
    {
        uint64_t map_idx = get_map_idx(n->get_handle());

        node_map_mutexes[map_idx].lock();
        n->in_use = false;

        if (migr_done) {
            n->migr_cv.broadcast();
        }

        if (n->to_evict && !n->permanently_deleted) {
            n->to_evict = false;
            choose_node_to_evict(map_idx, nodes[map_idx][n->get_handle()]);
        }

        if (n->waiters > 0) {
            n->cv.signal();
            node_map_mutexes[map_idx].unlock();
        } else if (n->permanently_deleted) {
            const node_handle_t &node_handle = n->get_handle();
            auto map_iter = nodes[map_idx].find(node_handle);
            assert(map_iter != nodes[map_idx].end());
            node_entry &entry = *map_iter->second;
            auto node_iter = entry.nodes.begin();
            for (; node_iter != entry.nodes.end(); node_iter++) {
                if (n == *node_iter) {
                    break;
                }
            }
            assert(node_iter != entry.nodes.end());
            entry.nodes.erase(node_iter);
            --nodes_in_memory[map_idx];
            if (entry.nodes.empty()) {
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
        } else if (n->evicted) {
            node_evict(n, map_idx);
            node_map_mutexes[map_idx].unlock();
        } else {
            node_map_mutexes[map_idx].unlock();
        }
        n = nullptr;
    }


    // Graph state update methods

    inline node*
    shard :: create_node(const node_handle_t &node_handle,
        vclock_ptr_t vclk,
        bool migrate)
    {
        uint64_t map_idx = get_map_idx(node_handle);
        node *new_node = new node(node_handle, shard_id, vclk, node_map_mutexes+map_idx);

        node_map_mutexes[map_idx].lock();
        auto map_iter = nodes[map_idx].find(node_handle);
        if (map_iter == nodes[map_idx].end()) {
            auto new_entry = std::make_shared<node_entry>(new_node);
            nodes[map_idx][node_handle] = new_entry;
            new_node_entry(map_idx, new_entry);
        } else {
            map_iter->second->nodes.emplace_back(new_node);
            map_iter->second->used = true;
        }
        //if (++nodes_in_memory[map_idx] > NodesPerMap) {
        if (!available_memory()) {
            new_node->to_evict = true;
        }
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
                                   vclock_ptr_t vclk)
    {
        node *new_node = new node(node_handle, shard_id, vclk, node_map_mutexes+map_idx);
        new_node->state = node::mode::STABLE;
#ifdef WEAVER_CLDG
        new_node->msg_count.resize(get_num_shards(), 0);
#endif
        new_node->in_use = false;

        return new_node;
    }

    inline bool
    shard :: add_node_to_nodemap_bulk_load(node *n,
                                           uint64_t map_idx,
                                           uint64_t block_index)
    {
        bool in_mem;
        node_handle_t nh = n->get_handle();

        node_map_mutexes[map_idx].lock();
        if (nodes[map_idx].find(n->get_handle()) != nodes[map_idx].end()) {
            std::string err_msg = "repeated node handle ";
            err_msg += n->get_handle();
            err_msg += " in bulk loading process, aborting now.";
            assert(false && err_msg.c_str());
        }
        std::shared_ptr<node_entry> new_entry;
        if (nodes_in_memory[map_idx] < NodesPerMap) {
        //if (available_memory()) {
        //if (block_index >= 199999 && block_index <= 200101) {
            new_entry = std::make_shared<node_entry>(n);
            new_node_entry(map_idx, new_entry);
            new_entry->used = true;
            ++nodes_in_memory[map_idx];
            in_mem = true;
        } else {
            new_entry = std::make_shared<node_entry>();
            in_mem = false;
        }
        nodes[map_idx][n->get_handle()] = new_entry;
        node_map_mutexes[map_idx].unlock();

        return in_mem;
    }

    inline bool
    shard :: node_exists_bulk_load(const node_handle_t &handle)
    {
        bool exists;
        uint64_t map_idx = get_map_idx(handle);

        node_map_mutexes[map_idx].lock();
        if (nodes[map_idx].find(handle) == nodes[map_idx].end()) {
            exists = false;
        } else {
            exists = true;
        }
        node_map_mutexes[map_idx].unlock();

        return exists;
    }

    inline void
    shard :: delete_node_nonlocking(node *n,
        vclock_ptr_t tdel)
    {
        n->base.update_del_time(tdel);
    }

    inline void
    shard :: delete_node(uint64_t tid, const node_handle_t &node_handle,
        vclock_ptr_t tdel,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, node_handle, tdel->vt_id, qts);
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
        vclock_ptr_t vclk)
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
    shard :: create_edge(uint64_t tid, const edge_handle_t &handle,
        const node_handle_t &local_node,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vclock_ptr_t vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, local_node, vclk->vt_id, qts);
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

    inline edge*
    shard :: create_edge_bulk_load(const edge_handle_t &handle,
        const node_handle_t &remote_node, uint64_t remote_loc,
        vclock_ptr_t vclk)
    {
        edge *new_edge = new edge(handle, vclk, remote_loc, remote_node);
        return new_edge;
    }

    inline bool
    shard :: add_edge_to_node_bulk_load(edge *e, const node_handle_t &node_handle, uint64_t map_idx)
    {
        bool found = false;

        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_handle);
        if (node_iter == nodes[map_idx].end()) {
            WDEBUG << "did not find node=" << node_handle
                   << " while adding edge=" << e->get_handle() << std::endl;
            assert(false);
        }

        std::shared_ptr<node_entry> entry = node_iter->second;
        if (entry->present) {
            node *n = entry->nodes.front();
            n->add_edge_unique(e);
            found = true;
        }
        node_map_mutexes[map_idx].unlock();

        return found;
    }

    inline void
    shard :: delete_edge_nonlocking(node *n,
        const edge_handle_t &edge_handle,
        vclock_ptr_t tdel)
    {
        // already_exec check for fault tolerance
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        assert(!out_edge_iter->second.empty());
        edge *e = out_edge_iter->second.back();
        // XXX nodeswap
        assert(!e->base.get_del_time());
        e->base.update_del_time(tdel);
    }

    inline void
    shard :: delete_edge(uint64_t tid, const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        vclock_ptr_t tdel,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, node_handle, tdel->vt_id, qts);
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
        vclock_ptr_t vclk)
    {
        n->base.set_property(key, value, vclk);
    }

    inline void
    shard :: set_node_property(uint64_t tid, const node_handle_t &node_handle,
        std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
        vclock_ptr_t vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, node_handle, vclk->vt_id, qts);
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
    shard :: set_node_property_bulk_load(node *n,
        std::string &key, std::string &value,
        vclock_ptr_t vclk)
    {
        n->base.add_property(key, value, vclk);
    }

    inline void
    shard :: set_edge_property_nonlocking(node *n,
        const edge_handle_t &edge_handle,
        std::string &key, std::string &value,
        vclock_ptr_t vclk)
    {
        auto out_edge_iter = n->out_edges.find(edge_handle);
        assert(out_edge_iter != n->out_edges.end());
        assert(!out_edge_iter->second.empty());
        edge *e = out_edge_iter->second.back();
        assert(!e->base.get_del_time());
        e->base.set_property(key, value, vclk);
    }

    inline void
    shard :: set_edge_property(uint64_t tid, const edge_handle_t &edge_handle, const node_handle_t &node_handle,
        std::unique_ptr<std::string> key, std::unique_ptr<std::string> value,
        vclock_ptr_t vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, node_handle, vclk->vt_id, qts);
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
    shard :: set_edge_property_bulk_load(edge *e,
        std::string &key, std::string &value,
        vclock_ptr_t vclk)
    {
        e->base.add_property(key, value, vclk);
    }

    void
    shard :: add_node_alias_nonlocking(node *n, node_handle_t &alias)
    {
        n->add_alias(alias);
    }

    void
    shard :: add_node_alias(uint64_t tid, const node_handle_t &node_handle, node_handle_t &alias,
        vclock_ptr_t vclk,
        uint64_t qts)
    {
        node *n = acquire_node_write(tid, node_handle, vclk->vt_id, qts);
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

    void
    shard :: add_node_alias_bulk_load(node *n,
        node_handle_t &alias)
    {
        n->add_alias(alias);
    }

    // return true if node already created
    inline bool
    shard :: bulk_load_node_exists(const node_handle_t &node_handle, uint64_t map_idx)
    {
        if (nodes[map_idx].find(node_handle) != nodes[map_idx].end()) {
            return true;
        } else {
            return false;
        }
    }

    // permanent deletion

    inline void
    shard :: delete_migrated_node(uint64_t tid, const node_handle_t &migr_node)
    {
        node *n;
        n = acquire_node_latest(tid, migr_node);
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
        const vc::vclock_t &clk1 = o1->tdel->clock;
        const vc::vclock_t &clk2 = o2->tdel->clock;
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
    shard :: permanent_delete_loop(uint64_t tid, uint64_t vt_id, bool outstanding_progs, order::oracle *time_oracle)
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
                        if (permdel_done_clk[i].size() < ClkSz) {
                            to_del = false;
                            break;
                        }
                        to_del = order::oracle::happens_before_no_kronos(dobj->tdel->clock, permdel_done_clk[i]);
                    }
                }
            }
            if (!to_del) {
                break;
            }

            // now dobj will be deleted
            switch (dobj->type) {
                case transaction::NODE_DELETE_REQ:
                    n = acquire_node_specific(tid, dobj->node, dobj->version, nullptr);
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
                    n = acquire_node_specific(tid, dobj->node, dobj->version, nullptr);
                    if (n != nullptr) {
                        auto map_iter = n->out_edges.find(dobj->edge);
                        assert(map_iter != n->out_edges.end());

                        edge *e = nullptr;
                        auto edge_iter = map_iter->second.begin();
                        for (; edge_iter != map_iter->second.end(); edge_iter++) {
                            vclock_ptr_t tdel = (*edge_iter)->base.get_del_time();
                            if (tdel && *tdel == *dobj->tdel) {
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
    shard :: update_node_mapping(uint64_t tid, const node_handle_t &handle, uint64_t shard)
    {
        hstub[tid]->update_mapping(handle, shard);
    }

    // node program

    inline void
    shard :: clear_all_state(uint64_t tid, const out_prog_map_t &prog_states)
    {
        std::unordered_map<node_handle_t, std::vector<vclock_ptr_t>> cleared;

        for (auto &p: prog_states) {
            for (const node_version_t &nv: p.second.second) {
                bool found = false;

                std::vector<vclock_ptr_t> &clk_vec = cleared[nv.first];
                for (vclock_ptr_t vclk: clk_vec) {
                    if (*vclk == *nv.second) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    node *n = acquire_node_specific(tid, nv.first, nv.second, nullptr);
                    n->prog_states.clear();
                    release_node(n);
                    clk_vec.emplace_back(nv.second);
                }
            }
        }
    }

    inline void
    shard :: delete_prog_states(uint64_t tid, uint64_t req_id, std::vector<node_version_t> &state_nodes)
    {
        for (const node_version_t &nv: state_nodes) {
            node *n = acquire_node_specific(tid, nv.first, nv.second, nullptr);

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
    shard :: record_node_recovery(uint64_t prog_id, const vc::vclock &vclk)
    {
        node_prog_state_mutex.lock();
        auto iter = m_prog_node_recover_counts.find(prog_id);
        if (iter == m_prog_node_recover_counts.end()) {
            m_prog_node_recover_counts.emplace(prog_id, std::make_pair(vclk, 1));
        } else {
            iter->second.second++;
        }
        node_prog_state_mutex.unlock();
    }

    inline void
    shard :: mark_nodes_using_state(uint64_t req_id, const vc::vclock &clk, std::vector<node_version_t> &state_nodes)
    {
        node_prog_state_mutex.lock();
        auto state_list_iter = outstanding_prog_states.find(req_id);
        if (state_list_iter == outstanding_prog_states.end()) {
            outstanding_prog_states.emplace(req_id, std::make_pair(clk, std::move(state_nodes)));
        } else {
            std::vector<node_version_t> &add_to = state_list_iter->second.second;
            add_to.reserve(add_to.size() + state_nodes.size());
            add_to.insert(add_to.end(), state_nodes.begin(), state_nodes.end());
        }
        node_prog_state_mutex.unlock();
    }

    inline void
    shard :: done_prog_clk(const vc::vclock_t *prog_clk, uint64_t vt_id)
    {
        if (prog_clk == nullptr) {
            return;
        }

        node_prog_state_mutex.lock();

        if (order::oracle::happens_before_no_kronos(prog_done_clk[vt_id], *prog_clk)) {
            prog_done_clk[vt_id] = *prog_clk;
        }

        node_prog_state_mutex.unlock();
    }

    inline void
    shard :: done_permdel_clk(const vc::vclock_t *permdel_clk, uint64_t vt_id)
    {
        if (permdel_clk == nullptr) {
            return;
        }

        perm_del_mutex.lock();

        if (order::oracle::happens_before_no_kronos(permdel_done_clk[vt_id], *permdel_clk)) {
            permdel_done_clk[vt_id] = *permdel_clk;
        }

        perm_del_mutex.unlock();
    }

    inline std::unordered_map<uint64_t, uint64_t>
    shard :: cleanup_prog_states(uint64_t tid)
    {
        std::vector<std::pair<uint64_t, std::vector<node_version_t>>> to_delete;

        node_prog_state_mutex.lock();
        for (const auto &p: outstanding_prog_states) {
            const vc::vclock &clk = p.second.first;
            if (order::oracle::happens_before_no_kronos(clk.clock, prog_done_clk[clk.vt_id])) {
                to_delete.emplace_back(std::make_pair(p.first, std::move(p.second.second)));
            }
        }

        for (const auto &p: to_delete) {
            outstanding_prog_states.erase(p.first);
        }

        std::unordered_map<uint64_t, uint64_t> node_recover_counts;
        std::vector<uint64_t> del_node_counts;
        for (auto &p: m_prog_node_recover_counts) {
            const vc::vclock &clk = p.second.first;
            if (order::oracle::happens_before_no_kronos(clk.clock, prog_done_clk[clk.vt_id])) {
                node_recover_counts.emplace(p.first, p.second.second);
                del_node_counts.emplace_back(p.first);
            }
        }

        for (uint64_t d: del_node_counts) {
            m_prog_node_recover_counts.erase(d);
        }

        node_prog_state_mutex.unlock();

        for (auto &p: to_delete) {
            delete_prog_states(tid, p.first, p.second);
        }

        return node_recover_counts;
    }

    inline bool
    shard :: check_done_prog(vc::vclock &clk)
    {
        node_prog_state_mutex.lock();
        bool done = (clk.get_epoch() < min_prog_epoch)
                 || (order::oracle::happens_before_no_kronos(clk.clock, prog_done_clk[clk.vt_id]));
        node_prog_state_mutex.unlock();
        return done;
    }


    // Fault tolerance

    inline int
    shard :: hstub_fd(uint64_t tid)
    {
        return hstub[tid]->fd();
    }

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

    inline void
    shard :: cleanup_done_txs(const std::vector<uint64_t> &clean_txs)
    {
        if (clean_txs.empty()) {
            return;
        }

        done_tx_mtx.lock();
        for (uint64_t id: clean_txs) {
            done_tx_ids.erase(id);
        }
        done_tx_mtx.unlock();
    }

    // restore state when backup becomes primary due to failure
    inline void
    shard :: restore_backup()
    {
        hstub.back()->restore_backup(nodes, /*XXX edge_map,*/ node_map_mutexes);
    }
}

#endif
