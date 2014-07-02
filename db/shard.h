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
#include "common/nmap_stub.h"
#include "common/comm_wrapper.h"
#include "common/event_order.h"
#include "common/configuration.h"
#include "common/server_manager_link_wrapper.h"
#include "common/bool_vector.h"
#include "db/shard_constants.h"
#include "db/element.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/queue_manager.h"
#include "db/deferred_write.h"
#include "db/del_obj.h"
#include "db/hyper_stub.h"

namespace std {
    // so we can use a pair as key to unordered_map
    template <typename T1, typename T2, typename T3>
        struct hash<std::tuple<T1, T2, T3>>
        {
            size_t operator()(const std::tuple<T1, T2, T3>& k) const
            {
                size_t hash = std::hash<uint64_t>()(std::get<0>(k));
                hash ^= std::hash<uint64_t>()(std::get<1>(k)) + 0x9e3779b9 + (hash<<6) + (hash>>2);
                hash ^= std::hash<uint64_t>()(std::get<2>(k)) + 0x9e3779b9 + (hash<<6) + (hash>>2);
                return hash;
            }
        };
    // so we can use a pair as key to unordered_map
    template <typename T1, typename T2>
        struct hash<std::pair<T1, T2>>
        {
            size_t operator()(const std::pair<T1, T2>& k) const
            {
                size_t hash = std::hash<uint64_t>()(k.first);
                hash ^= std::hash<uint64_t>()(k.second) + 0x9e3779b9 + (hash<<6) + (hash>>2);
                return hash;
            }
        };
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
        WEAVER
    };

    // graph partition state and associated data structures
    class shard
    {
        public:
            //shard(uint64_t shard, uint64_t server);
            shard(uint64_t serverid, po6::net::location &loc);
            void init(uint64_t shardid, bool backup);
            void reconfigure();
            void bulk_load_persistent();

            // Mutexes
            po6::threads::mutex meta_update_mutex // shard update mutex
                , edge_map_mutex
                , perm_del_mutex
                , config_mutex
                , restore_mutex
                , exit_mutex;
            po6::threads::mutex node_map_mutexes[NUM_NODE_MAPS];
        private:
            po6::threads::mutex clock_mutex; // vector clock/queue timestamp mutex
        public:
            po6::threads::mutex msg_count_mutex
                , migration_mutex
                , graph_load_mutex; // gather load times from all shards

            // Messaging infrastructure
            common::comm_wrapper comm;

            // Server manager
            server_manager_link_wrapper sm_stub;
            configuration config;
            bool active_backup, first_config, shard_init;
            po6::threads::cond backup_cond, first_config_cond, shard_init_cond;

            // Consistency
        public:
            void increment_qts(hyper_stub *hs, uint64_t vt_id, uint64_t incr);
            void record_completed_tx(vc::vclock &tx_clk);
            element::node* acquire_node(uint64_t node_id);
            element::node* acquire_node_write(uint64_t node, uint64_t vt_id, uint64_t qts);
            element::node* acquire_node_nonlocking(uint64_t node_id);
            void release_node_write(hyper_stub *hs, element::node *n);
            void release_node(element::node *n, bool migr_node);

            // Graph state
            uint64_t shard_id;
            server_id server;
            std::unordered_set<uint64_t> node_list; // list of node ids currently on this shard
            std::unordered_map<uint64_t, element::node*> nodes[NUM_NODE_MAPS]; // node id -> ptr to node object
            std::unordered_map<uint64_t, // node id n ->
                std::unordered_set<uint64_t>> edge_map; // in-neighbors of n
            queue_manager qm;
        public:
            element::node* create_node(hyper_stub *hs, uint64_t node_id, vc::vclock &vclk, bool migrate, bool init_load);
            void delete_node_nonlocking(hyper_stub *hs, element::node *n, vc::vclock &tdel);
            void delete_node(hyper_stub *hs, uint64_t node_id, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void create_edge_nonlocking(hyper_stub *hs, element::node *n, uint64_t edge, uint64_t remote_node,
                    uint64_t remote_loc, vc::vclock &vclk, bool init_load);
            void create_edge(hyper_stub *hs, uint64_t edge_id, uint64_t local_node,
                    uint64_t remote_node, uint64_t remote_loc, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void delete_edge_nonlocking(hyper_stub *hs, element::node *n, uint64_t edge, vc::vclock &tdel);
            void delete_edge(hyper_stub *hs, uint64_t edge_id, uint64_t node_id, vc::vclock &vclk, vc::qtimestamp_t &qts);
            // properties
            void set_node_property_nonlocking(element::node *n,
                    std::string &key, std::string &value, vc::vclock &vclk);
            void set_node_property(hyper_stub *hs, uint64_t node_id,
                    std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void set_edge_property_nonlocking(element::node *n, uint64_t edge_id,
                    std::string &key, std::string &value, vc::vclock &vclk);
            void set_edge_property(hyper_stub *hs, uint64_t node_id, uint64_t edge_id,
                    std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts);
            uint64_t get_node_count();
            bool node_exists_nonlocking(uint64_t node_id);

            // Initial graph loading
            uint64_t max_load_time;
            uint32_t load_count;

            // Permanent deletion
        public:
            typedef std::priority_queue<del_obj*, std::vector<del_obj*>, perm_del_compare> del_queue_t;
            del_queue_t perm_del_queue;
            void delete_migrated_node(uint64_t migr_node);
            void permanent_delete_loop(uint64_t vt_id, bool outstanding_progs);
        private:
            void permanent_node_delete(element::node *n);

            // Migration
        public:
            bool current_migr, migr_token, migrated;
            uint64_t migr_chance, migr_node, migr_shard, migr_token_hops, migr_vt;
            std::unordered_map<uint64_t, uint32_t> agg_msg_count;
            std::vector<std::pair<uint64_t, uint32_t>> cldg_nodes;
            std::vector<std::pair<uint64_t, uint32_t>>::iterator cldg_iter;
            std::unordered_set<uint64_t> ldg_nodes;
            std::unordered_set<uint64_t>::iterator ldg_iter;
            std::vector<uint64_t> shard_node_count;
            std::unordered_map<uint64_t, def_write_lst> deferred_writes; // for migrating nodes
            std::unordered_map<uint64_t, std::vector<std::unique_ptr<message::message>>> deferred_reads; // for migrating nodes
            std::vector<uint64_t> nop_count;
            vc::vclock max_clk // to compare against for checking if node is deleted
                , zero_clk; // all zero clock for migration thread in queue
            nmap::nmap_stub remapper;
            std::unordered_map<uint64_t, uint64_t> remap;
            void update_migrated_nbr_nonlocking(element::node *n, uint64_t migr_node, uint64_t old_loc, uint64_t new_loc);
            void update_migrated_nbr(uint64_t node, uint64_t old_loc, uint64_t new_loc);
            void update_node_mapping(uint64_t node, uint64_t shard);
            std::vector<uint64_t> max_prog_id // max prog id seen from each vector timestamper
                , target_prog_id
                , max_done_id; // max id done from each VT
            std::vector<vc::vclock_t> max_done_clk; // vclk of cumulative last node program completed
            std::vector<bool> migr_edge_acks;
            uint64_t msg_count;
            
            // node programs
        private:
            po6::threads::mutex node_prog_state_mutex;
            std::unordered_set<uint64_t> done_ids; // request ids that have finished
            std::unordered_map<uint64_t, std::vector<uint64_t>> outstanding_prog_states; // maps request_id to list of nodes that have created prog state for that req id
            void delete_prog_states(uint64_t req_id, std::vector<uint64_t> &node_ids);
        public:
            void mark_nodes_using_state(uint64_t req_id, std::vector<uint64_t> &node_ids);
            void add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests);
            bool check_done_request(uint64_t req_id);

            std::unordered_map<std::tuple<uint64_t, uint64_t, uint64_t>, void *> node_prog_running_states; // used for fetching cache contexts
            po6::threads::mutex node_prog_running_states_mutex;

            po6::threads::mutex watch_set_lookups_mutex;
            uint64_t watch_set_lookups;
            uint64_t watch_set_nops;
            uint64_t watch_set_piggybacks;

            // Fault tolerance
        public:
            std::vector<hyper_stub*> hstub;
            bool restore_done;
            po6::threads::cond restore_cv;
            void restore_backup();

            // exit
            bool to_exit;
    };

    inline
    shard :: shard(uint64_t serverid, po6::net::location &loc)
        : comm(loc, NUM_THREADS, SHARD_MSGRECV_TIMEOUT)
        , sm_stub(server_id(serverid), comm.get_loc())
        , active_backup(false)
        , first_config(false)
        , shard_init(false)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , shard_init_cond(&config_mutex)
        , shard_id(UINT64_MAX)
        //, shard_id(shardid)
        , server(serverid)
        , current_migr(false)
        , migr_token(false)
        , migrated(false)
        , migr_chance(0)
        , shard_node_count(NumShards, 0)
        , nop_count(NumVts, 0)
        , max_clk(UINT64_MAX, UINT64_MAX)
        , zero_clk(0, 0)
        , max_prog_id(NumVts, 0)
        , target_prog_id(NumVts, 0)
        , max_done_id(NumVts, 0)
        , max_done_clk(NumVts, vc::vclock_t(NumVts, 0))
        , migr_edge_acks(NumShards, false)
        , msg_count(0)
        , watch_set_lookups(0)
        , watch_set_nops(0)
        , watch_set_piggybacks(0)
        , restore_done(false)
        , restore_cv(&restore_mutex)
        , to_exit(false)
    {
    }

    // initialize: msging layer
    //           , chronos client
    //           , hyperdex stub
    // caution: assume holding config_mutex
    inline void
    shard :: init(uint64_t shardid, bool backup)
    {
        shard_id = shardid;
        comm.init(config);
        for (int i = 0; i < NUM_THREADS; i++) {
            hstub.push_back(new hyper_stub(shard_id));
        }
        if (!backup) {
            hstub.back()->init(); // put initial vclock, qts
        }
    }

    // reconfigure shard according to new cluster configuration
    // caution: assume holding config_mutex
    inline void
    shard :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure" << std::endl;

        comm.reconfigure(config);
        //if (comm.reconfigure(config) == server.get()
        // && !active_backup
        // && server.get() > NumEffectiveServers) {
        //        WDEBUG << "Now active for shard " << shard_id << std::endl;
        //        // this server is now primary for the shard
        //        active_backup = true;
        //        restore_backup();
        //        backup_cond.signal();
        //}
    }

    inline void
    shard :: bulk_load_persistent()
    {
        hstub[0]->bulk_load(nodes, edge_map);
    }

    // Consistency methods
    inline void
    shard :: increment_qts(hyper_stub *hs, uint64_t vt_id, uint64_t incr)
    {
        hs->increment_qts(vt_id, incr);
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
    shard :: acquire_node(uint64_t node_id)
    {
        uint64_t map_idx = node_id % NUM_NODE_MAPS;

        element::node *n = NULL;
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node_id);
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
    shard :: acquire_node_write(uint64_t node, uint64_t vt_id, uint64_t qts)
    {
        uint64_t map_idx = node % NUM_NODE_MAPS;

        element::node *n = NULL;
        auto comp = std::make_pair(vt_id, qts);
        node_map_mutexes[map_idx].lock();
        auto node_iter = nodes[map_idx].find(node);
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
    shard :: acquire_node_nonlocking(uint64_t node_id)
    {
        uint64_t map_idx = node_id % NUM_NODE_MAPS;

        element::node *n = NULL;
        auto node_iter = nodes[map_idx].find(node_id);
        if (node_iter != nodes[map_idx].end()) {
            n = node_iter->second;
        }
        return n;
    }

    // write n->tx_queue to HyperDex, and then release node
    inline void
    shard :: release_node_write(hyper_stub *hs, element::node *n)
    {
        hs->update_tx_queue(*n);
        release_node(n, false);
    }

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(element::node *n, bool migr_done=false)
    {
        uint64_t map_idx = n->base.get_id() % NUM_NODE_MAPS;

        node_map_mutexes[map_idx].lock();
        n->in_use = false;
        if (migr_done) {
            n->migr_cv.broadcast();
        }
        if (n->waiters > 0) {
            n->cv.signal();
            node_map_mutexes[map_idx].unlock();
        } else if (n->permanently_deleted) {
            uint64_t node_id = n->base.get_id();
            nodes[map_idx].erase(node_id);
            node_list.erase(node_id);
            node_map_mutexes[map_idx].unlock();

            migration_mutex.lock();
            shard_node_count[shard_id - ShardIdIncr]--;
            migration_mutex.unlock();
            
            msg_count_mutex.lock();
            agg_msg_count.erase(node_id);
            msg_count_mutex.unlock();
            
            permanent_node_delete(n);
        } else {
            node_map_mutexes[map_idx].unlock();
        }
        n = NULL;
    }


    // Graph state update methods

    inline element::node*
    shard :: create_node(hyper_stub *hs, uint64_t node_id, vc::vclock &vclk, bool migrate, bool init_load=false)
    {
        uint64_t map_idx = node_id % NUM_NODE_MAPS;
        element::node *new_node = new element::node(node_id, vclk, &node_map_mutexes[map_idx]);

        if (!init_load) {
            node_map_mutexes[map_idx].lock();
        }

        bool success = nodes[map_idx].emplace(node_id, new_node).second;
        if (!success) {
            // node already existed because of partially executed tx due to some failure
            assert(!init_load);
            node_map_mutexes[map_idx].unlock();
            return NULL;
        }
        node_list.emplace(node_id);
        if (!init_load) {
            node_map_mutexes[map_idx].unlock();
        }

        if (!init_load) {
            migration_mutex.lock();
        }
        shard_node_count[shard_id - ShardIdIncr]++;
        if (!init_load) {
            migration_mutex.unlock();
        }

        if (!migrate) {
            new_node->state = element::node::mode::STABLE;
            new_node->msg_count.resize(NumShards, 0);
            if (!init_load) {
                // store in Hyperdex
                std::unordered_set<uint64_t> empty_set;
                hs->put_node(*new_node, empty_set);
                release_node(new_node);
            } else {
                new_node->in_use = false;
            }
        }
        return new_node;
    }

    inline void
    shard :: delete_node_nonlocking(hyper_stub *hs, element::node *n, vc::vclock &tdel)
    {
        n->base.update_del_time(tdel);
        n->updated = true;
        // persist changes
        // we permanently delete node from HyperDex
        // since if shard crashes, concurrent node progs are dropped
        hs->del_node(*n);
    }

    inline void
    shard :: delete_node(hyper_stub *hs, uint64_t node_id, vc::vclock &tdel, vc::qtimestamp_t &qts)
    {
        element::node *n = acquire_node_write(node_id, tdel.vt_id, qts[tdel.vt_id]);
        if (n == NULL) {
            // node is being migrated
            migration_mutex.lock();
            deferred_writes[node_id].emplace_back(deferred_write(message::NODE_DELETE_REQ, tdel));
            migration_mutex.unlock();
        } else {
            delete_node_nonlocking(hs, n, tdel);
            release_node_write(hs, n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace(new del_obj(message::NODE_DELETE_REQ, tdel, node_id));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge_nonlocking(hyper_stub *hs, element::node *n, uint64_t edge, uint64_t remote_node,
            uint64_t remote_loc, vc::vclock &vclk, bool init_load=false)
    {
        element::edge *new_edge = new element::edge(edge, vclk, remote_loc, remote_node);
        n->add_edge(new_edge);
        n->updated = true;
        // update edge map
        if (!init_load) {
            edge_map_mutex.lock();
        }
        edge_map[remote_node].emplace(n->base.get_id());
        if (!init_load) {
            // store in Hyperdex
            hs->add_out_edge(*n, new_edge);
            hs->add_in_nbr(n->base.get_id(), remote_node);
            edge_map_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge(hyper_stub *hs, uint64_t edge_id, uint64_t local_node,
            uint64_t remote_node, uint64_t remote_loc, vc::vclock &vclk, vc::qtimestamp_t &qts)
    {
        // fault tolerance: create_edge is idempotent
        element::node *n = acquire_node_write(local_node, vclk.vt_id, qts[vclk.vt_id]);
        if (n == NULL) {
            // node is being migrated
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[local_node];
            dwl.emplace_back(deferred_write(message::EDGE_CREATE_REQ, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge = edge_id;
            dw.remote_node = remote_node;
            dw.remote_loc = remote_loc; 
            migration_mutex.unlock();
        } else {
            assert(n->base.get_id() == local_node);
            create_edge_nonlocking(hs, n, edge_id, remote_node, remote_loc, vclk);
            release_node_write(hs, n);
        }
    }

    inline void
    shard :: delete_edge_nonlocking(hyper_stub *hs, element::node *n, uint64_t edge, vc::vclock &tdel)
    {
        // already_exec check for fault tolerance
        assert(n->out_edges.find(edge) != n->out_edges.end()); // cannot have been permanently deleted
        element::edge *e = n->out_edges[edge];
        e->base.update_del_time(tdel);
        n->updated = true;
        n->dependent_del++;

        // update edge map
        uint64_t remote = e->nbr.get_id();
        edge_map_mutex.lock();
        if (edge_map.find(remote) != edge_map.end()) {
            auto &node_set = edge_map[remote];
            node_set.erase(n->base.get_id());
            if (node_set.empty()) {
                edge_map.erase(remote);
            }
        }
        // persist changes
        // we permanently delete edge from HyperDex
        // since if shard crashes, concurrent node progs are dropped
        hs->remove_out_edge(*n, e);
        hs->remove_in_nbr(n->base.get_id(), remote);
        edge_map_mutex.unlock();
    }

    inline void
    shard :: delete_edge(hyper_stub *hs, uint64_t edge_id, uint64_t node_id, vc::vclock &tdel, vc::qtimestamp_t &qts)
    {
        element::node *n = acquire_node_write(node_id, tdel.vt_id, qts[tdel.vt_id]);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_id];
            dwl.emplace_back(deferred_write(message::EDGE_DELETE_REQ, tdel));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge = edge_id;
            migration_mutex.unlock();
        } else {
            delete_edge_nonlocking(hs, n, edge_id, tdel);
            release_node_write(hs, n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace(new del_obj(message::EDGE_DELETE_REQ, tdel, node_id, edge_id));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: set_node_property_nonlocking(element::node *n,
            std::string &key, std::string &value, vc::vclock &vclk)
    {
        db::element::property p(key, value, vclk);
        n->base.check_and_add_property(p);
    }

    inline void
    shard :: set_node_property(hyper_stub *hs, uint64_t node_id,
            std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts)
    {
        // set_node_prop is idempotent for fault tolerance
        element::node *n = acquire_node_write(node_id, vclk.vt_id, qts[vclk.vt_id]);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_id];
            dwl.emplace_back(deferred_write(message::NODE_SET_PROP, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.key = std::move(key);
            dw.value = std::move(value);
            migration_mutex.unlock();
        } else {
            set_node_property_nonlocking(n, *key, *value, vclk);
            // store in Hyperdex
            hs->update_properties(*n);
            release_node_write(hs, n);
        }
    }

    inline void
    shard :: set_edge_property_nonlocking(element::node *n, uint64_t edge_id,
            std::string &key, std::string &value, vc::vclock &vclk)
    {
        if (n->out_edges.find(edge_id) != n->out_edges.end()) {
            db::element::edge *e = n->out_edges[edge_id];
            db::element::property p(key, value, vclk);
            e->base.check_and_add_property(p);
        }
    }

    inline void
    shard :: set_edge_property(hyper_stub *hs, uint64_t node_id, uint64_t edge_id,
            std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts)
    {
        element::node *n = acquire_node_write(node_id, vclk.vt_id, qts[vclk.vt_id]);
        if (n == NULL) {
            migration_mutex.lock();
            def_write_lst &dwl = deferred_writes[node_id];
            dwl.emplace_back(deferred_write(message::EDGE_SET_PROP, vclk));
            deferred_write &dw = dwl[dwl.size()-1];
            dw.edge = edge_id;
            dw.key = std::move(key);
            dw.value = std::move(value);
            migration_mutex.unlock();
        } else {
            set_edge_property_nonlocking(n, edge_id, *key, *value, vclk);
            if (n->out_edges.find(edge_id) != n->out_edges.end()) {
                // store in Hyperdex
                hs->add_out_edge(*n, n->out_edges[edge_id]);
            }
            release_node_write(hs, n);
        }
    }

    // return true if node already created
    inline bool
    shard :: node_exists_nonlocking(uint64_t node_id)
    {
        uint64_t map_idx = node_id % NUM_NODE_MAPS;
        return (nodes[map_idx].count(node_id) > 0);
    }

    // permanent deletion

    inline void
    shard :: delete_migrated_node(uint64_t migr_node)
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

    inline void
    shard :: permanent_delete_loop(uint64_t vt_id, bool outstanding_progs)
    {
        element::node *n;
        del_obj *dobj;
        perm_del_mutex.lock();
        while (true) {
            bool to_del = !perm_del_queue.empty();
            if (to_del) {
                dobj = perm_del_queue.top();
                if (!outstanding_progs) {
                    dobj->no_outstanding_progs[vt_id] = true;
                }
                // if all VTs have no outstanding node progs, then everything can be permanently deleted
                if (!weaver_util::all(dobj->no_outstanding_progs)) {
                    for (uint64_t i = 0; (i < NumVts) && to_del; i++) {
                        if (max_done_clk[i].size() < NumVts) {
                            to_del = false;
                            break;
                        }
                        to_del = (order::compare_two_clocks(dobj->vclk.clock, max_done_clk[i]) == 0);
                    }
                }
            }
            if (!to_del) {
                break;
            }
            switch (dobj->type) {
                case message::NODE_DELETE_REQ:
                    n = acquire_node(dobj->node);
                    if (n != NULL) {
                        n->permanently_deleted = true;
                        for (auto &e: n->out_edges) {
                            uint64_t node = e.second->nbr.id;
                            assert(edge_map.find(node) != edge_map.end());
                            auto &node_set = edge_map[node];
                            node_set.erase(dobj->node);
                            if (node_set.empty()) {
                                edge_map.erase(node);
                            }
                        }
                        release_node(n);
                    }
                    break;

                case message::EDGE_DELETE_REQ:
                    n = acquire_node(dobj->node);
                    if (n != NULL) {
                        if (n->out_edges.find(dobj->edge) != n->out_edges.end()) {
                            if (n->last_perm_deletion == nullptr ||
                                    order::compare_two_vts(*n->last_perm_deletion,
                                        n->out_edges.at(dobj->edge)->base.get_del_time()) == 0) {
                                n->last_perm_deletion.reset(new vc::vclock(std::move(n->out_edges.at(dobj->edge)->base.get_del_time())));
                            }
                            delete n->out_edges[dobj->edge];
                            n->out_edges.erase(dobj->edge);
                        }
                        release_node(n);
                    }
                    break;

                default:
                    WDEBUG << "invalid type " << dobj->type << " in deleted object" << std::endl;
            }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
            // we know dobj will be initialized here
            delete dobj;
#pragma GCC diagnostic pop
            perm_del_queue.pop();
        }
        if (!outstanding_progs) {
            auto copy_del_queue = perm_del_queue;
            while (!copy_del_queue.empty()) {
                dobj = copy_del_queue.top();
                dobj->no_outstanding_progs[vt_id] = true;
                copy_del_queue.pop();
            }
        }
        perm_del_mutex.unlock();
    }

    inline void
    shard :: permanent_node_delete(element::node *n)
    {
        message::message msg;
        assert(n->waiters == 0);
        assert(!n->in_use);
        // send msg to each shard to delete incoming edges
        // this happens lazily, and there could be dangling edges
        // users should explicitly delete edges before nodes if the program requires
        // this loop isn't executed in case of deletion of migrated nodes
        if (n->state != element::node::mode::MOVED) {
            for (uint64_t shard = ShardIdIncr; shard < ShardIdIncr + NumShards; shard++) {
                msg.prepare_message(message::PERMANENTLY_DELETED_NODE, n->base.get_id());
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
    shard :: update_migrated_nbr_nonlocking(element::node *n, uint64_t migr_node, uint64_t old_loc, uint64_t new_loc)
    {
        bool found = false;
        element::edge *e;
        for (auto &x: n->out_edges) {
            e = x.second;
            if (e->nbr.id == migr_node && e->nbr.loc == old_loc) {
                e->nbr.loc = new_loc;
                found = true;
            }
        }
        assert(found);
    }

    inline void
    shard :: update_migrated_nbr(uint64_t node, uint64_t old_loc, uint64_t new_loc)
    {
        std::unordered_set<uint64_t> nbrs;
        element::node *n;
        edge_map_mutex.lock();
        nbrs = edge_map[node];
        edge_map_mutex.unlock();
        for (uint64_t nbr: nbrs) {
            n = acquire_node(nbr);
            update_migrated_nbr_nonlocking(n, node, old_loc, new_loc);
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
    shard :: update_node_mapping(uint64_t node, uint64_t shard)
    {
        remap.clear();
        remap[node] = shard;
        remapper.put_mappings(remap);
    }

    // node program

    inline void
    shard :: delete_prog_states(uint64_t req_id, std::vector<uint64_t> &node_ids)
    {
        for (uint64_t node_id : node_ids) {
            db::element::node *node = acquire_node(node_id);

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
    shard :: mark_nodes_using_state(uint64_t req_id, std::vector<uint64_t> &node_ids)
    {
        node_prog_state_mutex.lock();
        bool done_request = (done_ids.find(req_id) != done_ids.end());
        if (!done_request) {
            auto state_list_iter = outstanding_prog_states.find(req_id);
            if (state_list_iter == outstanding_prog_states.end()) {
                outstanding_prog_states.emplace(req_id, std::move(node_ids));
            } else {
                std::vector<uint64_t> &add_to = state_list_iter->second;
                add_to.reserve(add_to.size() + node_ids.size());
                add_to.insert(add_to.end(), node_ids.begin(), node_ids.end());
            }
            node_prog_state_mutex.unlock();
        } else { // request is finished, just delete things
            node_prog_state_mutex.unlock();
            delete_prog_states(req_id, node_ids);
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

        std::vector<std::pair<uint64_t, std::vector<uint64_t>>> to_delete;

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
        std::unordered_map<uint64_t, uint64_t> qts_map;
        bool migr_token;
        restore_mutex.lock();
        hstub.back()->restore_backup(qts_map, migr_token, nodes, edge_map, node_map_mutexes);
        qm.restore_backup(qts_map);
        restore_done = true;
        restore_cv.signal();
        restore_mutex.unlock();
    }
}

#endif
