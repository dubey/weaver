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
#include <vector>
#include <unordered_map>
#include <bitset>
#include <po6/threads/mutex.h>
#include <po6/net/location.h>
#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>

#include "node_prog/base_classes.h"
#include "common/weaver_constants.h"
#include "common/ids.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/comm_wrapper.h"
#include "common/event_order.h"
#include "common/configuration.h"
#include "common/server_manager_link_wrapper.h"
#include "element/element.h"
#include "element/node.h"
#include "element/edge.h"
#include "db/state/program_state.h"
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
            shard(uint64_t shard, uint64_t server);
            void init(bool backup);
            void restore_backup();
            void reconfigure();
            void bulk_load_persistent();

            // Mutexes
            po6::threads::mutex update_mutex // shard update mutex
                , edge_map_mutex
                , perm_del_mutex
                , config_mutex;
        private:
            po6::threads::mutex clock_mutex; // vector clock/queue timestamp mutex
        public:
            po6::threads::mutex msg_count_mutex
                , migration_mutex
                , graph_load_mutex; // gather load times from all shards

            // Messaging infrastructure
            common::comm_wrapper comm;

            // Hyperdex stub
            std::vector<hyper_stub*> hstub;

            // Server manager
            server_manager_link_wrapper sm_stub;
            configuration config;
            bool active_backup, first_config;
            po6::threads::cond backup_cond, first_config_cond;

            // Consistency
        public:
            void increment_qts(uint64_t thread_id, uint64_t vt_id, uint64_t incr);
            void record_completed_tx(uint64_t thread_id, uint64_t vt_id, vc::vclock_t &tx_clk);
            element::node* acquire_node(uint64_t node_id);
            void node_tx_order(uint64_t node, uint64_t vt_id, uint64_t qts);
            element::node* acquire_node_write(uint64_t node, uint64_t vt_id, uint64_t qts);
            element::node* acquire_node_nonlocking(uint64_t node_id);
            void release_node(element::node *n, bool migr_node);

            // Graph state
            uint64_t shard_id;
            server_id server;
            std::unordered_set<uint64_t> node_list; // list of node ids currently on this shard
            std::unordered_map<uint64_t, element::node*> nodes; // node id -> ptr to node object
            std::unordered_map<uint64_t, // node id n ->
                std::unordered_set<uint64_t>> edge_map; // in-neighbors of n
            queue_manager qm;
        public:
            element::node* create_node(uint64_t thread_id, uint64_t node_id, vc::vclock &vclk, bool migrate, bool init_load);
            void delete_node_nonlocking(uint64_t thread_id, element::node *n, vc::vclock &tdel);
            void delete_node(uint64_t thread_id, uint64_t node_id, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void create_edge_nonlocking(uint64_t thread_id, element::node *n, uint64_t edge, uint64_t remote_node,
                    uint64_t remote_loc, vc::vclock &vclk, bool init_load);
            void create_edge(uint64_t thread_id, uint64_t edge_id, uint64_t local_node,
                    uint64_t remote_node, uint64_t remote_loc, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void delete_edge_nonlocking(uint64_t thread_id, element::node *n, uint64_t edge, vc::vclock &tdel);
            void delete_edge(uint64_t thread_id, uint64_t edge_id, uint64_t node_id, vc::vclock &vclk, vc::qtimestamp_t &qts);
            // properties
            void set_node_property_nonlocking(uint64_t thread_id, element::node *n,
                    std::string &key, std::string &value, vc::vclock &vclk);
            void set_node_property(uint64_t thread_id, uint64_t node_id,
                    std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts);
            void set_edge_property_nonlocking(uint64_t thread_id, element::node *n, uint64_t edge_id,
                    std::string &key, std::string &value, vc::vclock &vclk);
            void set_edge_property(uint64_t thread_id, uint64_t node_id, uint64_t edge_id,
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
            const char *loc_space = "weaver_loc_mapping";
            const char *loc_attrName = "shard";
            hyperdex::Client cl;
            void update_migrated_nbr_nonlocking(element::node *n, uint64_t migr_node, uint64_t old_loc, uint64_t new_loc);
            void update_migrated_nbr(uint64_t node, uint64_t old_loc, uint64_t new_loc);
            void update_node_mapping(uint64_t node, uint64_t shard);
            std::vector<uint64_t> max_prog_id // max prog id seen from each vector timestamper
                , target_prog_id
                , max_done_id; // max id done from each VT
            std::vector<vc::vclock_t> max_done_clk; // vclk of cumulative last node program completed
            std::bitset<NUM_SHARDS> migr_edge_acks;
            uint64_t msg_count;
            
            // node programs
        private:
            state::program_state prog_state; 
        public:
            std::shared_ptr<node_prog::Node_State_Base> 
                fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_id);
            void insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_id,
                    std::shared_ptr<node_prog::Node_State_Base> toAdd);
            void add_done_request(uint64_t completed_req_id, node_prog::prog_type type);
            void add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests);
            bool check_done_request(uint64_t req_id);

            std::unordered_map<std::tuple<uint64_t, uint64_t, uint64_t>, void *> node_prog_running_states; // used for fetching cache contexts
            po6::threads::mutex node_prog_running_states_mutex;

            po6::threads::mutex watch_set_lookups_mutex;
            uint64_t watch_set_lookups;
            uint64_t watch_set_nops;
            uint64_t cache_skips;
            //po6::threads::mutex node_prog_mutex;
            //std::unordered_map<uint64_t, std::deque<node_params_t>> node_progs_deque; // req id to node prog deque

    };

    inline
    shard :: shard(uint64_t shardid, uint64_t serverid)
        : comm(serverid, NUM_THREADS, SHARD_MSGRECV_TIMEOUT, false)
        , sm_stub(server_id(serverid), comm.get_loc())
        , active_backup(false)
        , first_config(false)
        , backup_cond(&config_mutex)
        , first_config_cond(&config_mutex)
        , shard_id(shardid)
        , server(serverid)
        , current_migr(false)
        , migr_token(false)
        , migrated(false)
        , migr_chance(0)
        , shard_node_count(NUM_SHARDS, 0)
        , nop_count(NUM_VTS, 0)
        , max_clk(UINT64_MAX, UINT64_MAX)
        , zero_clk(0, 0)
        , cl(HYPERDEX_COORD_IPADDR, HYPERDEX_COORD_PORT)
        , max_prog_id(NUM_VTS, 0)
        , target_prog_id(NUM_VTS, 0)
        , max_done_id(NUM_VTS, 0)
        , max_done_clk(NUM_VTS, vc::vclock_t())
        , msg_count(0)
        , prog_state()
        , watch_set_lookups(0)
        , watch_set_nops(0)
        , cache_skips(0)
    {
        assert(NUM_VTS == KRONOS_NUM_VTS);
        message::prog_state = &prog_state;
        for (int i = 0; i < NUM_THREADS; i++) {
            hstub.push_back(new hyper_stub(shard_id));
        }
    }

    // initialize: msging layer
    //           , chronos client
    //           , hyperdex stub
    // caution: assume holding config_mutex
    inline void
    shard :: init(bool backup)
    {
        comm.init(config);
        if (!backup) {
            hstub.back()->init(); // put initial vclock, qts
        }
    }

    // restore state when backup becomes primary due to failure
    inline void
    shard :: restore_backup()
    {
        std::unordered_map<uint64_t, uint64_t> qts_map;
        std::unordered_map<uint64_t, vc::vclock_t> last_clocks;
        hstub.back()->restore_backup(qts_map, last_clocks);
        qm.restore_backup(qts_map, last_clocks);
    }

    // reconfigure shard according to new cluster configuration
    // caution: assume holding config_mutex
    inline void
    shard :: reconfigure()
    {
        WDEBUG << "Cluster reconfigure triggered\n";
        for (uint64_t i = 0; i < NUM_SERVERS; i++) {
            server::state_t st = config.get_state(server_id(i));
            if (st != server::AVAILABLE) {
                WDEBUG << "Server " << i << " is in trouble, has state " << st << std::endl;
            } else {
                WDEBUG << "Server " << i << " is healthy, has state " << st << std::endl;
            }
        }
        if (comm.reconfigure(config) == server.get()) {
            // this server is now primary for the shard
            active_backup = true;
            backup_cond.signal();
        }
    }

    inline void
    shard :: bulk_load_persistent()
    {
        //std::unordered_set<uint64_t> empty_set;
        //uint64_t cnt = 0;
        //for (auto &x: nodes) {
        //    //hstub[0]->put_node(*x.second, empty_set);
        //    if (++cnt % 10000 == 0) {
        //        WDEBUG << "wrote " << cnt << " nodes to HyperDex" << std::endl;
        //    }
        //}
    }

    // Consistency methods
    inline void
    shard :: increment_qts(uint64_t, uint64_t vt_id, uint64_t incr)
    {
        //hstub[thread_id]->increment_qts(vt_id, incr);
        qm.increment_qts(vt_id, incr);
    }

    inline void
    shard :: record_completed_tx(uint64_t, uint64_t vt_id, vc::vclock_t &tx_clk)
    {
        //hstub[thread_id]->update_last_clocks(vt_id, tx_clk);
        qm.record_completed_tx(vt_id, tx_clk);
    }

    // find the node corresponding to given id
    // lock and return the node
    // return NULL if node does not exist (possibly permanently deleted)
    inline element::node*
    shard :: acquire_node(uint64_t node_id)
    {
        element::node *n = NULL;
        update_mutex.lock();
        if (nodes.find(node_id) != nodes.end()) {
            n = nodes[node_id];
            n->waiters++;
            while (n->in_use) {
                n->cv.wait();
            }
            n->waiters--;
            n->in_use = true;
        }
        update_mutex.unlock();

        return n;
    }

    inline void
    shard :: node_tx_order(uint64_t node, uint64_t vt_id, uint64_t qts)
    {
        update_mutex.lock();
        if (nodes.find(node) != nodes.end()) {
            nodes[node]->tx_queue.emplace_back(std::make_pair(vt_id, qts));
        }
        update_mutex.unlock();
    }

    inline element::node*
    shard :: acquire_node_write(uint64_t node, uint64_t vt_id, uint64_t qts)
    {
        element::node *n = NULL;
        auto comp = std::make_pair(vt_id, qts);
        update_mutex.lock();
        if (nodes.find(node) != nodes.end()) {
            n = nodes[node];
            n->waiters++;
            while (n->in_use || n->tx_queue.front() != comp) {
                n->cv.wait();
            }
            n->waiters--;
            n->in_use = true;
            n->tx_queue.pop_front();
        }
        update_mutex.unlock();

        return n;
    }

    inline element::node*
    shard :: acquire_node_nonlocking(uint64_t node_id)
    {
        element::node *n = NULL;
        if (nodes.find(node_id) != nodes.end()) {
            n = nodes[node_id];
        }
        return n;
    }

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(element::node *n, bool migr_done=false)
    {
        update_mutex.lock();
        n->in_use = false;
        if (migr_done) {
            n->migr_cv.broadcast();
        }
        if (n->waiters > 0) {
            n->cv.signal();
            update_mutex.unlock();
        } else if (n->permanently_deleted) {
            uint64_t node_id = n->base.get_id();
            nodes.erase(node_id);
            node_list.erase(node_id);
            update_mutex.unlock();

            migration_mutex.lock();
            shard_node_count[shard_id - SHARD_ID_INCR]--;
            migration_mutex.unlock();
            
            msg_count_mutex.lock();
            agg_msg_count.erase(node_id);
            msg_count_mutex.unlock();
            
            permanent_node_delete(n);
        } else {
            update_mutex.unlock();
        }
    }


    // Graph state update methods

    inline element::node*
    shard :: create_node(uint64_t, uint64_t node_id, vc::vclock &vclk, bool migrate, bool init_load=false)
    {
        element::node *new_node = new element::node(node_id, vclk, &update_mutex);
        
        if (!init_load) {
            update_mutex.lock();
        }
        bool success = nodes.emplace(node_id, new_node).second;
        assert(success);
        UNUSED(success);
        node_list.emplace(node_id);
        if (!init_load) {
            update_mutex.unlock();
        }

        if (!init_load) {
            migration_mutex.lock();
        }
        shard_node_count[shard_id - SHARD_ID_INCR]++;
        if (!init_load) {
            migration_mutex.unlock();
        }
        
        if (!migrate) {
            new_node->state = element::node::mode::STABLE;
            new_node->msg_count.resize(NUM_SHARDS, 0);
            // store in Hyperdex
            std::unordered_set<uint64_t> empty_set;
            //hstub[thread_id]->put_node(*new_node, empty_set);
            release_node(new_node);
        }
        return new_node;
    }

    inline void
    shard :: delete_node_nonlocking(uint64_t, element::node *n, vc::vclock &tdel)
    {
        n->base.update_del_time(tdel);
        n->updated = true;
        // store in Hyperdex
        //hstub[thread_id]->update_del_time(*n);
    }

    inline void
    shard :: delete_node(uint64_t thread_id, uint64_t node_id, vc::vclock &tdel, vc::qtimestamp_t &qts)
    {
        element::node *n = acquire_node_write(node_id, tdel.vt_id, qts[tdel.vt_id]);
        if (n == NULL) {
            // node is being migrated
            migration_mutex.lock();
            deferred_writes[node_id].emplace_back(deferred_write(message::NODE_DELETE_REQ, tdel));
            migration_mutex.unlock();
        } else {
            delete_node_nonlocking(thread_id, n, tdel);
            release_node(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace(new del_obj(message::NODE_DELETE_REQ, tdel, node_id));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: create_edge_nonlocking(uint64_t, element::node *n, uint64_t edge, uint64_t remote_node,
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
            edge_map_mutex.unlock();
        }
        // store in Hyperdex
        //hstub[thread_id]->add_out_edge(*n, new_edge);
        //hstub[thread_id]->add_in_nbr(remote_node, n->base.get_id());
    }

    inline void
    shard :: create_edge(uint64_t thread_id, uint64_t edge_id, uint64_t local_node,
            uint64_t remote_node, uint64_t remote_loc, vc::vclock &vclk, vc::qtimestamp_t &qts)
    {
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
            create_edge_nonlocking(thread_id, n, edge_id, remote_node, remote_loc, vclk);
            release_node(n);
        }
    }

    inline void
    shard :: delete_edge_nonlocking(uint64_t, element::node *n, uint64_t edge, vc::vclock &tdel)
    {
#ifdef weaver_debug_
        assert(n->edge_handles.find(edge) != n->edge_handles.end());
#endif
        assert(n->out_edges.find(edge) != n->out_edges.end());
        element::edge *e = n->out_edges[edge];
        e->base.update_del_time(tdel);
        n->updated = true;
        n->dependent_del++;
        // update edge map
        uint64_t remote = e->nbr.id;
        edge_map_mutex.lock();
        assert(edge_map.find(remote) != edge_map.end());
        auto &node_set = edge_map[remote];
        node_set.erase(n->base.get_id());
        if (node_set.empty()) {
                edge_map.erase(remote);
            }
        edge_map_mutex.unlock();        
        // store in Hyperdex
        //hstub[thread_id]->add_out_edge(*n, e);
    }

    inline void
    shard :: delete_edge(uint64_t thread_id, uint64_t edge_id, uint64_t node_id, vc::vclock &tdel, vc::qtimestamp_t &qts)
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
            delete_edge_nonlocking(thread_id, n, edge_id, tdel);
            release_node(n);
            // record object for permanent deletion later on
            perm_del_mutex.lock();
            perm_del_queue.emplace(new del_obj(message::EDGE_DELETE_REQ, tdel, node_id, edge_id));
            perm_del_mutex.unlock();
        }
    }

    inline void
    shard :: set_node_property_nonlocking(uint64_t, element::node *n,
            std::string &key, std::string &value, vc::vclock &vclk)
    {
        db::element::property p(key, value, vclk);
        n->base.check_and_add_property(p);
        // store in Hyperdex
        //hstub[thread_id]->update_properties(*n);
    }

    inline void
    shard :: set_node_property(uint64_t thread_id, uint64_t node_id,
            std::unique_ptr<std::string> key, std::unique_ptr<std::string> value, vc::vclock &vclk, vc::qtimestamp_t &qts)
    {
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
            set_node_property_nonlocking(thread_id, n, *key, *value, vclk);
            release_node(n);
        }
    }

    inline void
    shard :: set_edge_property_nonlocking(uint64_t, element::node *n, uint64_t edge_id,
            std::string &key, std::string &value, vc::vclock &vclk)
    {
        db::element::edge *e = n->out_edges[edge_id];
        db::element::property p(key, value, vclk);
        e->base.check_and_add_property(p);
        // store in Hyperdex
        //hstub[thread_id]->add_out_edge(*n, e);
    }

    inline void
    shard :: set_edge_property(uint64_t thread_id, uint64_t node_id, uint64_t edge_id,
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
            set_edge_property_nonlocking(thread_id, n, edge_id, *key, *value, vclk);
            release_node(n);
        }
    }

    // return true if node already created
    inline bool
    shard :: node_exists_nonlocking(uint64_t node_id)
    {
        return (nodes.find(node_id) != nodes.end());
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
        prog_state.delete_node_state(migr_node);
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
                    dobj->no_outstanding_progs.set(vt_id);
                }
                // if all VTs have no outstanding node progs, then everything can be permanently deleted
                if (!dobj->no_outstanding_progs.all()) {
                    for (uint64_t i = 0; (i < NUM_VTS) && to_del; i++) {
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
                    break;

                case message::EDGE_DELETE_REQ:
                    n = acquire_node(dobj->node);
                    assert(n->out_edges.count(dobj->edge) > 0);
                    if (n->last_perm_deletion == nullptr ||
                            order::compare_two_vts(*n->last_perm_deletion,
                                n->out_edges.at(dobj->edge)->base.get_del_time()) == 0) {
                        n->last_perm_deletion.reset(new vc::vclock(std::move(n->out_edges.at(dobj->edge)->base.get_del_time())));
                    }
                    delete n->out_edges[dobj->edge];
                    n->out_edges.erase(dobj->edge);
                    release_node(n);
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
                dobj->no_outstanding_progs.set(vt_id);
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
            for (uint64_t shard = SHARD_ID_INCR; shard < SHARD_ID_INCR + NUM_SHARDS; shard++) {
                message::prepare_message(msg, message::PERMANENTLY_DELETED_NODE, n->base.get_id());
                comm.send(shard, msg.buf);
            }
            for (auto &e: n->out_edges) {
                delete e.second;
            }
            n->out_edges.clear();
            prog_state.delete_node_state(n->base.get_id());
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
            message::prepare_message(msg, message::MIGRATED_NBR_ACK, shard_id, max_prog_id,
                    shard_node_count[shard_id-SHARD_ID_INCR]);
            comm.send(old_loc, msg.buf);
        } else {
            for (int i = 0; i < NUM_VTS; i++) {
                if (target_prog_id[i] < max_prog_id[i]) {
                    target_prog_id[i] = max_prog_id[i];
                }
            }
            migr_edge_acks.set(shard_id - SHARD_ID_INCR);
        }
        migration_mutex.unlock();
    }

    inline void
    shard :: update_node_mapping(uint64_t node, uint64_t shard)
    {
        const char *space = loc_space;
        const char *attrName = loc_attrName;
        hyperdex_client_attribute attrs_to_add;
        hyperdex_client_returncode status;

        attrs_to_add.attr = attrName;
        attrs_to_add.value = (char*)&shard;
        attrs_to_add.value_sz = sizeof(int64_t);
        attrs_to_add.datatype = HYPERDATATYPE_INT64;

        int64_t op_id = cl.put(space, (const char*)&node, sizeof(int64_t), &attrs_to_add, 1, &status);
        if (op_id < 0) {
            WDEBUG << "\"put\" returned " << op_id << " with status " << status << std::endl;
            return;
        }

        int64_t loop_id = cl.loop(-1, &status);
        if (loop_id < 0) {
            WDEBUG << "put \"loop\" returned " << loop_id << " with status " << status << std::endl;
        }
    }

    // node program

    inline std::shared_ptr<node_prog::Node_State_Base>
    shard :: fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_id)
    {
        return prog_state.get_state(t, request_id, local_node_id);
    }

    inline void
    shard :: insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_id,
        std::shared_ptr<node_prog::Node_State_Base> toAdd)
    {
        prog_state.put_state(t, request_id, local_node_id, toAdd);
    }

    inline void
    shard :: add_done_request(uint64_t completed_req_id, node_prog::prog_type type)
    {
        prog_state.done_request(completed_req_id, type);
    }

    inline void
    shard :: add_done_requests(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests)
    {
        prog_state.done_requests(completed_requests);
    }

    inline bool
    shard :: check_done_request(uint64_t req_id)
    {
        bool done = prog_state.check_done_request(req_id);
        return done;
    }

}

#endif
