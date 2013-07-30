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

#ifndef __SHARD_SERVER__
#define __SHARD_SERVER__

namespace db
{
    // state for a deleted graph element which is yet to be permanently deleted
    struct perm_del
    {
        enum message::msg_type type;
        vclock::timestamp tdel;
        union {
            struct
            {
                uint64_t node_handle;
            } del_node;
            struct
            {
                uint64_t node_handle;
                uint64_t edge_handle;
            } del_edge;
            struct
            {
                uint64_t node_handle;
                uint64_t edge_handle;
                uint32_t key;
            } del_edge_prop;
        } request;

        inline
        perm_del(enum message::msg_type t, vclock::timestamp &td, uint64_t node)
            : type(t)
            , tdel(td)
        {
            request.del_node.node_handle = node;
        }

        inline
        perm_del(enum message::msg_type t, vclock::timestamp &td, uint64_t node, uint64_t edge)
            : type(t)
            , tdel(td)
        {
            request.del_edge.node_handle = node;
            request.del_edge.edge_handle = edge;
        }
        
        inline
        perm_del(enum message::msg_type t, vclock::timestamp &td, uint64_t node, uint64_t edge, uint32_t k)
            : type(t)
            , tdel(td)
        {
            request.del_edge_prop.node_handle = node;
            request.del_edge_prop.edge_handle = edge;
            request.del_edge_prop.key = k;
        }
    };

    // graph partition state and associated data structures
    class shard
    {
            // Mutexes
        private:
            po6::threads::mutex update_mutex, edge_map_mutex, migration_mutex;

            // Consistency
        private:
            vclock::nvc my_clock;
        public:
            element::node* acquire_node(uint64_t node_handle);
            void release_node(element::node *n);

            // Graph state
            uint64_t shard_id;
        private:
            std::unordered_map<uint64_t, element::node*> nodes; // node handle -> ptr to node object
            std::unordered_map<uint64_t, uint64_t> edges; // edge handle -> node handle
            db::thread::pool thread_pool;
        public:
            void create_node(uint64_t node_handle, vclock::timestamp &ts, bool migrate);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_node(uint64_t node_handle, vclock::timestamp &ts);
            uint64_t create_edge(uint64_t edge_handle, uint64_t local_node, vclock::timestamp &local_time,
                    uint64_t remote_node, vclock::timestamp &remote_time);
            uint64_t create_reverse_edge(uint64_t edge_handle, uint64_t local_node, vclock::timestamp &local_time,
                    uint64_t remote_node, uint64_t remote_loc);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_edge(uint64_t edge_handle, vclock::timestamp &ts);
            uint64_t add_edge_property(uint64_t edge_handle, common::property &prop);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_all_edge_property(uint64_t edge_handle,
                    uint32_t key, vclock::timestamp &ts);
            void permanent_delete(uint64_t req_id, uint64_t migr_del_id);
            void permanent_node_delete(element::node *n);
            void permanent_edge_delete(uint64_t edge_handle);
            uint64_t get_node_count();

            // Permanent deletion
        private:
            std::unordered_set<perm_del> pending_deletes;
    };

    // Consistency methods

    // find the node corresponding to given handle
    // lock and return the node
    // return NULL if node does not exist (possibly permanently deleted)
    inline element::node*
    graph :: acquire_node(uint64_t node_handle)
    {
        element::node *n = NULL;
        update_mutex.lock();
        if (nodes.find(node_handle) != nodes.end()) {
            n = nodes.at(node_handle);
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

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    graph :: release_node(element::node *n)
    {
        update_mutex.lock();
        n->in_use = false;
        if (n->waiters > 0) {
            n->cv.signal();
            update_mutex.unlock();
        } else if (n->permanently_deleted) {
            uint64_t node_handle = n->get_creat_time();
            nodes.erase(node_handle);
            cur_node_count--;
            update_mutex.unlock();
            msg_count_mutex.lock();
            agg_msg_count.erase(node_handle);
            msg_count_mutex.unlock();
            permanent_node_delete(n);
        } else {
            update_mutex.unlock();
        }
    }


    // Graph state update methods

    inline void
    shard ::  create_node(uint64_t node_handle, vclock::timestamp &ts, bool migrate)
    {
        element::node *new_node = new element::node(ts, &update_mutex);
        update_mutex.lock();
        if (!nodes.emplace(node_handle, new_node).second) {
            DEBUG << "node already exists in node map!" << std::endl;
        }
        cur_node_count++;
        update_mutex.unlock();
        if (migrate) {
            migration_mutex.lock();
            migr_node = node_handle;
            migration_mutex.unlock();
        } else {
            new_node->state = element::node::mode::STABLE;
            for (uint64_t i = 0; i < NUM_SHARDS; i++) {
                new_node->prev_locs.emplace_back(0);
            }
            new_node->prev_locs.at(myid-1) = 1;
        }
        release_node(new_node);
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    shard :: delete_node(uint64_t node_handle, vclock::timestamp &tdel)
    {
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        element::node *n = acquire_node(node_handle);
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            n->update_del_time(tdel);
            n->updated = true;
            ret.first = 0;
        }
        // XXX check logic for caching
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            pending_deletes.emplace(std::unique_ptr<perm_del>(new perm_del(message::NODE_DELETE_REQ,
                    tdel, node_handle)));
            deletion_mutex.unlock();
        }
        return ret;
    }

    inline uint64_t
    shard :: create_edge(uint64_t edge_handle, uint64_t local_node, vclock::timestamp &local_time,
            uint64_t remote_node, vclock::timestamp &remote_time)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state == element::node::mode::IN_TRANSIT) {
            release_node(n);
        } else {
            element::edge *new_edge = new element::edge(local_time, remote_loc, remote_node);
            n->add_edge(new_edge, true);
            n->updated = true;
            release_node(n);
            edge_map_mutex.lock();
            edges.emplace(edge_handle, local_node);
            edge_map_mutex.unlock();
            message::message msg;
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE,
                edge_handle, remote_node, remote_time, local_node, shard_id);
            send(remote_loc, msg.buf);
            ret = 0;
        }
        DEBUG << "creating edge, req id = " << time << std::endl;
        return ret;
    }

    inline uint64_t
    shard :: create_reverse_edge(uint64_t edge_handle, uint64_t local_node, vclock::timestamp &local_time,
            uint64_t remote_node, uint64_t remote_loc)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *new_edge = new element::edge(local_time, remote_loc, remote_node);
            n->add_edge(new_edge, false);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        if (ret == 0) {
            edge_map_mutex.lock();
            edges.emplace(edge_handle, local_node);
            edge_map_mutex.unlock();
        }
        return ret;
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    shard :: delete_edge(uint64_t edge_handle, vclock::timestamp &tdel)
    {
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        edge_map_mutex.lock();
        uint64_t node_handle = edges.at(edge_handle);
        edge_map_mutex.unlock();
        element::node *n = acquire_node(node_handle);
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(tdel);
            n->updated = true;
            n->dependent_del++;
            ret.first = 0;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            pending_deletes.emplace(std::unique_ptr<perm_del>(new perm_del(message::EDGE_DELETE_REQ,
                    tdel, node_handle, edge_handle)));
            deletion_mutex.unlock();
        }
        return ret;
    }

    inline uint64_t
    shard :: add_edge_property(uint64_t edge_handle, common::property &prop)
    {
        edge_map_mutex.lock();
        uint64_t node_handle = edges.at(edge_handle);
        edge_map_mutex.unlock();
        element::node *n = acquire_node(node_handle);
        element::edge *e = n->out_edges.at(edge_handle);
        uint64_t ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            e->add_property(prop);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        return ret;
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    shard :: delete_all_edge_property(uint64_t edge_handle, uint32_t key, vclock::timestamp &tdel)
    {
        edge_map_mutex.lock();
        uint64_t node_handle = edges.at(edge_handle);
        edge_map_mutex.unlock();
        element::node *n = acquire_node(node_handle);
        element::edge *e = n->out_edges.at(edge_handle);
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            e->delete_property(key, tdel);
            n->updated = true;
            n->dependent_del++;
            ret.first = 0;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            pending_deletes.emplace(std::unique_ptr<perm_del>(new perm_del(message::EDGE_DELETE_PROP,
                    tdel, node_handle, edge_handle, key)));
            deletion_mutex.unlock();
        }
        return ret;
    }

}

#endif
