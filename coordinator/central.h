/*
 * ===============================================================
 *
 *    Description:  Coordinator class
 *
 *        Created:  10/27/2012 05:20:01 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CENTRAL__
#define __CENTRAL__

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <fstream>
#include <random> // testing
#include <chrono>
#include <thread>
#include <busybee_sta.h>
#include <po6/net/location.h>
#include <po6/threads/cond.h>

#include "common/meta_element.h"
#include "common/weaver_constants.h"
#include "common/vclock.h"
#include "threadpool/threadpool.h"

namespace coordinator
{
    class pending_req;
    class central;
}

void reachability_request_end(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request);
void reachability_request_propagate(coordinator::central *server, std::shared_ptr<coordinator::pending_req> request);

namespace coordinator
{
    class out_counter
    {
        // a counter of outstanding requests, a shared pointer to which
        // is passed to a del request when it returns. After counter reaches
        // zero, can commit the request.
        public:
            uint32_t cnt;
            uint64_t req_id;
            std::shared_ptr<out_counter> next;

        public:
            out_counter()
                : cnt(0)
            {
            }
    };
    // pending request state 
    class pending_req
    {
        public:
            // request init
            message::msg_type req_type;
            common::meta_element *elem1, *elem2;
            std::vector<common::property> edge_props;
            uint32_t key;
            size_t value;
            std::unique_ptr<po6::net::location> client;
            // permanent deletion counter
            std::shared_ptr<out_counter> out_count;
            // request process
            uint64_t req_id;
            uint64_t clock1, clock2;
            int shard_id;
            std::vector<std::shared_ptr<pending_req>> dependent_traversals;
            std::unique_ptr<std::vector<size_t>> src_node;
            std::unique_ptr<std::vector<uint64_t>> vector_clock;
            std::vector<uint64_t> ignore_cache;
            std::shared_ptr<pending_req> del_request;
            // used for dijkstra requests
            bool is_widest;
            std::unique_ptr<std::vector<size_t>> path;
            // request reply
            bool done;
            size_t node_handle;
            uint64_t edge_handle;
            bool reachable;
            size_t clustering_numerator;
            size_t clustering_denominator;
            size_t cost;
            uint64_t cached_req_id;
            std::unique_ptr<std::vector<uint64_t>> cached_req_ids;
            
        /*
        pending_req(message::msg_type type, common::meta_element *el1, common::meta_element *el2, 
            std::unique_ptr<po6::net::location> cloc, uint32_t k=0, size_t v=0)
            : req_type(type)
            , elem1(el1)
            , elem2(el2)
            , key(k)
            , value(v)
            , client(std::move(cloc))
            , done(false)
            , cached_req_id(0)
        {
        }

        pending_req(message::msg_type type, common::meta_element *el1, common::meta_element *el2, 
            std::shared_ptr<std::vector<common::property>> eprops, std::unique_ptr<po6::net::location> cloc)
            : req_type(type)
            , elem1(el1)
            , elem2(el2)
            , edge_props(eprops)
            , client(std::move(cloc))
            , done(false)
            , cached_req_id(0)
        {
        }
        */

        pending_req(message::msg_type type)
            : req_type(type)
            , done(false)
            , cached_req_id(0)
            {
            }
    };

    class central
    {
        public:
            central();

        public:
            uint64_t request_id;
            thread::pool thread_pool;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> myrecloc;
            std::shared_ptr<po6::net::location> client_send_loc, client_rec_loc;
            // messaging
            busybee_sta bb;
            busybee_sta rec_bb;
            busybee_sta client_send_bb;
            busybee_sta client_rec_bb;
            po6::threads::mutex bb_mutex;
            po6::threads::mutex client_bb_mutex;
            // graph state
            int port_ctr;
            std::vector<std::shared_ptr<po6::net::location>> shards;
            uint32_t num_shards;
            std::unordered_map<uint64_t, common::meta_element*> nodes;
            std::vector<common::meta_element*> edges;
            vclock::vector vc;
            // big mutex
            po6::threads::mutex update_mutex;
            // caching
            std::vector<std::shared_ptr<pending_req>> pending_delete_requests;
            // TODO clean-up of old deleted cache ids
            std::unordered_map<uint64_t, std::shared_ptr<pending_req>> pending;
            std::unique_ptr<std::unordered_set<uint64_t>> bad_cache_ids;
            std::unique_ptr<std::unordered_set<uint64_t>> good_cache_ids;
            std::unique_ptr<std::unordered_set<uint64_t>> transient_bad_cache_ids;
            // permanent deletion
            std::shared_ptr<out_counter> first_del; // head
            std::shared_ptr<out_counter> last_del; // tail
            // daemon
            uint32_t cache_acks;
            // testing
            std::default_random_engine generator;
            std::uniform_real_distribution<double> dist;

        public:
            void add_node(common::meta_element *n, uint64_t index);
            void add_edge(common::meta_element *e);
            void add_pending_del_req(std::shared_ptr<pending_req> request);
            std::shared_ptr<pending_req> get_last_del_req(std::shared_ptr<pending_req> request);
            //bool insert_del_wait(size_t del_req_id, size_t wait_req_id);
            bool still_pending_del_req(uint64_t req_id);
            void add_deleted_cache(std::shared_ptr<pending_req> request, std::vector<uint64_t> &cached_ids);
            void add_deleted_cache(uint64_t req_ids, std::vector<uint64_t> &cached_ids);
            bool is_deleted_cache_id(uint64_t id);
            void add_good_cache_id(uint64_t id);
            void add_bad_cache_id(uint64_t id);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc,
                std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::shared_ptr<po6::net::location> loc,
                std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int shard_id, std::auto_ptr<e::buffer> buf);
            busybee_returncode client_send(po6::net::location loc,
                std::auto_ptr<e::buffer> buf);
            busybee_returncode flaky_send(int loc, std::auto_ptr<e::buffer> buf, bool delay);
    };
    
    inline
    central :: central()
        : request_id(1)
        , thread_pool(NUM_THREADS)
        , myloc(new po6::net::location(COORD_IPADDR, COORD_PORT))
        , myrecloc(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , client_send_loc(new po6::net::location(COORD_IPADDR, COORD_CLIENT_SEND_PORT))
        , client_rec_loc(new po6::net::location(COORD_IPADDR, COORD_CLIENT_REC_PORT))
        , bb(myloc->address, myloc->port, 0)
        , rec_bb(myrecloc->address, myrecloc->port, 0)
        , client_send_bb(client_send_loc->address, client_send_loc->port, 0)
        , client_rec_bb(client_rec_loc->address, client_rec_loc->port, 0)
        , port_ctr(0)
        , bad_cache_ids(new std::unordered_set<uint64_t>())
        , good_cache_ids(new std::unordered_set<uint64_t>())
        , transient_bad_cache_ids(new std::unordered_set<uint64_t>())
        , first_del(new out_counter())
        , last_del(first_del)
        , cache_acks(0)
        , generator((unsigned)42) // fixed seed for deterministic random numbers
        , dist(0.0, 1.0)
    {
        // initialize array of shard server locations
        std::ifstream file(SHARDS_DESC_FILE);
        std::string ipaddr;
        int port;
        if (file != NULL) {
            while (file >> ipaddr >> port) {
                auto new_shard = std::make_shared<po6::net::location>(ipaddr.c_str(), port);
                shards.push_back(new_shard);
            }
        } else {
            std::cerr << "File " << SHARDS_DESC_FILE << " not found.\n";
        }
        file.close();
        num_shards = NUM_SHARDS;
    }

    inline void
    central :: add_node(common::meta_element *n, uint64_t index)
    {
        nodes.emplace(index, n);
    }

    inline void
    central :: add_edge(common::meta_element *e)
    {
        edges.push_back(e);
    }

    // caution: assuming we hold update_mutex
    inline void
    central :: add_pending_del_req(std::shared_ptr<pending_req> request)
    {
        pending_delete_requests.emplace_back(request);
        last_del->req_id = request->req_id;
        last_del->next.reset(new out_counter());
        last_del = last_del->next;
    }

    // caution: assuming we hold update_mutex
    inline std::shared_ptr<pending_req>
    central :: get_last_del_req(std::shared_ptr<pending_req> request)
    {
        std::shared_ptr<pending_req> ret;
        if (!pending_delete_requests.empty()) {
            ret = pending_delete_requests[pending_delete_requests.size()-1];
            ret->dependent_traversals.emplace_back(request);
        }
        return ret;
    }

    // record all the invalid cached req ids
    // also update pending_delete_requests
    // caution: assuming we hold update_mutex
    inline void
    central :: add_deleted_cache(std::shared_ptr<pending_req> request, std::vector<uint64_t> &cached_ids)
    {
        std::vector<std::shared_ptr<pending_req>>::iterator pend_iter;
        for (uint64_t del_iter: cached_ids) {
#ifdef DEBUG
            std::cout << "Inserting bad cache " << del_iter << std::endl;
#endif
            bad_cache_ids->insert(del_iter);
        }
        for (pend_iter = pending_delete_requests.begin(); pend_iter != pending_delete_requests.end(); pend_iter++) {
            if ((**pend_iter).req_id == request->req_id) {
                break;
            }
        }
        assert(pend_iter != pending_delete_requests.end());
        for (auto &dep_req: (**pend_iter).dependent_traversals) {
            if (dep_req->done) {
                reachability_request_end(this, dep_req); // TODO this is bad, should be processed by different threads.
            }
        }
        pending_delete_requests.erase(pend_iter);
#ifdef DEBUG
        std::cout << "Bad cache ids:\n";
        for (auto &it: *bad_cache_ids) {
            std::cout << it << " ";
        }
        std::cout << std::endl;
#endif
    }

    inline bool
    central :: is_deleted_cache_id(uint64_t id)
    {
#ifdef DEBUG
        std::cout << "Bad cache ids:\n";
        for (auto &it: *bad_cache_ids) {
            std::cout << it << " ";
        }
        std::cout << std::endl;
        std::cout << "Transient Bad cache ids:\n";
        for (auto &it: *transient_bad_cache_ids) {
            std::cout << it << " ";
        }
        std::cout << std::endl;
#endif
        return ((bad_cache_ids->find(id) != bad_cache_ids->end()) || (transient_bad_cache_ids->find(id) != transient_bad_cache_ids->end()));
    }

    inline void
    central :: add_good_cache_id(uint64_t id)
    {
        if (bad_cache_ids->find(id) != bad_cache_ids->end()) {
            good_cache_ids->insert(id);
        }
    }

    inline void
    central :: add_bad_cache_id(uint64_t id)
    {
        bad_cache_ids->insert(id);
        good_cache_ids->erase(id);
    }

    inline busybee_returncode
    central :: send(po6::net::location loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error: " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }

    inline busybee_returncode
    central :: send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error: " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }

    inline busybee_returncode
    central :: send(std::shared_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error: " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }

    inline busybee_returncode
    central :: send(int shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(*shards[shard_id], buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error: " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }
    
    inline busybee_returncode
    central :: client_send(po6::net::location loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        client_bb_mutex.lock();
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error " << ret << std::endl;
        }
        client_bb_mutex.unlock();
        return ret;
    }
    
    inline busybee_returncode
    central :: flaky_send(int loc, std::auto_ptr<e::buffer> buf, bool delay)
    {
        busybee_returncode ret;
        // 50% messages delayed
        if (dist(generator) <= 0.5 && delay) {
            std::chrono::seconds duration(1);
            std::this_thread::sleep_for(duration);
        }
        bb_mutex.lock();
        if ((ret = bb.send(*shards[loc], buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "message sending error " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }
}

#endif // __CENTRAL__
