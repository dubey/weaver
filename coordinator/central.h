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
#include <random> // XXX for testing
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
    // used to wait on pending update/reachability requests
    class pending_req
    {
        public:
            void *addr;
            bool reachable;
            po6::threads::mutex mutex;
            size_t clustering_numerator;
            size_t clustering_denominator;
            bool waiting;
            po6::threads::cond reply;
            po6::threads::cond del_reply;
            size_t cached_req_id;
            std::unique_ptr<std::vector<size_t>> cached_req_ids;
            bool deleted;
            
        pending_req(po6::threads::mutex *mtx)
            : addr(NULL)
            , waiting(true)
            , reply(&mutex)
            , del_reply(mtx)
            , cached_req_id(0)
            , deleted(false)
        {
        }
    };

    class central
    {
        public:
            central();

        public:
            size_t request_id;
            thread::pool thread_pool;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> myrecloc;
            std::shared_ptr<po6::net::location> client_send_loc, client_rec_loc;
            busybee_sta bb;
            busybee_sta rec_bb;
            busybee_sta client_send_bb;
            busybee_sta client_rec_bb;
            po6::threads::mutex bb_mutex;
            po6::threads::mutex client_bb_mutex;
            int port_ctr;
            std::vector<std::shared_ptr<po6::net::location>> shards;
            int num_shards;
            std::unordered_map<size_t, common::meta_element *> nodes;
            std::vector<common::meta_element *> edges;
            vclock::vector vc;
            po6::threads::mutex update_mutex;
            std::vector<std::pair<size_t, std::vector<size_t>>> pending_delete_requests;
            // TODO clean-up of old deleted cache ids
            std::unordered_map<size_t, pending_req *> pending;
            std::unique_ptr<std::unordered_set<size_t>> bad_cache_ids;
            std::unique_ptr<std::unordered_set<size_t>> good_cache_ids;
            std::unique_ptr<std::unordered_set<size_t>> transient_bad_cache_ids;
            int cache_acks;
            po6::threads::cond cache_cond;
            std::default_random_engine generator;
            std::uniform_real_distribution<double> dist;

        public:
            void add_node(common::meta_element *n);
            void add_edge(common::meta_element *e);
            void add_pending_del_req(size_t req_id);
            size_t get_last_del_req(size_t wait_req_id);
            //bool insert_del_wait(size_t del_req_id, size_t wait_req_id);
            bool still_pending_del_req(size_t req_id);
            void add_deleted_cache(size_t req_ids, std::vector<size_t> &cached_ids);
            bool is_deleted_cache_id(size_t id);
            void add_good_cache_id(size_t id);
            void add_bad_cache_id(size_t id);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
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
        , bad_cache_ids(new std::unordered_set<size_t>())
        , good_cache_ids(new std::unordered_set<size_t>())
        , transient_bad_cache_ids(new std::unordered_set<size_t>())
        , cache_acks(0)
        , cache_cond(&update_mutex)
        , generator((unsigned)42) // fixed seed for deterministic random numbers
        , dist(0.0, 1.0)
    {
        // initialize array of shard server locations
        std::ifstream file(SHARDS_DESC_FILE);
        std::string ipaddr;
        int port;
        if (file != NULL) {
            while (file >> ipaddr >> port)
            {
                auto new_shard = std::make_shared<po6::net::location>(ipaddr.c_str(), port);
                shards.push_back(new_shard);
            }
        } else {
            std::cerr << "File " << SHARDS_DESC_FILE << " not found.\n";
        }
        file.close();
        num_shards = shards.size();
    }

    inline void
    central :: add_node(common::meta_element *n, size_t index)
    {
        nodes.emplace(index, n);
    }

    inline void
    central :: add_edge(common::meta_element *e)
    {
        edges.push_back(e);
    }

    inline void
    central :: add_pending_del_req(size_t req_id)
    {
        std::vector<size_t> empty;
        pending_delete_requests.push_back(std::make_pair(req_id, empty));
    }

    inline size_t
    central :: get_last_del_req(size_t wait_req_id)
    {
        if (pending_delete_requests.empty()) {
            return 0;
        } else {
            pending_delete_requests[pending_delete_requests.size()-1].second.push_back(wait_req_id);
            return pending_delete_requests[pending_delete_requests.size()-1].first;
        }
    }

    /*
    inline bool
    central :: insert_del_wait(size_t del_req_id, size_t wait_req_id)
    {
        for(std::vector<T>::reverse_iterator it = pending_delete_requests.rbegin(); 
            it != pending_delete_requests.rend(); ++it) 
        {
            if (it->first == del_req_id) {
                it->second.push_back(wait_req_id);
                return true;
            }
        }
        return false;
    }
    */

    inline bool
    central :: still_pending_del_req(size_t req_id)
    {
        std::vector<std::pair<size_t, std::vector<size_t>>>::iterator iter;
        if (req_id == 0) {
            return false;
        }
        for (iter = pending_delete_requests.begin(); iter != pending_delete_requests.end(); iter++)
        {
            if (iter->first > req_id) {
                return false;
            } else if (iter->first == req_id) {
                return true;
            }
        }
        return false;
    }

    // record all the invalid cached req ids
    // also update pending_delete_requests
    inline void
    central :: add_deleted_cache(size_t req_id, std::vector<size_t> &cached_ids)
    {
        std::vector<size_t>::iterator del_iter;
        pending_req *request;
        std::vector<std::pair<size_t, std::vector<size_t>>>::iterator pend_iter;
        for (del_iter = cached_ids.begin(); del_iter != cached_ids.end(); del_iter++)
        {
            bad_cache_ids->insert(*del_iter);
        }
        for (pend_iter = pending_delete_requests.begin(); pend_iter != pending_delete_requests.end(); pend_iter++)
        {
            if (pend_iter->first == req_id) {
                break;
            }
        }
        assert(pend_iter != pending_delete_requests.end());
        for (del_iter = pend_iter->second.begin(); del_iter != pend_iter->second.end(); del_iter++)
        {
            request = (pending_req *)(*del_iter);
            if (request->deleted) {
                delete request;
            } else {
                request->del_reply.signal();
            }
        }
        pending_delete_requests.erase(pend_iter);
    }

    inline bool
    central :: is_deleted_cache_id(size_t id)
    {
        return ((bad_cache_ids->find(id) != bad_cache_ids->end()) &&
            (transient_bad_cache_ids->find(id) != transient_bad_cache_ids->end()));
    }

    inline void
    central :: add_good_cache_id(size_t id)
    {
        if (bad_cache_ids->find(id) != bad_cache_ids->end())
        {
            good_cache_ids->insert(id);
        }
    }

    inline void
    central :: add_bad_cache_id(size_t id)
    {
        bad_cache_ids->insert(id);
        good_cache_ids->erase(id);
    }

    inline busybee_returncode
    central :: send(po6::net::location loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS)
        {
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
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS)
        {
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
        if ((ret = bb.send(*shards[shard_id], buf)) != BUSYBEE_SUCCESS)
        {
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
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        client_bb_mutex.unlock();
        return ret;
    }
    
    inline busybee_returncode
    central :: flaky_send(int loc, std::auto_ptr<e::buffer> buf, bool delay)
    {
        busybee_returncode ret;
        if (dist(generator) <= 0.5 && delay) // 50% messages delayed
        {
            std::chrono::seconds duration(1);
            std::this_thread::sleep_for(duration);
        }
        bb_mutex.lock();
        if ((ret = bb.send(*shards[loc], buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }
}

#endif // __CENTRAL__
