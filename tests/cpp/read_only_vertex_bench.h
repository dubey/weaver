/*
 * ===============================================================
 *    Description:  Read-only multi-client benchmark which only
 *                  reads 1 vertex per query.
 *
 *        Created:  04/07/2014 08:51:26 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <thread>
#include <chrono>
#include <random>
#include <po6/threads/mutex.h>
#include <busybee_utils.h>

#include "common/clock.h"
#include "client/weaver_client.h"

using cl::client;

void
exec_reads(std::vector<std::string> *reqs,
    client *cl,
    uint64_t num_clients,
    uint64_t *num_start,
    uint64_t *num_fin,
    uint64_t *done,
    po6::threads::cond *cond,
    uint64_t /*tid*/)
{
    cond->lock();
    *num_start = *num_start + 1;
    while (*num_start < num_clients) {
        cond->wait();
    }
    cond->broadcast();
    cond->unlock();

    node_prog::read_node_props_params rp;

    uint64_t cnt = 0;
    for (const std::string &h: *reqs) {
        std::vector<std::pair<std::string, node_prog::read_node_props_params>> args(1, std::make_pair(h, rp));
        cl->read_node_props_program(args);
        //if (++cnt % 100 == 0) {
        //    std::cout << "client " << tid << " completed " << cnt << std::endl;
        //}
        if (++cnt % 1000 == 0) {
            cond->lock();
            *done = *done + 1000;
            cond->unlock();
        }
    }

    cond->lock();
    *num_fin = *num_fin + 1;
    cond->signal();
    cond->unlock();
}

void
run_read_only_vertex_bench(uint64_t num_clients, uint64_t num_nodes, uint64_t num_requests)
{
    po6::net::ipaddr ip;
    busybee_discover(&ip);
    uint64_t pid = getpid();

    std::vector<client*> clients;
    std::vector<std::vector<std::string>> requests(num_clients, std::vector<std::string>());
    clients.reserve(num_clients);

    std::unordered_map<uint64_t, uint64_t> vt_count;

    for (uint64_t i = 0; i < num_clients; i++) {
        clients.emplace_back(new client("128.84.167.101", 2002, "/home/dubey/installs/etc/weaver.yaml"));
        requests[i].reserve(num_requests);

        uint64_t vt = clients.back()->get_vt_id();
        if (vt_count.find(vt) == vt_count.end()) {
            vt_count[vt] = 0;
        }
        vt_count[vt]++;
    }

    std::cout << "[vt counts]: ";
    for (const auto &p: vt_count) {
        std::cout << p.first << " " << p.second << "; ";
    }
    std::cout << std::endl;

    std::default_random_engine generator;
    std::uniform_int_distribution<uint64_t> distribution(0,num_nodes-1);
    for (uint64_t i = 0; i < num_requests; i++) {
        for (uint64_t j = 0; j < num_clients; j++) {
            requests[j].emplace_back(std::to_string(distribution(generator) % num_nodes));
        }
    }

    wclock::weaver_timer timer;
    po6::threads::mutex mtx;
    po6::threads::cond cond(&mtx);
    std::vector<std::thread*> threads;
    std::vector<uint64_t> done(num_clients, 0);
    threads.reserve(num_clients);
    uint64_t num_start = 0;
    uint64_t num_fin = 0;

    for (uint64_t i = 0; i < num_clients; i++) {
        threads.emplace_back(new std::thread(exec_reads, &requests[i], clients[i], num_clients, &num_start, &num_fin, &done[i], &cond, i));
    }

    cond.lock();
    while (num_start < num_clients) {
        cond.wait();
    }

    uint64_t start = timer.get_time_elapsed_millis();
    std::chrono::milliseconds duration(1000); // 1 sec

    while (num_fin < num_clients) {
        cond.unlock();
        std::this_thread::sleep_for(duration);
        cond.lock();

        uint64_t tot_done = 0;
        bool some_finished = false;
        if (some_finished) {
            std::cout << "threads not yet finished:";
        }
        for (uint64_t i = 0; i < done.size(); i++) {
            tot_done += done[i];
            if (done[i] == num_requests) {
                some_finished = true;
            } else if (some_finished) {
                std::cout << " " << i;
            }
        }
        if (some_finished) {
            std::cout << std::endl;
        }

        uint64_t cur = timer.get_time_elapsed_millis();
        uint64_t real = timer.get_real_time_millis();
        std::cout << "[progress@" << ip << pid << "@" << real << "=" << (tot_done * 1000 / (cur-start)) << std::endl;
    }
    cond.unlock();

    uint64_t end = timer.get_time_elapsed_millis();

    for (uint64_t i = 0; i < num_clients; i++) {
        threads[i]->join();
        delete threads[i];
        delete clients[i];
    }

    uint64_t ops = (num_requests*num_clients);
    float time = (end-start)/1000;
    float tput = ops / time;

    std::cout << "[throughput=] " << tput << " , time = " << time << std::endl;

    std::cout << ops << " =tot_requests" << std::endl;
}
