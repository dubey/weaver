/*
 * ===============================================================
 *    Description:  Hyperdex client stub for shard state.
 *
 *        Created:  2014-02-02 16:54:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __HDEX_SHARD_STUB__
#define __HDEX_SHARD_STUB__

#include <e/endian.h>
#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>

#include "common/weaver_constants.h"
#include "common/message_graph_elem.h"

namespace db
{
    class hyper_stub
    {
        private:
            const uint64_t shard_id;
            const char *graph_space = "weaver_graph_data";
            const char *graph_attrs[5];
            const enum hyperdatatype graph_dtypes[5];
            const char *shard_space = "weaver_shard_data";
            const char *shard_attrs[2];
            const enum hyperdatatype shard_dtypes[2];
            hyperdex::Client cl;
            typedef int64_t (hyperdex::Client::*hyper_func)(const char*,
                const char*,
                size_t,
                const struct hyperdex_client_attribute*,
                size_t,
                hyperdex_client_returncode*);
            typedef int64_t (hyperdex::Client::*hyper_map_func)(const char*,
                const char*,
                size_t,
                const struct hyperdex_client_map_attribute*,
                size_t,
                hyperdex_client_returncode*);

        private:
            template <typename T> void prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf);
            template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, T &t);
            template <typename T> void prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<char> &buf, uint64_t &buf_sz);
            template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map);
            void prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<char> &buf, uint64_t &buf_sz);
            void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map);
            void prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<char> &buf, uint64_t &buf_sz);
            void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set);
            void hyper_call_and_loop(hyper_func h, const char *space, uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs);
            void hypermap_call_and_loop(hyper_map_func h, const char *space, uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs);
            void hyper_get_and_loop(const char *space, uint64_t key, const hyperdex_client_attribute **cl_attr, size_t *num_attrs);

        public:
            hyper_stub(uint64_t sid);
            void init();
            void restore_backup(std::unordered_map<uint64_t, uint64_t> &qts_map,
                std::unordered_map<uint64_t, vc::vclock_t> &last_clocks);
            // graph updates
            void put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map);
            element::node* get_node(uint64_t node);
            void update_creat_time(element::node &n);
            void update_del_time(element::node &n);
            void update_properties(element::node &n);
            void add_out_edge(element::node &n, element::edge *e);
            void remove_out_edge(element::node &n, element::edge *e);
            void add_in_nbr(uint64_t n_hndl, uint64_t nbr);
            void remove_in_nbr(uint64_t n_hndl, uint64_t nbr);
            // shard updates
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void update_last_clocks(uint64_t vt_id, vc::vclock_t &vclk);
    };
}

#endif
