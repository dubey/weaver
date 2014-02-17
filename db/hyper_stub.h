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
            template <typename T> std::unique_ptr<e::buffer> prepare_buffer(const T &t);
            template <typename T> std::pair<std::unique_ptr<char>, uint64_t> prepare_buffer(const std::unordered_map<uint64_t, T> &map);
            std::pair<std::unique_ptr<char>, uint64_t> prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map);
            std::pair<std::unique_ptr<char>, uint64_t> prepare_buffer(const std::unordered_set<uint64_t> &set);
            void hyper_call_and_loop(hyper_func h, const char *space,
                uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs);
            void hypermap_call_and_loop(hyper_map_func h, const char *space,
                uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs);

        public:
            hyper_stub(uint64_t sid);
            void init();
            // XXX init from backup after failure
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

    inline
    hyper_stub :: hyper_stub(uint64_t sid)
        : shard_id(sid)
        , graph_attrs{"creat_time",
            "del_time",
            "properties",
            "out_edges",
            "in_nbrs"}
        , graph_dtypes{HYPERDATATYPE_STRING,
            HYPERDATATYPE_STRING,
            HYPERDATATYPE_STRING, // can change to map(int, string) to simulate vector with random access
            HYPERDATATYPE_MAP_INT64_STRING,
            HYPERDATATYPE_SET_INT64}
        , shard_attrs{"qts", "last_clocks"}
        , shard_dtypes{HYPERDATATYPE_MAP_INT64_INT64, HYPERDATATYPE_MAP_INT64_STRING}
        , cl(HYPERDEX_COORD_IPADDR, HYPERDEX_COORD_PORT)
    { }

    inline void
    hyper_stub :: init()
    {
        vc::vclock_t zero_clk(NUM_VTS, 0);
        std::unordered_map<uint64_t, uint64_t> qts_map;
        std::unordered_map<uint64_t, vc::vclock_t> last_clocks;
        
        for (uint64_t vt_id = 0; vt_id < NUM_VTS; vt_id++) {
            qts_map.emplace(vt_id, 0);
            last_clocks.emplace(vt_id, zero_clk);
        }
        auto qts = prepare_buffer(qts_map);
        auto last_clk = prepare_buffer(last_clocks);

        hyperdex_client_attribute *cl_attr = (hyperdex_client_attribute*)malloc(1 * sizeof(hyperdex_client_attribute));
        cl_attr[0].attr = shard_attrs[0];
        cl_attr[0].value = qts.first.get();
        cl_attr[0].value_sz = qts.second;
        cl_attr[0].datatype = shard_dtypes[0];
        cl_attr[0].attr = shard_attrs[1];
        cl_attr[0].value = last_clk.first.get();
        cl_attr[0].value_sz = last_clk.second;
        cl_attr[0].datatype = shard_dtypes[1];

        hyper_call_and_loop(&hyperdex::Client::put, shard_space, shard_id, cl_attr, 1);
        free(cl_attr);
    }

    // store the given t as a HYPERDATATYPE_STRING
    template <typename T>
    inline std::unique_ptr<e::buffer>
    hyper_stub :: prepare_buffer(const T &t)
    {
        uint64_t buf_sz = message::size(t);
        std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
        e::buffer::packer packer = buf->pack_at(0);
        message::pack_buffer(packer, t);
        return std::move(buf);
    }

    // store the given unordered_map as a HYPERDATATYPE_MAP_INT64_STRING
    template <typename T>
    inline std::pair<std::unique_ptr<char>, uint64_t>
    hyper_stub :: prepare_buffer(const std::unordered_map<uint64_t, T> &map)
    {
        uint64_t buf_sz = 0;
        std::set<uint64_t> sorted;
        std::vector<uint32_t> edge_sz;
        for (auto &p: map) {
            sorted.emplace(p.first);
            edge_sz.emplace_back(message::size(p.second));
            buf_sz += sizeof(p.first) // map key
                    + sizeof(uint32_t) // edge encoding sz
                    + edge_sz.back(); // edge encoding
        }

        char *buf = (char*)malloc(buf_sz);
        std::unique_ptr<char> ret_buf(buf);
        // now iterate in sorted order
        uint64_t i = 0;
        std::unique_ptr<e::buffer> temp_buf;
        for (uint64_t hndl: sorted) {
            buf = e::pack64le(hndl, buf);
            buf = e::pack32le(edge_sz[i], buf);
            temp_buf.reset(e::buffer::create(edge_sz[i]));
            e::buffer::packer packer = temp_buf->pack_at(0);
            message::pack_buffer(packer, map.at(hndl));
            memmove(buf, temp_buf->data(), edge_sz[i]);
            buf += edge_sz[i];
            i++;
        }

        return std::make_pair(std::move(ret_buf), buf_sz);
    }

    // store the given unordered_map<int, int> as a HYPERDATATYPE_MAP_INT64_INT64
    inline std::pair<std::unique_ptr<char>, uint64_t>
    hyper_stub :: prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map)
    {
        size_t buf_sz = NUM_VTS * (sizeof(int64_t) + sizeof(int64_t));
        char *buf = (char*)malloc(buf_sz);
        std::unique_ptr<char> ret_buf(buf);
        
        for (auto &p: map) {
            buf = e::pack64le(p.first, buf);
            buf = e::pack64le(p.second, buf);
        }

        return std::make_pair(std::move(ret_buf), buf_sz);
    }

    // store the given unordered_set as a HYPERDATATYPE_SET_INT64
    inline std::pair<std::unique_ptr<char>, uint64_t>
    hyper_stub :: prepare_buffer(const std::unordered_set<uint64_t> &set)
    {
        uint64_t buf_sz = sizeof(uint64_t) * set.size();
        std::set<uint64_t> sorted;
        for (uint64_t x: set) {
            sorted.emplace(x);
        }

        char *buf = (char*)malloc(buf_sz);
        std::unique_ptr<char> ret_buf(buf);
        // now iterate in sorted order
        for (uint64_t x: sorted) {
            buf = e::pack64le(x, buf);
        }

        return std::make_pair(std::move(ret_buf), buf_sz);
    }

    // call hyperdex function h using key hndl, attributes cl_attr, and then loop for response
    inline void
    hyper_stub :: hyper_call_and_loop(hyper_func h, const char *space,
        uint64_t key, hyperdex_client_attribute *cl_attr, size_t num_attrs)
    {
        hyperdex_client_returncode status;
        std::unique_ptr<int64_t> key_buf(new int64_t(key));
        int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), cl_attr, num_attrs, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex function failed, op id = " << hdex_id << ", status = " << status << std::endl;
            return;
        }
        hdex_id = cl.loop(-1, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
        }
    }

    // call hyperdex map function h using key hndl, attributes cl_attr, and then loop for response
    inline void
    hyper_stub :: hypermap_call_and_loop(hyper_map_func h, const char *space,
        uint64_t key, hyperdex_client_map_attribute *map_attr, size_t num_attrs)
    {
        hyperdex_client_returncode status;
        std::unique_ptr<int64_t> key_buf(new int64_t(key));
        int64_t hdex_id = (cl.*h)(space, (const char*)key_buf.get(), sizeof(int64_t), map_attr, num_attrs, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex map function failed, op id = " << hdex_id << ", status = " << status << std::endl;
            return;
        }
        hdex_id = cl.loop(-1, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
        }
    }

    inline void
    hyper_stub :: put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map)
    {
        hyperdex_client_attribute *cl_attr = (hyperdex_client_attribute*)malloc(5 * sizeof(hyperdex_client_attribute));
        // create clock
        std::unique_ptr<e::buffer> creat_clk_buf = prepare_buffer(n.get_creat_time());
        cl_attr[0].attr = graph_attrs[0];
        cl_attr[0].value = (const char*)creat_clk_buf->data();
        cl_attr[0].value_sz = creat_clk_buf->size();
        cl_attr[0].datatype = graph_dtypes[0];
        // delete clock
        std::unique_ptr<e::buffer> del_clk_buf = prepare_buffer(n.get_del_time());
        cl_attr[1].attr = graph_attrs[1];
        cl_attr[1].value = (const char*)del_clk_buf->data();
        cl_attr[1].value_sz = del_clk_buf->size();
        cl_attr[1].datatype = graph_dtypes[1];
        // properties
        std::unique_ptr<e::buffer> props_buf = prepare_buffer(*n.get_props());
        cl_attr[2].attr = graph_attrs[2];
        cl_attr[2].value = (const char*)props_buf->data();
        cl_attr[2].value_sz = props_buf->size();
        cl_attr[2].datatype = graph_dtypes[2];
        // out edges
        auto out_edges = prepare_buffer<element::edge*>(n.out_edges);
        cl_attr[3].attr = graph_attrs[3];
        cl_attr[3].value = out_edges.first.get();
        cl_attr[3].value_sz = out_edges.second;
        cl_attr[3].datatype = graph_dtypes[3];
        // in nbrs
        auto in_nbrs = prepare_buffer(nbr_map);
        cl_attr[4].attr = graph_attrs[4];
        cl_attr[4].value = in_nbrs.first.get();
        cl_attr[4].value_sz = in_nbrs.second;
        cl_attr[4].datatype = graph_dtypes[4];

        hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.get_handle(), cl_attr, 5);
        free(cl_attr);
    }

    inline void
    hyper_stub :: update_creat_time(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> creat_clk_buf = prepare_buffer(n.get_creat_time());
        cl_attr.attr = graph_attrs[0];
        cl_attr.value = (const char*)creat_clk_buf->data();
        cl_attr.value_sz = creat_clk_buf->size();
        cl_attr.datatype = graph_dtypes[0];

        hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: update_del_time(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> del_clk_buf = prepare_buffer(n.get_del_time());
        cl_attr.attr = graph_attrs[1];
        cl_attr.value = (const char*)del_clk_buf->data();
        cl_attr.value_sz = del_clk_buf->size();
        cl_attr.datatype = graph_dtypes[1];

        hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: update_properties(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> props_buf = prepare_buffer(*n.get_props());
        cl_attr.attr = graph_attrs[2];
        cl_attr.value = (const char*)props_buf->data();
        cl_attr.value_sz = props_buf->size();
        cl_attr.datatype = graph_dtypes[2];

        hyper_call_and_loop(&hyperdex::Client::put, graph_space, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: add_out_edge(element::node &n, element::edge *e)
    {
        hyperdex_client_map_attribute map_attr;
        std::unique_ptr<int64_t> key_buf(new int64_t(e->get_handle()));
        std::unique_ptr<e::buffer> val_buf = prepare_buffer(e);
        map_attr.attr = graph_attrs[3];
        map_attr.map_key = (const char*)key_buf.get();
        map_attr.map_key_sz = sizeof(int64_t);
        map_attr.map_key_datatype = HYPERDATATYPE_INT64;
        map_attr.value = (const char*)val_buf->data();
        map_attr.value_sz = val_buf->size();
        map_attr.value_datatype = HYPERDATATYPE_STRING;

        hypermap_call_and_loop(&hyperdex::Client::map_add, graph_space, n.get_handle(), &map_attr, 1);
    }

    inline void
    hyper_stub :: remove_out_edge(element::node &n, element::edge *e)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<int64_t> key_buf(new int64_t(e->get_handle()));
        cl_attr.attr = graph_attrs[3];
        cl_attr.value = (const char*)key_buf.get();
        cl_attr.value_sz = sizeof(int64_t);
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::map_remove, graph_space, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: add_in_nbr(uint64_t n_hndl, uint64_t nbr)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<int64_t> nbr_buf(new int64_t(nbr));
        cl_attr.attr = graph_attrs[4];
        cl_attr.value = (const char*)nbr_buf.get();
        cl_attr.value_sz = sizeof(int64_t);
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::set_add, graph_space, n_hndl, &cl_attr, 1);
    }

    inline void
    hyper_stub :: remove_in_nbr(uint64_t n_hndl, uint64_t nbr)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<int64_t> nbr_buf(new int64_t(nbr));
        cl_attr.attr = graph_attrs[4];
        cl_attr.value = (const char*)nbr_buf.get();
        cl_attr.value_sz = sizeof(int64_t);
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::set_remove, graph_space, n_hndl, &cl_attr, 1);
    }

    inline void
    hyper_stub :: increment_qts(uint64_t vt_id, uint64_t incr)
    {
        hyperdex_client_map_attribute map_attr;
        std::unique_ptr<int64_t> key_buf(new int64_t(vt_id));
        std::unique_ptr<int64_t> val_buf(new int64_t(incr));
        map_attr.attr = shard_attrs[0];
        map_attr.map_key = (const char*)key_buf.get();
        map_attr.map_key_sz = sizeof(int64_t);
        map_attr.map_key_datatype = HYPERDATATYPE_INT64;
        map_attr.value = (const char*)val_buf.get();
        map_attr.value_sz = sizeof(int64_t);
        map_attr.value_datatype = HYPERDATATYPE_INT64;

        hypermap_call_and_loop(&hyperdex::Client::map_atomic_add, shard_space, shard_id, &map_attr, 1);
    }

    inline void
    hyper_stub :: update_last_clocks(uint64_t vt_id, vc::vclock_t &vclk)
    {
        hyperdex_client_map_attribute map_attr;
        std::unique_ptr<int64_t> key_buf(new int64_t(vt_id));
        std::unique_ptr<e::buffer> clk_buf = prepare_buffer(vclk);
        map_attr.attr = shard_attrs[1];
        map_attr.map_key = (const char*)key_buf.get();
        map_attr.map_key_sz = sizeof(int64_t);
        map_attr.map_key_datatype = HYPERDATATYPE_INT64;
        map_attr.value = (const char*)clk_buf->data();
        map_attr.value_sz = clk_buf->size();
        map_attr.value_datatype = HYPERDATATYPE_STRING;

        hypermap_call_and_loop(&hyperdex::Client::map_add, shard_space, shard_id, &map_attr, 1);
    }
}

#endif
