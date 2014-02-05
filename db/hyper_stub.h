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

#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>

#include "common/weaver_constants.h"
#include "common/message_graph_elem.h"

namespace db
{
    class hyper_stub
    {
        private:
            const char *space = "weaver_shard_data";
            const char *attrs[5];
            const enum hyperdatatype dtypes[5];
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
            std::unique_ptr<e::buffer> prepare_buffer(const std::unordered_map<uint64_t, element::edge*> &map);
            std::unique_ptr<e::buffer> prepare_buffer(const std::unordered_set<uint64_t> &set);
            void hyper_call_and_loop(hyper_func h, uint64_t hndl, hyperdex_client_attribute *cl_attr, size_t num_attrs);
            void hypermap_call_and_loop(hyper_map_func h, uint64_t hndl, hyperdex_client_map_attribute *map_attr, size_t num_attrs);

        public:
            hyper_stub();
            void put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map);
            element::node* get_node(uint64_t node);
            void update_creat_time(element::node &n);
            void update_del_time(element::node &n);
            void update_properties(element::node &n);
            void add_out_edge(element::node &n, element::edge *e);
            void remove_out_edge(element::node &n, element::edge *e);
            void add_in_nbr(uint64_t n_hndl, uint64_t nbr);
            void remove_in_nbr(uint64_t n_hndl, uint64_t nbr);
    };

    inline
    hyper_stub :: hyper_stub()
        : attrs{"creat_time",
            "del_time",
            "properties",
            "out_edges",
            "in_nbrs"}
        , dtypes {HYPERDATATYPE_STRING,
            HYPERDATATYPE_STRING,
            HYPERDATATYPE_STRING,
            HYPERDATATYPE_MAP_INT64_STRING,
            HYPERDATATYPE_SET_INT64}
        , cl(HYPERDEX_COORD_IPADDR, HYPERDEX_COORD_PORT)
    { }

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

    inline std::unique_ptr<e::buffer>
    hyper_stub :: prepare_buffer(const std::unordered_map<uint64_t, element::edge*> &map)
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
        std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
        e::buffer::packer packer = buf->pack_at(0);
        // now iterate in sorted order
        uint64_t i = 0;
        for (uint64_t hndl: sorted) {
            message::pack_buffer(packer, hndl);
            message::pack_buffer(packer, edge_sz[i++]);
            message::pack_buffer(packer, map.at(hndl));
        }
        return std::move(buf);
    }

    inline std::unique_ptr<e::buffer>
    hyper_stub :: prepare_buffer(const std::unordered_set<uint64_t> &set)
    {
        uint64_t buf_sz = sizeof(uint64_t) * set.size();
        std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
        e::buffer::packer packer = buf->pack_at(0);
        std::set<uint64_t> sorted;
        for (uint64_t x: set) {
            sorted.emplace(x);
        }
        // now iterate in sorted order
        for (uint64_t x: sorted) {
            message::pack_buffer(packer, x);
        }
        return std::move(buf);
    }

    // call hyperdex function h using key hndl, attributes cl_attr, and then loop for response
    inline void
    hyper_stub :: hyper_call_and_loop(hyper_func h, uint64_t hndl, hyperdex_client_attribute *cl_attr, size_t num_attrs)
    {
        hyperdex_client_returncode status;
        int64_t hdex_id = (cl.*h)(space, (const char*)&hndl, sizeof(int64_t), cl_attr, num_attrs, &status);
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
    hyper_stub :: hypermap_call_and_loop(hyper_map_func h, uint64_t hndl, hyperdex_client_map_attribute *map_attr, size_t num_attrs)
    {
        hyperdex_client_returncode status;
        int64_t hdex_id = (cl.*h)(space, (const char*)&hndl, sizeof(int64_t), map_attr, num_attrs, &status);
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
        cl_attr[0].attr = attrs[0];
        cl_attr[0].value = (const char*)creat_clk_buf->data();
        cl_attr[0].value_sz = creat_clk_buf->size();
        cl_attr[0].datatype = dtypes[0];
        // delete clock
        std::unique_ptr<e::buffer> del_clk_buf = prepare_buffer(n.get_del_time());
        cl_attr[1].attr = attrs[1];
        cl_attr[1].value = (const char*)del_clk_buf->data();
        cl_attr[1].value_sz = del_clk_buf->size();
        cl_attr[1].datatype = dtypes[1];
        // properties
        std::unique_ptr<e::buffer> props_buf = prepare_buffer(*n.get_props());
        cl_attr[2].attr = attrs[2];
        cl_attr[2].value = (const char*)props_buf->data();
        cl_attr[2].value_sz = props_buf->size();
        cl_attr[2].datatype = dtypes[2];
        // out edges
        std::unique_ptr<e::buffer> out_edges_buf = prepare_buffer(n.out_edges);
        cl_attr[3].attr = attrs[3];
        cl_attr[3].value = (const char*)out_edges_buf->data();
        cl_attr[3].value_sz = out_edges_buf->size();
        cl_attr[3].datatype = dtypes[3];
        // in nbrs
        std::unique_ptr<e::buffer> in_nbrs_buf = prepare_buffer(nbr_map);
        cl_attr[4].attr = attrs[4];
        cl_attr[4].value = (const char*)in_nbrs_buf->data();
        cl_attr[4].value_sz = in_nbrs_buf->size();
        cl_attr[4].datatype = dtypes[4];

        hyper_call_and_loop(&hyperdex::Client::put, n.get_handle(), cl_attr, 5);
        free(cl_attr);
    }

    inline void
    hyper_stub :: update_creat_time(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> creat_clk_buf = prepare_buffer(n.get_creat_time());
        cl_attr.attr = attrs[0];
        cl_attr.value = (const char*)creat_clk_buf->data();
        cl_attr.value_sz = creat_clk_buf->size();
        cl_attr.datatype = dtypes[0];

        hyper_call_and_loop(&hyperdex::Client::put, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: update_del_time(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> del_clk_buf = prepare_buffer(n.get_del_time());
        cl_attr.attr = attrs[1];
        cl_attr.value = (const char*)del_clk_buf->data();
        cl_attr.value_sz = del_clk_buf->size();
        cl_attr.datatype = dtypes[1];

        hyper_call_and_loop(&hyperdex::Client::put, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: update_properties(element::node &n)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> props_buf = prepare_buffer(*n.get_props());
        cl_attr.attr = attrs[2];
        cl_attr.value = (const char*)props_buf->data();
        cl_attr.value_sz = props_buf->size();
        cl_attr.datatype = dtypes[2];

        hyper_call_and_loop(&hyperdex::Client::put, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: add_out_edge(element::node &n, element::edge *e)
    {
        hyperdex_client_map_attribute map_attr;
        std::unique_ptr<e::buffer> key_buf = prepare_buffer(e->get_handle());
        std::unique_ptr<e::buffer> val_buf = prepare_buffer(e);
        map_attr.attr = attrs[3];
        map_attr.map_key = (const char*)key_buf->data();
        map_attr.map_key_sz = key_buf->size();
        map_attr.map_key_datatype = HYPERDATATYPE_INT64;
        map_attr.value = (const char*)val_buf->data();
        map_attr.value_sz = val_buf->size();
        map_attr.value_datatype = HYPERDATATYPE_STRING;

        hypermap_call_and_loop(&hyperdex::Client::map_add, n.get_handle(), &map_attr, 1);
    }

    inline void
    hyper_stub :: remove_out_edge(element::node &n, element::edge *e)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> key_buf = prepare_buffer(e->get_handle());
        cl_attr.attr = attrs[3];
        cl_attr.value = (const char*)key_buf->data();
        cl_attr.value_sz = key_buf->size();
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::map_remove, n.get_handle(), &cl_attr, 1);
    }

    inline void
    hyper_stub :: add_in_nbr(uint64_t n_hndl, uint64_t nbr)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> nbr_buf = prepare_buffer(nbr);
        cl_attr.attr = attrs[4];
        cl_attr.value = (const char*)nbr_buf->data();
        cl_attr.value_sz = nbr_buf->size();
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::set_add, n_hndl, &cl_attr, 1);
    }

    inline void
    hyper_stub :: remove_in_nbr(uint64_t n_hndl, uint64_t nbr)
    {
        hyperdex_client_attribute cl_attr;
        std::unique_ptr<e::buffer> nbr_buf = prepare_buffer(nbr);
        cl_attr.attr = attrs[4];
        cl_attr.value = (const char*)nbr_buf->data();
        cl_attr.value_sz = nbr_buf->size();
        cl_attr.datatype = HYPERDATATYPE_INT64;

        hyper_call_and_loop(&hyperdex::Client::set_remove, n_hndl, &cl_attr, 1);
    }
}

#endif
