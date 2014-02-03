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
    class hdex_stub
    {
        private:
            const char *space = "weaver_shard_data";
            const char *attrs[5];
            const enum hyperdatatype dtypes[5];
            hyperdex::Client cl;

        private:
            template <typename T> std::unique_ptr<e::buffer> prepare_buffer(const T &t);
            std::unique_ptr<e::buffer> prepare_buffer(const std::unordered_map<uint64_t, element::edge*> &map);
            std::unique_ptr<e::buffer> prepare_buffer(const std::unordered_set<uint64_t> &set);

        public:
            hdex_stub();
            void put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map);
            element::node* get_node(uint64_t node);
            void put_edge(element::node &n, element::edge *e);
    };

    inline
    hdex_stub :: hdex_stub()
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

    inline std::unique_ptr<e::buffer>
    prepare_buffer(const std::unordered_map<uint64_t, element::edge*> &map)
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
    prepare_buffer(const std::unordered_set<uint64_t> &set)
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

    template <typename T>
    inline std::unique_ptr<e::buffer>
    prepare_buffer(const T &t)
    {
        uint64_t buf_sz = message::size(t);
        std::unique_ptr<e::buffer> buf(e::buffer::create(buf_sz));
        e::buffer::packer packer = buf->pack_at(0);
        message::pack_buffer(packer, t);
        return std::move(buf);
    }

    inline void
    hdex_stub :: put_node(element::node &n, std::unordered_set<uint64_t> &nbr_map)
    {
        uint64_t hndl = n.get_handle();
        hyperdex_client_attribute *cl_attr = (hyperdex_client_attribute*)malloc(5 * sizeof(hyperdex_client_attribute));
        hyperdex_client_returncode status;
        int64_t hdex_id;
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

        hdex_id = cl.put(space, (const char*)&hndl, sizeof(int64_t), cl_attr, 5, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex put failed, op id = " << hdex_id << ", status = " << status << std::endl;
            free(cl_attr);
            return;
        }
        hdex_id = cl.loop(-1, &status);
        if (hdex_id < 0) {
            WDEBUG << "Hyperdex loop failed, op id = " << hdex_id << ", status = " << status << std::endl;
        }
        free(cl_attr);
    }
}

#endif
