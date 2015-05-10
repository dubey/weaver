/*
 * ===============================================================
 *    Description:  Base class for Hyperdex client, used by both
 *                  db::hyper_stub and coordinator::hyper_stub.
 *
 *        Created:  2014-02-26 15:23:54
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_hyper_stub_base_h_
#define weaver_common_hyper_stub_base_h_

#include <memory>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <e/endian.h>
#include <e/buffer.h>
#include <hyperdex/client.h>
#include <hyperdex/datastructures.h>

#include "common/message.h"
#include "common/transaction.h"
#include "db/types.h"
#include "db/node.h"

#define NUM_INDEX_ATTRS 2
#define NUM_GRAPH_ATTRS 8
#define NUM_TX_ATTRS 2

enum persist_node_state
{
    STABLE = 0,
    MOVING
};

struct async_put_node
{
    std::string handle;
    hyperdex_client_attribute attrs[NUM_GRAPH_ATTRS];
    std::unique_ptr<e::buffer> creat_clk_buf;
    std::unique_ptr<e::buffer> props_buf;
    std::unique_ptr<e::buffer> out_edges_buf;
    std::unique_ptr<e::buffer> aliases_buf;
    size_t num_attrs;
};

struct async_put_edge_unit
{
    db::edge *e;
    std::string edge_handle, alias;
    bool del_after_call;
    std::unique_ptr<e::buffer> edge_buf;
};

struct async_put_edge
{
    bool used;
    std::string node_handle;
    std::vector<async_put_edge_unit> batched;
    hyperdex_client_map_attribute *attr;

    async_put_edge() : used(false) { }

    void reset() { used = false; }
};

struct async_add_index
{
    std::string node_handle, alias;
    hyperdex_client_attribute index_attrs[NUM_INDEX_ATTRS];
};

class hyper_stub_base
{
    protected:
        // node handle -> node data
        const char *graph_space = "weaver_graph_data";
        const char *graph_attrs[NUM_GRAPH_ATTRS];
        const char *graph_key = "node";
        const enum hyperdatatype graph_dtypes[NUM_GRAPH_ATTRS];
        // tx id -> vt id, tx data
        const char *tx_space = "weaver_tx_data";
        const char *tx_attrs[NUM_TX_ATTRS];
        const char *tx_key = "tx_id";
        const enum hyperdatatype tx_dtypes[NUM_TX_ATTRS];
        // aux index: index -> node,shard
        const char *index_space = "weaver_index_data";
        const char *index_attrs[NUM_INDEX_ATTRS];
        const char *index_key = "idx";
        const enum hyperdatatype index_dtypes[NUM_INDEX_ATTRS];

        // no loop calls
        int64_t delayed_idx;
        std::unordered_map<int64_t, int64_t> delayed_opid_to_idx;
        std::unordered_map<int64_t, hyperdex_client_returncode> delayed_call_status;

        using hyper_func = int64_t (*) (struct hyperdex_client *client,
            const char*,
            const char*,
            size_t,
            const struct hyperdex_client_attribute*,
            size_t,
            hyperdex_client_returncode*);
        using hyper_map_func = int64_t (*) (struct hyperdex_client *client,
            const char*,
            const char*,
            size_t,
            const struct hyperdex_client_map_attribute*,
            size_t,
            hyperdex_client_returncode*);
        using hyper_tx_func = int64_t (*) (struct hyperdex_client_transaction *client,
            const char*,
            const char*,
            size_t,
            const struct hyperdex_client_attribute*,
            size_t,
            hyperdex_client_returncode*);
        using hyper_map_tx_func = int64_t (*) (struct hyperdex_client_transaction *client,
            const char*,
            const char*,
            size_t,
            const struct hyperdex_client_map_attribute*,
            size_t,
            hyperdex_client_returncode*);

        hyperdex_client *cl;
        hyperdex_client_transaction *hyper_tx;

        void begin_tx();
        void commit_tx(hyperdex_client_returncode &commit_status);
        void abort_tx();
        bool call(hyper_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_attribute *cl_attr, size_t num_attrs);
        bool call(hyper_tx_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_attribute *cl_attr, size_t num_attrs);
        bool call_no_loop(hyper_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_attribute *cl_attr, size_t num_attrs);
        bool map_call(hyper_map_tx_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_map_attribute *map_attr, size_t num_attrs);
        bool map_call_no_loop(hyper_map_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_map_attribute *map_attr, size_t num_attrs);
        bool loop();

        bool multiple_call(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs);
        bool multiple_call(std::vector<hyper_tx_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs);
        bool multiple_call(std::vector<hyper_tx_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
            std::vector<hyper_map_tx_func> &map_funcs,
            std::vector<const char*> &map_spaces,
            std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
            std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs);
        bool multiple_call_no_loop(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs);
        bool multiple_call_no_loop(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
            std::vector<hyper_map_func> &map_funcs,
            std::vector<const char*> &map_spaces,
            std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
            std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs);
        bool multiple_loop(uint64_t num_loops);

        bool get(const char *space,
            const char *key, size_t key_sz,
            const hyperdex_client_attribute **cl_attr, size_t *num_attrs,
            bool tx);
        bool get_partial(const char *space,
            const char *key, size_t key_sz,
            const char** attrnames, size_t attrnames_sz,
            const hyperdex_client_attribute **cl_attr, size_t *num_attrs,
            bool tx);
        bool multiple_get(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs,
            bool tx);
        bool multiple_get_partial(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            const char** attrnames, size_t attrnames_sz,
            std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs,
            bool tx);
        bool del(const char* space,
            const char *key, size_t key_sz);
        bool multiple_del(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs);

        // graph data functions
        bool get_node(db::node &n);
        bool get_nodes(std::unordered_map<node_handle_t, db::node*> &nodes, bool tx);
        //bool put_node(db::node &n);
        bool put_nodes(std::unordered_map<node_handle_t, db::node*> &nodes, bool if_not_exist);
        bool put_nodes_bulk(std::unordered_map<node_handle_t, db::node*> &nodes,
            std::shared_ptr<vc::vclock> last_upd_clk,
            std::shared_ptr<vc::vclock_t> restore_clk);
        bool del_node(const node_handle_t &h);
        bool del_nodes(std::unordered_set<node_handle_t> &to_del);
        bool recreate_node(const hyperdex_client_attribute *cl_attr, db::node &n);

        // node map functions
        bool update_nmap(const node_handle_t &handle, uint64_t loc);
        std::unordered_map<node_handle_t, uint64_t> get_nmap(std::unordered_set<node_handle_t> &toGet, bool tx);
        uint64_t get_nmap(node_handle_t &handle);

        // auxiliary index functions
    private:
        bool recreate_index(const hyperdex_client_attribute *cl_attr, std::pair<node_handle_t, uint64_t> &value);
        void sort_and_pack_as_set(std::vector<std::string>&, std::unique_ptr<e::buffer>&);
    public:
        bool add_indices(std::unordered_map<std::string, db::node*> &indices, bool tx, bool if_not_exist);
        bool get_indices(std::unordered_map<std::string, std::pair<node_handle_t, uint64_t>> &indices, bool tx);
        bool del_indices(std::vector<std::string> &indices);

        // tx data functions
        bool put_tx_data(transaction::pending_tx *tx);
        bool del_tx_data(uint64_t tx_id);

        template <typename T> void prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, T &t);
        template <typename T> void prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map);
        template <typename T> void prepare_buffer(const db::data_map<T> &map, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, db::data_map<T> &map);
        void prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set);
        void prepare_buffer(const db::string_set&, std::unique_ptr<e::buffer> &buf);
        void unpack_buffer(const char *buf, uint64_t buf_sz, db::string_set&);
        // properties
        void prepare_buffer(const std::vector<std::shared_ptr<db::property>>&, std::unique_ptr<e::buffer>&);
        void unpack_buffer(const char *buf, uint64_t buf_sz, std::vector<std::shared_ptr<db::property>>&);

    protected:
        void prepare_node(hyperdex_client_attribute *attr,
            db::node &n,
            std::unique_ptr<e::buffer>&,
            std::unique_ptr<e::buffer>&,
            std::unique_ptr<e::buffer>&,
            std::unique_ptr<e::buffer>&,
            std::unique_ptr<e::buffer>&,
            std::unique_ptr<e::buffer>&,
            size_t &num_attrs);
        void prepare_edges(hyperdex_client_map_attribute *cl_attrs,
            std::vector<async_put_edge_unit> &edges);
        void pack_uint64(e::buffer::packer &packer, uint64_t num);
        void unpack_uint64(e::unpacker &unpacker, uint64_t &num);
        void pack_uint32(e::buffer::packer &packer, uint32_t num);
        void unpack_uint32(e::unpacker &unpacker, uint32_t &num);
        void pack_string(e::buffer::packer &packer, const std::string &t);
        void unpack_string(e::unpacker &unpacker, std::string &t, const uint32_t sz);

    public:
        hyper_stub_base();
};

// store the given t as a HYPERDATATYPE_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = message::size(t);
    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack_at(0);
    message::pack_buffer(packer, t);
}

// unpack the HYPERDATATYPE_STRING in to the given object
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, T &t)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    message::unpack_buffer(unpacker, t);
}

// store the given unordered_map as a HYPERDATATYPE_MAP_STRING_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<std::string> sorted;
    sorted.reserve(map.size());

    for (const auto &p: map) {
        sorted.emplace_back(p.first);
    }
    std::sort(sorted.begin(), sorted.end());

    std::vector<uint32_t> val_sz(map.size(), UINT32_MAX);
    // now iterate in sorted order
    for (uint64_t i = 0; i < sorted.size(); i++) {
        val_sz[i] = message::size(map.at(sorted[i]));
        buf_sz += sizeof(uint32_t) // map key encoding sz
                + sorted[i].size()
                + sizeof(uint32_t) // map val encoding sz
                + val_sz[i]; // map val encoding
    }

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t i = 0; i < sorted.size(); i++) {
        pack_uint32(packer, sorted[i].size());
        pack_string(packer, sorted[i]);

        pack_uint32(packer, val_sz[i]);
        message::pack_buffer(packer, map.at(sorted[i]));
    }
}

// unpack the HYPERDATATYPE_MAP_STRING_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string key;
    uint32_t sz;

    while (!unpacker.empty()) {
        key.erase();

        unpack_uint32(unpacker, sz);
        unpack_string(unpacker, key, sz);

        unpack_uint32(unpacker, sz);
        message::unpack_buffer(unpacker, map[key]);
    }
}

// store the given unordered_map as a HYPERDATATYPE_MAP_STRING_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const db::data_map<T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<std::string> sorted;
    sorted.reserve(map.size());

    for (const auto &p: map) {
        sorted.emplace_back(p.first);
    }
    std::sort(sorted.begin(), sorted.end());

    std::vector<uint32_t> val_sz(map.size(), UINT32_MAX);
    // now iterate in sorted order
    for (uint64_t i = 0; i < sorted.size(); i++) {
        val_sz[i] = message::size(map.find(sorted[i])->second);
        buf_sz += sizeof(uint32_t) // map key encoding sz
                + sorted[i].size()
                + sizeof(uint32_t) // map val encoding sz
                + val_sz[i]; // map val encoding
    }

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();

    for (uint64_t i = 0; i < sorted.size(); i++) {
        pack_uint32(packer, sorted[i].size());
        pack_string(packer, sorted[i]);

        pack_uint32(packer, val_sz[i]);
        message::pack_buffer(packer, map.find(sorted[i])->second);
    }
}

// unpack the HYPERDATATYPE_MAP_STRING_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, db::data_map<T> &map)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string key;
    uint32_t sz;

    while (!unpacker.empty()) {
        key.erase();

        unpack_uint32(unpacker, sz);
        unpack_string(unpacker, key, sz);

        unpack_uint32(unpacker, sz);
        message::unpack_buffer(unpacker, map[key]);
    }
}

#endif
