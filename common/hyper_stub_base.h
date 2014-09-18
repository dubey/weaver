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

#include "common/message.h"
#include "common/transaction.h"
#include "db/node.h"

#define NUM_GRAPH_ATTRS 6
#define NUM_TX_ATTRS 2


enum persist_node_state
{
    STABLE = 0,
    MOVING
};

class hyper_stub_base
{
    protected:
        // node handle -> node data
        const char *graph_space = "weaver_graph_data";
        const char *graph_attrs[NUM_GRAPH_ATTRS];
        const enum hyperdatatype graph_dtypes[NUM_GRAPH_ATTRS];
        // node handle -> shard
        const char *nmap_space = "weaver_loc_mapping";
        const char *nmap_attr = "shard";
        const enum hyperdatatype nmap_dtype = HYPERDATATYPE_INT64;
        // tx id -> vt id, tx data
        const char *tx_space = "weaver_tx_data";
        const char *tx_attrs[NUM_TX_ATTRS];
        const enum hyperdatatype tx_dtypes[NUM_TX_ATTRS];

        //using hyper_func = int64_t (*) (struct hyperdex_client_transaction *client,
        using hyper_func = int64_t (*) (struct hyperdex_client *client,
            const char*,
            const char*,
            size_t,
            const struct hyperdex_client_attribute*,
            size_t,
            hyperdex_client_returncode*);
        //using hyper_map_func = int64_t (*) (struct hyperdex_client_transaction *client,
        using hyper_map_func = int64_t (*) (struct hyperdex_client *client,
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
        bool map_call(hyper_map_func h,
            const char *space,
            const char *key, size_t key_sz,
            hyperdex_client_map_attribute *map_attr, size_t num_attrs);

        bool multiple_call(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs);
        bool multiple_call(std::vector<hyper_func> &funcs,
            std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<hyperdex_client_attribute*> &attrs, std::vector<size_t> &num_attrs,
            std::vector<hyper_map_func> &map_funcs,
            std::vector<const char*> &map_spaces,
            std::vector<const char*> &map_keys, std::vector<size_t> &map_key_szs,
            std::vector<hyperdex_client_map_attribute*> &map_attrs, std::vector<size_t> &map_num_attrs);
        bool get(const char *space,
            const char *key, size_t key_sz,
            const hyperdex_client_attribute **cl_attr, size_t *num_attrs);
        bool multiple_get(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs,
            std::vector<const hyperdex_client_attribute**> &cl_attrs, std::vector<size_t*> &num_attrs);
        bool del(const char* space,
            const char *key, size_t key_sz);
        bool multiple_del(std::vector<const char*> &spaces,
            std::vector<const char*> &keys, std::vector<size_t> &key_szs);

        // graph data functions
        bool get_node(db::element::node &n);
        bool put_node(db::element::node &n);
        void del_node(const node_handle_t &h);
        void update_creat_time(db::element::node &n);
        void update_properties(db::element::node &n);
        void add_out_edge(db::element::node &n, db::element::edge *e);
        void remove_out_edge(db::element::node &n, db::element::edge *e);
        void add_in_nbr(const node_handle_t &node, const node_handle_t &nbr);
        void remove_in_nbr(const node_handle_t &n_hndl, const node_handle_t &nbr);
        bool recreate_node(const hyperdex_client_attribute *cl_attr, db::element::node &n);

        // node map functions
        bool put_nmap(std::unordered_map<node_handle_t, uint64_t> &pairs_to_add);
        bool put_nmap(const node_handle_t &handle, uint64_t loc);
        std::unordered_map<node_handle_t, uint64_t> get_nmap(std::unordered_set<node_handle_t> &toGet);
        bool del_nmap(std::unordered_set<node_handle_t> &toDel);

        // tx data functions
        bool put_tx_data(transaction::pending_tx *tx);
        bool del_tx_data(uint64_t tx_id);

        template <typename T> void prepare_buffer(const T &t, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, T &t);
        //template <typename T> void prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<e::buffer> &buf);
        //template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map);
        template <typename T> void prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf);
        template <typename T> void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map);
        //void prepare_buffer(const std::unordered_map<uint64_t, uint64_t> &map, std::unique_ptr<e::buffer> &buf);
        //void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, uint64_t> &map);
        //void prepare_buffer(const std::unordered_set<uint64_t> &set, std::unique_ptr<e::buffer> &buf);
        //void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<uint64_t> &set);
        //void prepare_buffer(const std::unordered_set<std::string> &set, std::unique_ptr<e::buffer> &buf);
        //void unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_set<std::string> &set);

    private:
        void pack_uint64(e::buffer::packer &packer, uint64_t num);
        void pack_uint32(e::buffer::packer &packer, uint32_t num);

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

/*
// store the given unordered_map as a HYPERDATATYPE_MAP_INT64_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_map<uint64_t, T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<uint64_t> sorted;
    sorted.reserve(map.size());
    std::vector<uint32_t> val_sz;
    for (auto &p: map) {
        sorted.emplace_back(p.first);
        val_sz.emplace_back(message::size(p.second));
        buf_sz += sizeof(p.first) // map key
                + sizeof(uint32_t) // map val encoding sz
                + val_sz.back(); // map val encoding
    }
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();
    // now iterate in sorted order
    uint64_t i = 0;
    for (uint64_t key: sorted) {
        pack_uint64(packer, key);
        message::pack_buffer(packer, map[key]);
        i++;
    }
}

// unpack the HYPERDATATYPE_MAP_INT64_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<uint64_t, T> &map)
{
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    const uint8_t *cur = ebuf->data();
    e::unpacker unpacker = ebuf->unpack_from(0);
    uint64_t key;

    while (!unpacker.empty()) {
        cur = e::unpack64le(cur, &key);
        unpacker.advance(8);
        message::unpack_buffer(unpacker, map[key]);
        cur += message::size(map[key]);
    }
}
*/

// store the given unordered_map as a HYPERDATATYPE_MAP_STRING_STRING
template <typename T>
inline void
hyper_stub_base :: prepare_buffer(const std::unordered_map<std::string, T> &map, std::unique_ptr<e::buffer> &buf)
{
    uint64_t buf_sz = 0;
    std::vector<std::string> sorted;
    sorted.reserve(map.size());
    std::vector<uint32_t> key_sz;
    std::vector<uint32_t> val_sz;
    for (auto &p: map) {
        sorted.emplace_back(p.first);
        key_sz.emplace_back(message::size(p.first));
        val_sz.emplace_back(message::size(p.second));
        buf_sz += sizeof(uint32_t) // map key encoding sz
                + key_sz.back()
                + sizeof(uint32_t) // map val encoding sz
                + val_sz.back(); // map val encoding
        //WDEBUG << "key " << p.first << ", key_sz " << key_sz.back() << std::endl;
        //WDEBUG << "value_sz " << val_sz.back() << std::endl;
    }
    //WDEBUG << "Total buf sz = " << buf_sz << std::endl;
    std::sort(sorted.begin(), sorted.end());

    buf.reset(e::buffer::create(buf_sz));
    e::buffer::packer packer = buf->pack();
    // now iterate in sorted order
    uint64_t i = 0;
    for (const std::string &key: sorted) {
        //WDEBUG << "packer remain = " << packer.remain() << std::endl;
        pack_uint32(packer, key_sz[i]);
        //WDEBUG << "packer remain = " << packer.remain() << std::endl;
        message::pack_buffer(packer, key);
        //WDEBUG << "packer remain = " << packer.remain() << std::endl;

        pack_uint32(packer, val_sz[i]);
        //WDEBUG << "packer remain = " << packer.remain() << std::endl;
        message::pack_buffer(packer, map.at(key));
        //WDEBUG << "packer remain = " << packer.remain() << std::endl;
        i++;
    }
}

// unpack the HYPERDATATYPE_MAP_STRING_STRING in to the given map
template <typename T>
inline void
hyper_stub_base :: unpack_buffer(const char *buf, uint64_t buf_sz, std::unordered_map<std::string, T> &map)
{
    //WDEBUG << "Total buf sz = " << buf_sz << std::endl;
    std::unique_ptr<e::buffer> ebuf(e::buffer::create(buf, buf_sz));
    e::unpacker unpacker = ebuf->unpack_from(0);
    std::string key;
    //uint32_t sz;

    while (!unpacker.empty()) {
        key.erase();

        unpacker = unpacker.advance(sizeof(uint32_t));
        //buf = e::unpack32le(buf, &sz);
        //WDEBUG << "got key sz " << sz << std::endl;
        message::unpack_buffer(unpacker, key);
        //WDEBUG << "got key: sz = " << key.size() << ", val = " << key << std::endl;
        //buf += sz;

        unpacker = unpacker.advance(sizeof(uint32_t));
        message::unpack_buffer(unpacker, map[key]);
    }
}

inline void
hyper_stub_base :: pack_uint64(e::buffer::packer &pkr, uint64_t num)
{
    uint8_t intbuf[8];
    e::pack64le(num, intbuf);
    e::slice intslc(intbuf, 8);
    pkr = pkr.copy(intslc);
}

inline void
hyper_stub_base :: pack_uint32(e::buffer::packer &pkr, uint32_t num)
{
    uint8_t intbuf[4];
    e::pack32le(num, intbuf);
    e::slice intslc(intbuf, 4);
    pkr = pkr.copy(intslc);
}

#endif
