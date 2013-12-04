/*
 * ================================================================
 *    Description:  Inter-server message packing and unpacking
 *
 *        Created:  11/07/2012 01:40:52 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __MESSAGE__
#define __MESSAGE__

#include <memory>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <string.h>
#include <e/buffer.h>
#include <po6/net/location.h>
#include <busybee_constants.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "common/vclock.h"
#include "common/transaction.h"
#include "db/element/node.h"
#include "db/element/edge.h"
#include "db/element/remote_node.h"
#include "node_prog/node_prog_type.h" // used for packing Packable objects

namespace message
{
    enum msg_type
    {
        // client messages
        CLIENT_NODE_CREATE_REQ = 0,
        CLIENT_EDGE_CREATE_REQ,
        CLIENT_NODE_DELETE_REQ,
        CLIENT_EDGE_DELETE_REQ,
        CLIENT_ADD_EDGE_PROP,
        CLIENT_DEL_EDGE_PROP,
        CLIENT_TX_INIT,
        CLIENT_TX_DONE,
        CLIENT_REPLY,
        CLIENT_COMMIT_GRAPH,
        CLIENT_NODE_PROG_REQ,
        CLIENT_NODE_PROG_REPLY,
        CLIENT_NODE_LOC_REQ,
        CLIENT_NODE_LOC_REPLY,
        START_MIGR,
        ONE_STREAM_MIGR,
        EXIT_WEAVER,
        // graph update messages
        TX_INIT,
        TX_DONE,
        NODE_CREATE_REQ,
        EDGE_CREATE_REQ,
        TRANSIT_EDGE_CREATE_REQ,
        REVERSE_EDGE_CREATE,
        TRANSIT_REVERSE_EDGE_CREATE,
        NODE_CREATE_ACK,
        EDGE_CREATE_ACK,
        TRANSIT_EDGE_CREATE_ACK,
        NODE_DELETE_REQ,
        TRANSIT_NODE_DELETE_REQ,
        EDGE_DELETE_REQ,
        TRANSIT_EDGE_DELETE_REQ,
        PERMANENT_DELETE_EDGE,
        PERMANENT_DELETE_EDGE_ACK,
        NODE_DELETE_ACK,
        EDGE_DELETE_ACK,
        EDGE_ADD_PROP,
        TRANSIT_EDGE_ADD_PROP,
        EDGE_DELETE_PROP,
        TRANSIT_EDGE_DELETE_PROP,
        EDGE_DELETE_PROP_ACK,
        CLEAN_UP,
        CLEAN_UP_ACK,
        // node program messages
        NODE_PROG,
        NODE_PROG_RETURN,
        CACHE_UPDATE,
        CACHE_UPDATE_ACK,
        // migration messages
        MIGRATE_SEND_NODE,
        COORD_CLOCK_REQ,
        COORD_CLOCK_REPLY,
        MIGRATED_NBR_UPDATE,
        MIGRATED_NBR_ACK,
        MIGRATION_TOKEN,
        REQUEST_COUNT,
        REQUEST_COUNT_ACK,
        // initial graph loading
        LOADED_GRAPH,
        // coordinator group
        COORD_LOC_REQ,
        COORD_LOC_REPLY,
        VT_CLOCK_UPDATE,
        VT_CLOCK_UPDATE_ACK,
        VT_NOP,
        VT_NOP_ACK,
        DONE_MIGR,

        ERROR
    };

    enum edge_direction
    {
        FIRST_TO_SECOND = 0,
        SECOND_TO_FIRST = 1
    };

    class message
    {
        public:
            message();
            message(enum msg_type t);
            message(message &copy);

        public:
            enum msg_type type;
            std::auto_ptr<e::buffer> buf;

        public:
            void change_type(enum msg_type t);
    };

    template <typename T1, typename T2> inline uint64_t size(const std::unordered_map<T1, T2>& t);
    template <typename T> inline uint64_t size(const std::unordered_set<T>& t);
    template <typename T> inline uint64_t size(const std::vector<T>& t);
    template <typename T1, typename T2, typename T3> uint64_t size(std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline uint64_t size(const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline uint64_t size(const std::tuple<T1, T2, T3>& t);

    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_map<T1, T2>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::unordered_set<T>& t);
    template <typename T> inline void pack_buffer(e::buffer::packer& packer, const std::vector<T>& t);
    template <typename T1, typename T2, typename T3> void pack_buffer(e::buffer::packer&, std::priority_queue<T1, T2, T3>);
    template <typename T1, typename T2> inline void pack_buffer(e::buffer::packer &packer, const std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void pack_buffer(e::buffer::packer &packer, const std::tuple<T1, T2, T3>& t);

    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_map<T1, T2>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::unordered_set<T>& t);
    template <typename T> inline void unpack_buffer(e::unpacker& unpacker, std::vector<T>& t);
    template <typename T1, typename T2, typename T3> void unpack_buffer(e::unpacker&, std::priority_queue<T1, T2, T3>&);
    template <typename T1, typename T2> inline void unpack_buffer(e::unpacker& unpacker, std::pair<T1, T2>& t);
    template <typename T1, typename T2, typename T3> inline void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t);
    uint64_t size(const db::element::edge* const &t);
    uint64_t size(const db::element::node &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t);
    void pack_buffer(e::buffer::packer &packer, const db::element::node &t);
    void unpack_buffer(e::unpacker &unpacker, db::element::edge *&t);
    void unpack_buffer(e::unpacker &unpacker, db::element::node &t);

    inline
    message :: message()
        : type(ERROR)
    {
    }

    inline
    message :: message(enum msg_type t)
        : type(t)
    {
    }

    inline 
    message :: message(message &copy)
        : type(copy.type)
    {
        buf = copy.buf;
    }

    inline void
    message :: change_type(enum msg_type t)
    {
        type = t;
    }

// size templates
    inline uint64_t size(const node_prog::prog_type&)
    {
        return sizeof(uint32_t);
    }
    inline uint64_t size(const node_prog::Packable &t)
    {
        return t.size();
    }
    inline uint64_t size(const node_prog::Packable_Deletable *&t)
    {
        return t->size();
    }
    inline uint64_t size(const bool&)
    {
        return sizeof(uint16_t);
    }
    inline uint64_t size(const uint16_t&)
    {
        return sizeof(uint16_t);
    }
    inline uint64_t size(const uint32_t&)
    {
        return sizeof(uint32_t);
    }
    inline uint64_t size(const uint64_t&)
    {
        return sizeof(uint64_t);
    }
    inline uint64_t size(const int&)
    {
        return sizeof(int);
    }
    inline uint64_t size(const double&)
    {
        return sizeof(uint64_t);
    }
    inline uint64_t size(const vc::vclock &t)
    {
        return size(t.vt_id)
            + size(t.clock);
    }
    inline uint64_t size(const common::property &t)
    {
        return sizeof(t.key)
            + sizeof(t.value)
            + 2*sizeof(t.creat_time);
    }
    inline uint64_t size(const db::element::remote_node &t)
    {
        return size(t.loc) + size(t.handle);
    }
    inline uint64_t size(const common::meta_element &t)
    {
        return size(t.get_loc())
            + size(t.get_creat_time())
            + size(t.get_del_time())
            + size(t.get_handle());
    }

    inline uint64_t
    size(const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        return sizeof(t.type)
             + size(t.qts)
             + size(t.handle)
             + size(t.elem1)
             + size(t.elem2)
             + size(t.loc1)
             + size(t.loc2);
    }

    template <typename T1, typename T2>
    inline uint64_t size(const std::pair<T1, T2> &t)
    {
        return size(t.first) + size(t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(const std::tuple<T1, T2, T3> &t){
        return size(std::get<0>(t)) + size(std::get<1>(t)) + size(std::get<2>(t));
    }

    template <typename T>
    inline uint64_t size(const std::unordered_set<T> &t)
    {
        // O(n) size operation can handle elements of differing sizes
        uint64_t total_size = 0;
        for(const T &elem : t) {
            total_size += size(elem);
        }
        return sizeof(uint64_t)+total_size;
    }

    template <typename T1, typename T2>
    inline uint64_t size(const std::unordered_map<T1, T2> &t)
    {
        uint64_t total_size = 0;
        // O(n) size operation can handle keys and values of differing sizes
        for (const std::pair<T1,T2> &pair : t) {
            total_size += size(pair.first) + size(pair.second);
        }
        return sizeof(uint64_t)+total_size;
    }

    template <typename T>
    inline uint64_t size(const std::vector<T> &t)
    {
        uint64_t tot_size = sizeof(uint64_t);
        for (const T &elem : t) {
            tot_size += size(elem);
        }
        return tot_size;
    }

    template <typename T1, typename T2, typename T3>
    inline uint64_t size(std::priority_queue<T1, T2, T3> t)
    {
        uint64_t sz = sizeof(uint64_t);
        while (!t.empty()) {
            sz += size(t.top());
            t.pop();
        }
        return sz;
    }

    template <typename T, typename... Args>
    inline uint64_t size(const T &t, const Args&... args)
    {
        return size(t) + size(args...);
    }

    // packing templates

    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Packable &t)
    {
        t.pack(packer);
    }
    inline void pack_buffer(e::buffer::packer &packer, const node_prog::Packable_Deletable *&t)
    {
        t->pack(packer);
    }
    inline void pack_buffer(e::buffer::packer &packer, const node_prog::prog_type &t)
    {
        packer = packer << t;
    }

    inline void pack_buffer(e::buffer::packer &packer, const bool &t)
    {
        uint16_t to_pack = t ? 1 : 0;
        packer = packer << to_pack;
    }
    inline void 
    pack_buffer(e::buffer::packer &packer, const uint16_t &t)
    {
        packer = packer << t;
    }
    inline void 
    pack_buffer(e::buffer::packer &packer, const uint32_t &t)
    {
        packer = packer << t;
    }
    inline void 
    pack_buffer(e::buffer::packer &packer, const uint64_t &t)
    {
        packer = packer << t;
    }
    inline void 
    pack_buffer(e::buffer::packer &packer, const int &t)
    {
        packer = packer << t;
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const double &t)
    {
        uint64_t dbl;
        memcpy(&dbl, &t, sizeof(double)); //to avoid casting issues, probably could avoid copy
        packer = packer << dbl;
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const vc::vclock &t)
    {
        pack_buffer(packer, t.vt_id);
        pack_buffer(packer, t.clock);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const common::property &t)
    {
        pack_buffer(packer, t.key);
        pack_buffer(packer, t.value);
        pack_buffer(packer, t.creat_time);
        pack_buffer(packer, t.del_time);
    }

    inline void 
    pack_buffer(e::buffer::packer &packer, const db::element::remote_node &t)
    {
        packer = packer << t.loc << t.handle;
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const common::meta_element &t)
    {
        packer = packer << t.get_loc() << t.get_creat_time()
            << t.get_del_time() << t.get_handle();
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        transaction::pending_update &t = *ptr_t;
        packer = packer << t.type;
        pack_buffer(packer, t.qts);
        pack_buffer(packer, t.handle);
        pack_buffer(packer, t.elem1);
        pack_buffer(packer, t.elem2);
        pack_buffer(packer, t.loc1);
        pack_buffer(packer, t.loc2);
    }

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::pair<T1, T2> &t)
    {
        // assumes constant size
        pack_buffer(packer, t.first);
        pack_buffer(packer, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void pack_buffer(e::buffer::packer &packer, const std::tuple<T1, T2, T3> &t){
        pack_buffer(packer, std::get<0>(t));
        pack_buffer(packer, std::get<1>(t));
        pack_buffer(packer, std::get<2>(t));
    }

    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::vector<T> &t)
    {
        // !assumes constant element size
        uint64_t num_elems = t.size();
        packer = packer << num_elems;
        if (num_elems > 0){
            for (const T &elem : t) {
                pack_buffer(packer, elem);
            }
        }
    }
    
    template <typename T1, typename T2, typename T3>
    inline void
    pack_buffer(e::buffer::packer &packer, std::priority_queue<T1, T2, T3> t)
    {
        uint64_t num_elems = t.size();
        packer = packer << num_elems;
        while (!t.empty()) {
            pack_buffer(packer, t.top());
            t.pop();
        }
    }

    /* vector with elements of constant size
    template <typename T> 
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::vector<T> &t)
    {
        // !assumes constant element size
        uint64_t num_elems = t.size();
        //std::cout << "pack buffer packed vector of size " << num_elems<< std::endl;
        packer = packer << num_elems;
        if (num_elems > 0){
            uint64_t element_size = size(t[0]);
            for (const T &elem : t) {
                pack_buffer(packer, elem);
                assert(element_size == size(elem));//slow
            }
        }
    }
    */

    template <typename T>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_set<T> &t)
    {
        uint64_t num_keys = t.size();
        packer = packer << num_keys;
        for (const T &elem : t) {
            pack_buffer(packer, elem);
        }
    }

    template <typename T1, typename T2>
    inline void 
    pack_buffer(e::buffer::packer &packer, const std::unordered_map<T1, T2> &t)
    {
        uint64_t num_keys = t.size();
        packer = packer << num_keys;
        for (const std::pair<T1, T2> &pair : t) {
            pack_buffer(packer, pair.first);
            pack_buffer(packer, pair.second);
        }
    }

    template <typename T, typename... Args>
    inline void 
    pack_buffer(e::buffer::packer &packer, const T &t, const Args&... args)
    {
        pack_buffer(packer, t);
        pack_buffer(packer, args...);
    }

    inline void
    prepare_message(message &m, enum msg_type given_type)
    {
        uint32_t index = BUSYBEE_HEADER_SIZE;
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + sizeof(enum msg_type)));

        m.buf->pack_at(index) << given_type;
    }

    template <typename... Args>
    inline void
    prepare_message(message &m, const enum msg_type given_type, const Args&... args)
    {
        uint64_t bytes_to_pack = size(args...) + sizeof(enum msg_type);
        m.type = given_type;
        m.buf.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + bytes_to_pack));
        e::buffer::packer packer = m.buf->pack_at(BUSYBEE_HEADER_SIZE); 

        packer = packer << given_type;
        pack_buffer(packer, args...);
    }

    inline void
    shift_buffer(message &m, uint32_t shift_off)
    {
        m.buf->shift(shift_off);
    }

    inline bool
    empty_buffer(message &m)
    {
        return m.buf->empty();
    }

    // unpacking templates
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Packable &t)
    {
        t.unpack(unpacker);
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::Packable_Deletable *&t)
    {
        t->unpack(unpacker);
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, node_prog::prog_type &t)
    {
        uint32_t _type;
        unpacker = unpacker >> _type;
        t = (enum node_prog::prog_type)_type;
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, bool &t)
    {
        uint16_t temp;
        unpacker = unpacker >> temp;
        t = (temp != 0);
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, uint16_t &t)
    {
        unpacker = unpacker >> t;
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, uint32_t &t)
    {
        unpacker = unpacker >> t;
    }
    inline void 
    unpack_buffer(e::unpacker &unpacker, uint64_t &t)
    {
        unpacker = unpacker >> t;
    }
    inline void 
    unpack_buffer(e::unpacker &unpacker, int &t)
    {
        unpacker = unpacker >> t;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, vc::vclock &t)
    {
        unpack_buffer(unpacker, t.vt_id);
        unpack_buffer(unpacker, t.clock);
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, common::property &t)
    {
        unpack_buffer(unpacker, t.key);
        unpack_buffer(unpacker, t.value);
        unpack_buffer(unpacker, t.creat_time);
        unpack_buffer(unpacker, t.del_time);
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, double &t)
    {
        uint64_t dbl;
        unpacker = unpacker >> dbl;
        memcpy(&t, &dbl, sizeof(double)); //to avoid casting issues, probably could avoid copy
    }

    inline void 
    unpack_buffer(e::unpacker &unpacker, db::element::remote_node& t)
    {
        unpacker = unpacker >> t.loc >> t.handle;
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, common::meta_element &t)
    {
        uint64_t handle, loc, tc, td;
        unpacker = unpacker >> loc >> tc >> td >> handle;
        t.update_creat_time(tc);
        t.update_del_time(td);
        t.update_handle(handle);
        t.update_loc(loc);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, std::shared_ptr<transaction::pending_update> &ptr_t)
    {
        uint32_t mtype;
        ptr_t.reset(new transaction::pending_update());
        transaction::pending_update &t = *ptr_t;
        unpacker = unpacker >> mtype;
        t.type = ((enum transaction::update_type)mtype);
        unpack_buffer(unpacker, t.qts);
        unpack_buffer(unpacker, t.handle);
        unpack_buffer(unpacker, t.elem1);
        unpack_buffer(unpacker, t.elem2);
        unpack_buffer(unpacker, t.loc1);
        unpack_buffer(unpacker, t.loc2);
    }

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::pair<T1, T2> &t)
    {
        //assumes constant size
        unpack_buffer(unpacker, t.first);
        unpack_buffer(unpacker, t.second);
    }

    template <typename T1, typename T2, typename T3>
    inline void unpack_buffer(e::unpacker& unpacker, std::tuple<T1, T2, T3>& t){
        unpack_buffer(unpacker, std::get<0>(t));
        unpack_buffer(unpacker, std::get<1>(t));
        unpack_buffer(unpacker, std::get<2>(t));
    }

    template <typename T> 
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::vector<T> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;

        t.reserve(elements_left);

        while (elements_left > 0) {
            T to_add;
            unpack_buffer(unpacker, to_add);
            t.emplace_back(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T1, typename T2, typename T3>
    inline void
    unpack_buffer(e::unpacker &unpacker, std::priority_queue<T1, T2, T3> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left = 0;
        unpacker = unpacker >> elements_left;
        while (elements_left > 0) {
            T1 to_add;
            unpack_buffer(unpacker, to_add);
            t.push(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::unordered_set<T> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;

        t.rehash(elements_left*1.25); // set number of buckets to 1.25*elements it will contain

        while (elements_left > 0) {
            T to_add;
            unpack_buffer(unpacker, to_add);
            t.emplace(std::move(to_add));
            elements_left--;
        }
    }

    template <typename T1, typename T2>
    inline void 
    unpack_buffer(e::unpacker &unpacker, std::unordered_map<T1, T2> &t)
    {
        assert(t.size() == 0);
        uint64_t elements_left;
        unpacker = unpacker >> elements_left;
        // set number of buckets to 1.25*elements it will contain
        // did not use reserve as max_load_factor is default 1
        t.rehash(elements_left*1.25); 

        while (elements_left > 0) {
            T1 key_to_add;
            T2 val_to_add;

            unpack_buffer(unpacker, key_to_add);
            unpack_buffer(unpacker, val_to_add);
            t.emplace(key_to_add, std::move(val_to_add));
            elements_left--;
        }
    }

    template <typename T, typename... Args>
    inline void 
    unpack_buffer(e::unpacker &unpacker, T &t, Args&... args)
    {
        unpack_buffer(unpacker, t);
        unpack_buffer(unpacker, args...);
    }

    template <typename... Args>
    inline void
    unpack_message(const message &m, const enum msg_type expected_type, Args&... args)
    {
        uint32_t _type;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpacker = unpacker >> _type;
        assert((enum msg_type)_type == expected_type);

        unpack_buffer(unpacker, args...);
    }

    inline void
    unpack_client_tx(message &m, transaction::pending_tx &tx)
    {
        uint64_t num_tx;
        uint32_t type;
        enum msg_type mtype;
        e::unpacker unpacker = m.buf->unpack_from(BUSYBEE_HEADER_SIZE);
        unpacker = unpacker >> type;
        mtype = (enum msg_type)type;
        assert(mtype == CLIENT_TX_INIT);
        unpack_buffer(unpacker, num_tx);
        while (num_tx-- > 0) {
            auto upd = std::make_shared<transaction::pending_update>();
            tx.writes.emplace_back(upd);
            unpacker = unpacker >> type;
            mtype = (enum msg_type)type;
            switch (mtype) {
                case CLIENT_NODE_CREATE_REQ:
                    upd->type = transaction::NODE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->handle); 
                    break;

                case CLIENT_EDGE_CREATE_REQ:
                    upd->type = transaction::EDGE_CREATE_REQ;
                    unpack_buffer(unpacker, upd->handle, upd->elem1, upd->elem2);
                    break;

                case CLIENT_NODE_DELETE_REQ:
                    upd->type = transaction::NODE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1);
                    break;

                case CLIENT_EDGE_DELETE_REQ:
                    upd->type = transaction::EDGE_DELETE_REQ;
                    unpack_buffer(unpacker, upd->elem1, upd->elem2);
                    break;

                default:
                    WDEBUG << "bad msg type" << std::endl;
            }
        }
    }
}

#endif
