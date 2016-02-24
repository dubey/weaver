/*
 * ===============================================================
 *    Description:  Weaver data structures serialization headers
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_weaver_serialization_h_
#define weaver_common_weaver_serialization_h_

#include <stdint.h>
#include <memory>
#include <e/serialization.h>

namespace vc
{
    class vclock;
}

namespace transaction
{
    class pending_tx;
    class pending_update;
    class nop_data;
}

namespace predicate
{
    class prop_predicate;
}

namespace node_prog
{
    class Node_Parameters_Base;
    class Node_State_Base;
    class Cache_Value_Base;
    class property;
    struct edge_cache_context;
    struct node_cache_context;
}

namespace db
{
    class property;
    class remote_node;
    class element;
    class edge;
    class node;
}

namespace cl
{
    class node;
    class edge;
}

namespace message
{
    uint64_t size(void*, const vc::vclock &t);
    uint64_t size(void*, const transaction::pending_tx &t);
    uint64_t size(void*, const std::shared_ptr<transaction::pending_update> &ptr_t);
    uint64_t size(void*, const std::shared_ptr<transaction::nop_data> &ptr_t);
    uint64_t size(void*, const predicate::prop_predicate&);
    uint64_t size(void*, const std::shared_ptr<node_prog::Node_Parameters_Base> &t);
    uint64_t size(void*, const std::shared_ptr<node_prog::Node_State_Base> &t);
    uint64_t size(void*, const node_prog::Cache_Value_Base &t);
    uint64_t size(void*, const node_prog::property &t);
    uint64_t size(void*, const node_prog::node_cache_context &t);
    uint64_t size(void*, const node_prog::edge_cache_context &t);
    uint64_t size(void*, const db::property &t);
    uint64_t size(void*, const db::remote_node &t);
    uint64_t size(void*, const db::element &t);
    uint64_t size(void*, const db::edge &t);
    uint64_t size(void*, const db::edge* const &t);
    uint64_t size(void*, const db::node &t);
    uint64_t size(void*, const cl::node &t);
    uint64_t size(void*, const cl::edge &t);

    void pack_buffer(e::packer&, void*, const vc::vclock &t);
    void pack_buffer(e::packer&, void*, const transaction::pending_tx &t);
    void pack_buffer(e::packer&, void*, const std::shared_ptr<transaction::pending_update> &ptr_t);
    void pack_buffer(e::packer&, void*, const std::shared_ptr<transaction::nop_data> &ptr_t);
    void pack_buffer(e::packer&, void*, const predicate::prop_predicate &t);
    void pack_buffer(e::packer&, void*, const std::shared_ptr<node_prog::Node_Parameters_Base> &t);
    void pack_buffer(e::packer&, void*, const std::shared_ptr<node_prog::Node_State_Base> &t);
    void pack_buffer(e::packer&, void*, const node_prog::Cache_Value_Base *&t);
    void pack_buffer(e::packer&, void*, const node_prog::property &t);
    void pack_buffer(e::packer&, void*, const node_prog::node_cache_context &t);
    void pack_buffer(e::packer&, void*, const node_prog::edge_cache_context &t);
    void pack_buffer(e::packer&, void*, const db::property &t);
    void pack_buffer(e::packer&, void*, const db::remote_node &t);
    void pack_buffer(e::packer&, void*, const db::element &t);
    void pack_buffer(e::packer&, void*, const db::edge &t);
    void pack_buffer(e::packer&, void*, const db::edge* const &t);
    void pack_buffer(e::packer&, void*, const db::node &t);
    void pack_buffer(e::packer&, void*, const cl::node &t);
    void pack_buffer(e::packer&, void*, const cl::edge &t);

    void unpack_buffer(e::unpacker&, void*, vc::vclock &t);
    void unpack_buffer(e::unpacker&, void*, transaction::pending_tx &t);
    void unpack_buffer(e::unpacker&, void*, std::shared_ptr<transaction::pending_update> &ptr_t);
    void unpack_buffer(e::unpacker&, void*, std::shared_ptr<transaction::nop_data> &ptr_t);
    void unpack_buffer(e::unpacker&, void*, predicate::prop_predicate &t);
    void unpack_buffer(e::unpacker&, void*, std::shared_ptr<node_prog::Node_Parameters_Base> &t);
    void unpack_buffer(e::unpacker&, void*, std::shared_ptr<node_prog::Node_State_Base> &t);
    void unpack_buffer(e::unpacker&, void*, node_prog::Cache_Value_Base &t);
    void unpack_buffer(e::unpacker&, void*, node_prog::property &t);
    void unpack_buffer(e::unpacker&, void*, node_prog::node_cache_context &t);
    void unpack_buffer(e::unpacker&, void*, node_prog::edge_cache_context &t);
    void unpack_buffer(e::unpacker&, void*, db::property &t);
    void unpack_buffer(e::unpacker&, void*, db::remote_node& t);
    void unpack_buffer(e::unpacker&, void*, db::element &t);
    void unpack_buffer(e::unpacker&, void*, db::edge &t);
    void unpack_buffer(e::unpacker&, void*, db::edge *&t);
    void unpack_buffer(e::unpacker&, void*, db::node &t);
    void unpack_buffer(e::unpacker&, void*, cl::node &t);
    void unpack_buffer(e::unpacker&, void*, cl::edge &t);
}

#endif
