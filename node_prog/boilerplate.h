/*
 * ===============================================================
 *    Description:  Boilerplate declarations and definitions for
 *                  node programs
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_boilerplate_h_
#define weaver_node_prog_boilerplate_h_

#include <memory>
#include <vector>
#include <deque>

#include "common/message.h"
#include "node_prog/base_classes.h"
#include "node_prog/node.h"
#include "db/remote_node.h"

#define PROG_FUNC_DECLARE \
    std::shared_ptr<Node_Parameters_Base> param_ctor(); \
    std::shared_ptr<Node_State_Base> state_ctor(); \
    \
    uint64_t param_size(const Node_Parameters_Base&, void*); \
    void param_pack(const Node_Parameters_Base&, e::packer&, void*); \
    void param_unpack(Node_Parameters_Base&, e::unpacker&, void*); \
    \
    uint64_t state_size(const Node_State_Base&, void*); \
    void state_pack(const Node_State_Base&, e::packer&, void*); \
    void state_unpack(Node_State_Base&, e::unpacker&, void*); \
    \
    std::pair<search_type, std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>>> \
    node_program(node &n, \
        db::remote_node &rn, \
        std::shared_ptr<Node_Parameters_Base> param_ptr, \
        std::function<Node_State_Base&()> state_getter);

#define CAST_ARG_REF(type, arg) \
    type &tp = dynamic_cast<type&>(p);

#define PROG_FUNC_DEFINE(PREFIX) \
    std::shared_ptr<Node_Parameters_Base> \
    param_ctor() \
    { \
        auto new_params = std::make_shared<PREFIX##_params>(); \
        return std::dynamic_pointer_cast<Node_Parameters_Base>(new_params); \
    } \
    \
    std::shared_ptr<Node_State_Base> \
    state_ctor() \
    { \
        auto new_state = std::make_shared<PREFIX##_state>(); \
        return std::dynamic_pointer_cast<Node_State_Base>(new_state); \
    } \
    \
    uint64_t \
    param_size(const Node_Parameters_Base &p, void *aux_args) \
    { \
        CAST_ARG_REF(const PREFIX##_params, p); \
        return tp.size(aux_args); \
    } \
    \
    void \
    param_pack(const Node_Parameters_Base &p, e::packer &packer, void *aux_args) \
    { \
        CAST_ARG_REF(const PREFIX##_params, p); \
        tp.pack(packer, aux_args); \
    } \
    \
    void \
    param_unpack(Node_Parameters_Base &p, e::unpacker &unpacker, void *aux_args) \
    { \
        CAST_ARG_REF(PREFIX##_params, p); \
        tp.unpack(unpacker, aux_args); \
    } \
    \
    uint64_t \
    state_size(const Node_State_Base &p, void *aux_args) \
    { \
        CAST_ARG_REF(const PREFIX##_state, p); \
        return tp.size(aux_args); \
    } \
    \
    void \
    state_pack(const Node_State_Base &p, e::packer &packer, void *aux_args) \
    { \
        CAST_ARG_REF(const PREFIX##_state, p); \
        tp.pack(packer, aux_args); \
    } \
    \
    void \
    state_unpack(Node_State_Base &p, e::unpacker &unpacker, void *aux_args) \
    { \
        CAST_ARG_REF(PREFIX##_state, p); \
        tp.unpack(unpacker, aux_args); \
    }


#endif
