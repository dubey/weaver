/*
 * ===============================================================
 *    Description:  Node program to read properties of a single node
 *
 *        Created:  Friday 17 January 2014 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef weaver_node_prog_two_neighborhood_program_h 
#define weaver_node_prog_two_neighborhood_program_h 

#include <vector>
#include <string>

#include "node_prog/base_classes.h"
#include "db/element/remote_node.h"
#include "node_prog/node.h"
#include "node_prog/cache_response.h"

namespace node_prog
{
    class two_neighborhood_params : public virtual Node_Parameters_Base 
    {
        public:
            bool _search_cache;
            bool cache_update;
            std::string prop_key;
            uint32_t on_hop;
            bool outgoing;
            db::element::remote_node prev_node;
            std::vector<std::pair<uint64_t, std::string>> responses;

        public:
            bool search_cache();
            uint64_t cache_key();
            uint64_t size() const;
            void pack(e::buffer::packer& packer) const;
            void unpack(e::unpacker& unpacker);
    };

    struct two_neighborhood_state : public virtual Node_State_Base
    {
        bool one_hop_visited;
        bool two_hop_visited;
        uint32_t responses_left;
        db::element::remote_node prev_node;
        std::vector<std::pair<uint64_t, std::string>> responses;

        two_neighborhood_state();
        ~two_neighborhood_state() { }
        uint64_t size() const;
        void pack(e::buffer::packer& packer) const;
        void unpack(e::unpacker& unpacker);
    };

    struct two_neighborhood_cache_value : public virtual Cache_Value_Base 
    {
        std::string prop_key;
        std::vector<std::pair<uint64_t, std::string>> responses;

        two_neighborhood_cache_value(std::string &prop_key, std::vector<std::pair<uint64_t, std::string>> &responses);
        ~two_neighborhood_cache_value () { }
        uint64_t size() const;
        void pack(e::buffer::packer& packer) const;
        void unpack(e::unpacker& unpacker);
    };

    std::pair<search_type, std::vector<std::pair<db::element::remote_node, two_neighborhood_params>>>
    two_neighborhood_node_program(
            node &,
            db::element::remote_node &,
            two_neighborhood_params &,
            std::function<two_neighborhood_state&()>,
            std::function<void(std::shared_ptr<two_neighborhood_cache_value>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)> &,
            cache_response<two_neighborhood_cache_value> *);
}

#endif
