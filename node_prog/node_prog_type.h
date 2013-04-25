/*
 * ===============================================================
 *    Description:  something
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill, dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __NODE_PROG_TYPE__
#define __NODE_PROG_TYPE__

#include <vector>
#include <map>
#include <unordered_map>
#include "db/element/node.h"
#include "db/element/remote_node.h"

namespace node_prog
{
    enum prog_type
    {
        REACHABILITY = 0,
        DIJKSTRA,
        CLUSTERING
    };

    template <typename params_type, typename node_state_type, typename cache_value_type>
    struct node_function_type
    {
        public:
            typedef std::vector<std::pair<db::element::remote_node, params_type>> (*value_type)(uint64_t, // req_id
                db::element::node&, // this node
                db::element::remote_node&, // this remote node
                params_type&,
                std::function<node_state_type&()>,
                std::function<cache_value_type&()>);
    };

    class Packable 
    {
        public:
            virtual size_t size() const  = 0;
            virtual void pack(e::buffer::packer& packer) const = 0;
            virtual void unpack(e::unpacker& unpacker) = 0;
    };

    class Deletable 
    {
        public:
            virtual ~Deletable() = 0;
    };

    Deletable::~Deletable() 
    { 
        /* destructor must be defined */ 
    }
}

namespace std
{
    // used if we want a hash table with a prog type as the key
    template <>
    struct hash<node_prog::prog_type>
    {
        public:
            size_t operator()(node_prog::prog_type x) const throw() 
            {
                return hash<int>()(x);
            }
    };
}

#endif //__NODE_PROG_TYPE__
