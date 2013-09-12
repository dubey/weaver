/*
 * ===============================================================
 *    Description:  Base classes, typedefs, and necessary enum
 *                  declarations for all node programs. 
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
        DEFAULT,
        REACHABILITY,
        DIJKSTRA,
        CLUSTERING
    };

    template <typename params_type, typename node_state_type>
    struct node_function_type
    {
        public:
            typedef std::vector<std::pair<db::element::remote_node, params_type>> (*value_type)(uint64_t, // req_id
                db::element::node&, // this node
                db::element::remote_node&, // this remote node
                params_type&,
                std::function<node_state_type&()>);
    };

    template <typename params_type, typename node_state_type>
    struct deleted_node_function_type
    {
        public:
            typedef std::vector<std::pair<db::element::remote_node, params_type>> (*value_type)(uint64_t, // req_id
                db::element::node&, // node who asked to go to deleted node
                uint64_t, // handle of node that didn't exist
                params_type&, // params we had sent to deleted node
                std::function<node_state_type&()>);
    };

    class Packable 
    {
        public:
            virtual uint64_t size() const  = 0;
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

    class Packable_Deletable : public Packable, public Deletable { };
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
