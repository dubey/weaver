/*
 * ===============================================================
 *    Description:  Count number of edges at a node.
 *
 *        Created:  2014-04-16 17:41:08
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_count_program_h_
#define weaver_node_prog_edge_count_program_h_

namespace node_prog
{
    class edge_count_params : public virtual Node_Parameters_Base 
    {
        public:
            std::vector<std::pair<std::string, std::string>> edges_props;
            uint64_t edge_count;

        public:
            virtual bool search_cache() {
                return false; // would never need to cache
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(edges_props)
                    + message::size(edge_count);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, edges_props);
                message::pack_buffer(packer, edge_count);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, edges_props);
                message::unpack_buffer(unpacker, edge_count);
            }
    };

    struct edge_count_state : public virtual Node_State_Base
    {
        virtual ~edge_count_state() { }
        virtual uint64_t size() const { return 0; }
        virtual void pack(e::buffer::packer&) const { }
        virtual void unpack(e::unpacker&) { }
    };

    inline std::pair<search_type, std::vector<std::pair<db::element::remote_node, edge_count_params>>>
    edge_count_node_program(
            node &n,
            db::element::remote_node &,
            edge_count_params &params,
            std::function<edge_count_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        auto elist = n.get_edges();
        params.edge_count = 0;
        for (edge &e: elist) {
            if (e.has_all_properties(params.edges_props)) {
                params.edge_count++;
            }
        }

        return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::element::remote_node, edge_count_params>>
                (1, std::make_pair(db::element::coordinator, std::move(params)))); 
    }
}

#endif
