/*
 * ===============================================================
 *    Description:  Get all edges from the node to a particular
 *                  vertex which have specified properties.
 *
 *        Created:  2014-04-17 11:33:57
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_edge_get_program_h_
#define weaver_node_prog_edge_get_program_h_

namespace node_prog
{
    class edge_get_params : public virtual Node_Parameters_Base 
    {
        public:
            uint64_t nbr_id;
            std::vector<std::pair<std::string, std::string>> edges_props;
            std::vector<uint64_t> return_edges;

        public:
            virtual bool search_cache() {
                return false; // would never need to cache
            }

            virtual uint64_t cache_key() {
                return 0;
            }

            virtual uint64_t size() const 
            {
                uint64_t toRet = message::size(nbr_id)
                    + message::size(edges_props)
                    + message::size(return_edges);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, nbr_id);
                message::pack_buffer(packer, edges_props);
                message::pack_buffer(packer, return_edges);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, nbr_id);
                message::unpack_buffer(unpacker, edges_props);
                message::unpack_buffer(unpacker, return_edges);
            }
    };

    struct edge_get_state : public virtual Node_State_Base
    {
        virtual ~edge_get_state() { }
        virtual uint64_t size() const { return 0; }
        virtual void pack(e::buffer::packer&) const { }
        virtual void unpack(e::unpacker&) { }
    };

    inline std::vector<std::pair<db::element::remote_node, edge_get_params>> 
    edge_get_node_program(
            node &n,
            db::element::remote_node &,
            edge_get_params &params,
            std::function<edge_get_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        auto elist = n.get_edges();
        for (edge &e : elist) {
            if (e.get_neighbor().id == params.nbr_id
             && e.has_all_properties(params.edges_props)) {
                params.return_edges.emplace_back(e.get_id());
            }
        }
        return {std::make_pair(db::element::coordinator, std::move(params))};
    }
}

#endif
