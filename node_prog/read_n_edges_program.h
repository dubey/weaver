/*
 * ===============================================================
 *    Description:  Program to read the 'top' n edges, used to
 *                  simulate range queries for soc. net. workload.
 *
 *        Created:  2014-04-15 21:30:10
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_read_n_edges_program_h_
#define weaver_node_prog_read_n_edges_program_h_

namespace node_prog
{
    class read_n_edges_params : public virtual Node_Parameters_Base 
    {
        public:
            uint64_t num_edges;
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
                uint64_t toRet = message::size(num_edges)
                    + message::size(edges_props)
                    + message::size(return_edges);
                return toRet;
            }

            virtual void pack(e::buffer::packer& packer) const 
            {
                message::pack_buffer(packer, num_edges);
                message::pack_buffer(packer, edges_props);
                message::pack_buffer(packer, return_edges);
            }

            virtual void unpack(e::unpacker& unpacker)
            {
                message::unpack_buffer(unpacker, num_edges);
                message::unpack_buffer(unpacker, edges_props);
                message::unpack_buffer(unpacker, return_edges);
            }
    };

    struct read_n_edges_state : public virtual Node_State_Base
    {
        virtual ~read_n_edges_state() { }
        virtual uint64_t size() const { return 0; }
        virtual void pack(e::buffer::packer&) const { }
        virtual void unpack(e::unpacker&) { }
    };

    inline std::vector<std::pair<db::element::remote_node, read_n_edges_params>> 
    read_n_edges_node_program(
            node &n,
            db::element::remote_node &,
            read_n_edges_params &params,
            std::function<read_n_edges_state&()>,
            std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
                std::shared_ptr<std::vector<db::element::remote_node>>, uint64_t)>&,
            cache_response<Cache_Value_Base>*)
    {
        auto elist = n.get_edges();
        int pushcnt = 0;
        for (edge &e : elist) {
            if (e.has_all_properties(params.edges_props)) {
                pushcnt++;
                params.return_edges.emplace_back(e.get_id());
                if (--params.num_edges == 0) {
                    break;
                }
            } else {
            }
        }
        return {std::make_pair(db::element::coordinator, std::move(params))};
    }
}

#endif
