/*
 * ===============================================================
 *    Description:  N-gram path matching implementation.
 *
 *         Author:  Ted Yin, ted.sybil@gmail.com
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/n_gram_path.h"

using node_prog::search_type;
using node_prog::n_gram_path_params;
using node_prog::n_gram_path_state;
using node_prog::cache_response;
using node_prog::Cache_Value_Base;

// params
n_gram_path_params :: n_gram_path_params()
    : step(0)
{
}

uint64_t
n_gram_path_params :: size() const
{
    return message::size(node_preds)
         + message::size(edge_preds)
         + message::size(progress)
         + message::size(coord)
         + message::size(remaining_path)
         + message::size(step)
         + message::size(unigram)
         ;
}

void
n_gram_path_params :: pack(e::packer &packer) const
{
    message::pack_buffer(packer, node_preds);
    message::pack_buffer(packer, edge_preds);
    message::pack_buffer(packer, progress);
    message::pack_buffer(packer, coord);
    message::pack_buffer(packer, remaining_path);
    message::pack_buffer(packer, step);
    message::pack_buffer(packer, unigram);
}

void
n_gram_path_params :: unpack(e::unpacker &unpacker)
{
    message::unpack_buffer(unpacker, node_preds);
    message::unpack_buffer(unpacker, edge_preds);
    message::unpack_buffer(unpacker, progress);
    message::unpack_buffer(unpacker, coord);
    message::unpack_buffer(unpacker, remaining_path);
    message::unpack_buffer(unpacker, step);
    message::unpack_buffer(unpacker, unigram);
}

n_gram_path_state :: n_gram_path_state()
{ }

uint64_t
n_gram_path_state :: size() const
{
    return 0;
}

void
n_gram_path_state :: pack(e::packer &packer) const
{
    (void)packer;
}

void
n_gram_path_state :: unpack(e::unpacker &unpacker)
{
    (void)unpacker;
}

std::pair<search_type, std::vector<std::pair<db::remote_node, n_gram_path_params>>>
node_prog :: n_gram_path_node_program(node_prog::node &n,
   db::remote_node &rn,
   n_gram_path_params &params,
   std::function<n_gram_path_state&()> state_getter,
   std::function<void(std::shared_ptr<Cache_Value_Base>, std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
   cache_response<Cache_Value_Base>*)
{
    (void)rn;
    (void)state_getter;
    /* node progs to trigger next */
    std::vector<std::pair<db::remote_node, n_gram_path_params>> next;
    node_handle_t cur_handle = n.get_handle();

    fprintf(stderr, "Propagate to %s\n", cur_handle.c_str());
    // visit this node now
    if (!n.has_all_predicates(params.node_preds)) {
        params.progress.clear();
        next.emplace_back(std::make_pair(params.coord, params));
    } else {
        /* already reaches the target */
        if (params.remaining_path.empty()) {
            if (params.unigram) {
                for (edge &e: n.get_edges()) {
                    const db::remote_node &nbr = e.get_neighbor();
                    if (e.has_all_predicates(params.edge_preds)) {
                        uint32_t doc_id = -1;
                        for (auto iter: e.get_properties()) {
                            if (iter[0]->key == "doc_id")
                                doc_id = std::stoul(iter[0]->value);
                        }
                        if (doc_id == (uint32_t)-1)
                        {
                            WDEBUG << "doc_id property not found for edge: " +
                                cur_handle + " -> " + nbr.handle + "\n";
                            continue;
                        }
                        params.progress.emplace(doc_id, 0);
                    }
                }
                next.emplace_back(std::make_pair(params.coord, params));
            } else {
                WDEBUG << "Hit destination\n";
                next.emplace_back(std::make_pair(params.coord, params));
            }
        } else {
            db::remote_node *next_node = nullptr;
            params.step++;
            std::unordered_map<uint32_t, uint32_t> new_progress;
            for (edge &e: n.get_edges()) {
                const db::remote_node &nbr = e.get_neighbor();
                if (nbr.handle == params.remaining_path[0] &&
                    e.has_all_predicates(params.edge_preds)) {
                    uint32_t doc_id = -1;
                    uint32_t doc_pos = -1;
                    for (auto iter: e.get_properties()) {
                        if (iter[0]->key == "doc_id")
                            doc_id = std::stoul(iter[0]->value);
                        else if (iter[0]->key == "doc_pos")
                            doc_pos = std::stoul(iter[0]->value);
                    }
                    if (doc_id == (uint32_t)-1 || doc_pos == (uint32_t)-1)
                    {
                        WDEBUG << "doc_id/doc_pos property not found for edge: " +
                            cur_handle + " -> " + nbr.handle + "\n";
                        continue;
                    }
                    auto iter = params.progress.find(doc_id);
                    if ((iter != params.progress.end() &&
                        iter->second + 1 == doc_pos) ||
                        (params.step == 1))
                        new_progress[doc_id] = doc_pos;
                    if (!next_node)
                        next_node = new db::remote_node(nbr);
                }
            }
            if (next_node) {
                params.progress = new_progress;
                params.remaining_path = std::vector<node_handle_t>(
                                            params.remaining_path.begin() + 1,
                                            params.remaining_path.end());
                fprintf(stderr, "== progress at node %s ===\n", cur_handle.c_str());
                for (const auto &ref: params.progress)
                    fprintf(stderr, "%u: %u\n", ref.first, ref.second);
                fprintf(stderr, "==========================\n");
                next.emplace_back(std::make_pair(*next_node, params));
                delete next_node;
            }
            else {
                params.progress.clear();
                next.emplace_back(std::make_pair(params.coord, params));
            }
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}
