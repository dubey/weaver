/*
 * ===============================================================
 *    Description:  Shard hyperdex stub implementation.
 *
 *        Created:  2014-02-18 15:32:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "db/shard_constants.h"
#include "db/hyper_stub.h"

#define PUT_EDGE_BATCH_SZ 1000
#define FLUSH_CALL_SZ 100000
#define INITIAL_POOL_SZ (FLUSH_CALL_SZ*3)

using db::hyper_stub;
using db::hyper_stub_pool;
using db::apn_ptr_t;
using db::ape_ptr_t;
using db::aai_ptr_t;

template <typename T>
hyper_stub_pool<T> :: hyper_stub_pool()
    : sz(INITIAL_POOL_SZ)
{
    pool.reserve(INITIAL_POOL_SZ);
    for (uint32_t i = 0; i < INITIAL_POOL_SZ; i++) {
        pool.emplace_back(std::make_shared<T>());
    }
}

template <typename T>
uint64_t
hyper_stub_pool<T> :: size()
{
    return sz;
}

template <typename T>
std::shared_ptr<T>
hyper_stub_pool<T> :: acquire()
{
    std::shared_ptr<T> ret;

    if (pool.empty()) {
        for (uint32_t i = 0; i < FLUSH_CALL_SZ; i++) {
            std::shared_ptr<T> new_ptr= std::make_shared<T>();
            pool.emplace_back(new_ptr);
        }
        sz += FLUSH_CALL_SZ;
        WDEBUG << "pool sz=" << sz << std::endl;
    }

    async_call_ptr_t back = pool.back();
    ret = std::static_pointer_cast<T>(back);
    pool.pop_back();

    return ret;
}

template <typename T>
void
hyper_stub_pool<T> :: release(std::shared_ptr<T> ptr)
{
    pool.emplace_back(ptr);
}


hyper_stub :: hyper_stub(uint64_t sid, int tid)
    : shard_id(sid)
    , thread_id(tid)
    , put_edge_batch_clkhand(0)
{
    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        put_edge_batch.emplace_back(ape_pool.acquire());
    }

    hyperdex_client_set_tid(cl, thread_id);
}

void
hyper_stub :: restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
    /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
    po6::threads::mutex *shard_mutexes)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // node list
    const hyperdex_client_attribute_check attr_check = {graph_attrs[0], (const char*)&shard_id, sizeof(int64_t), graph_dtypes[0], HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(cl, graph_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
        return;
    }

    std::vector<node_handle_t> node_list;
    int loop_id;
    bool loop_done = false;
    char *handle_str = (char*)malloc(sizeof(char)*128);
    size_t handle_sz = 128;
    size_t cur_sz;
    node_handle_t node_handle;
    vc::vclock_ptr_t dummy_clock;
    node *n;
    uint64_t map_idx;

    while (!loop_done) {
        // loop until search done
        loop_id = hyperdex_client_loop(cl, -1, &loop_status);
        if (loop_id != call_id
         || loop_status != HYPERDEX_CLIENT_SUCCESS
         || (search_status != HYPERDEX_CLIENT_SUCCESS && search_status != HYPERDEX_CLIENT_SEARCHDONE)) {
            WDEBUG << "Hyperdex function failed, call id = " << call_id
                   << ", loop_id = " << loop_id
                   << ", loop status = " << hyperdex_client_returncode_to_string(loop_status)
                   << ", search status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
            WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
            WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
            return;
        }

        if (search_status == HYPERDEX_CLIENT_SEARCHDONE) {
            loop_done = true;
        } else if (search_status == HYPERDEX_CLIENT_SUCCESS) {
            assert(num_attrs == NUM_GRAPH_ATTRS+1); // node handle + graph attrs

            uint64_t key_idx = UINT64_MAX;
            hyperdex_client_attribute node_attrs[NUM_GRAPH_ATTRS];
            for (uint64_t i = 0, j = 0; i < num_attrs; i++) {
                if (strncmp(cl_attr[i].attr, graph_key, 4) == 0) {
                    key_idx = i;
                } else {
                    node_attrs[j++] = cl_attr[i];
                }
            }
            assert(key_idx != UINT64_MAX);

            // null terminated copy of the obtained handle
            cur_sz = cl_attr[key_idx].value_sz + 1;
            if (handle_sz < cur_sz) {
                while(handle_sz < cur_sz) {
                    handle_sz *= 2;
                }
                handle_str = (char*)realloc(handle_str, handle_sz);
            }
            memcpy(handle_str, cl_attr[key_idx].value, cur_sz-1);
            handle_str[cur_sz-1] = '\0';
            node_handle = node_handle_t(handle_str);

            // recreate node
            map_idx = hash_node_handle(node_handle) % NUM_NODE_MAPS;
            n = new node(node_handle, UINT64_MAX, dummy_clock, shard_mutexes+map_idx);
            recreate_node(node_attrs, *n);

            //XXX edge map
            //for (const auto &p: n->out_edges) {
            //    assert(p.second.size() == 1);
            //    edge *e = p.second.front();
            //    edge_map[e->nbr.handle].emplace(std::make_pair(node_handle, e->base.get_creat_time()));
            //}

            // node map
            auto &node_map = nodes[map_idx];
            assert(node_map.find(node_handle) == node_map.end());
            node_map[node_handle] = std::make_shared<db::node_entry>(n);

            hyperdex_client_destroy_attrs(cl_attr, num_attrs);
        } else {
            WDEBUG << "unexpected search status " << search_status << std::endl;
        }
    }

    free(handle_str);
}

void
hyper_stub :: put_node_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
    std::unordered_map<node_handle_t, node*> &node_map,
    int &progress,
    std::shared_ptr<vc::vclock> last_upd_clk,
    std::shared_ptr<vc::vclock_t> restore_clk)
{
    for (const auto &p: nodes) {
        auto &nodes_vec = p.second->nodes;
        assert(nodes_vec.size() <= 1);
        if (!nodes_vec.empty()) {
            node_map.emplace(p.first, nodes_vec.front());
            if (node_map.size() >= 1000) {
                put_nodes_bulk(node_map, last_upd_clk, restore_clk);
                node_map.clear();
                WDEBUG << "bulk load hyperdex progress " << ++progress << std::endl;
            }
        }
    }
}

void
hyper_stub :: put_index_loop(db::data_map<std::shared_ptr<db::node_entry>> &nodes,
    std::unordered_map<std::string, node*> &idx_add_if_not_exist,
    std::unordered_map<std::string, node*> &idx_add,
    int &ine_progress,
    int &progress)
{
    for (const auto &p: nodes) {
        auto &nodes_vec = p.second->nodes;
        if (!nodes_vec.empty()) {
            node *n = nodes_vec.front();
            idx_add_if_not_exist.emplace(p.first, n);
            for (const node_handle_t &alias: n->aliases) {
                assert(idx_add_if_not_exist.find(alias) == idx_add_if_not_exist.end());
                idx_add_if_not_exist.emplace(alias, n);
            }

            for (auto &x: n->out_edges) {
                assert(x.second.size() == 1);
                assert(idx_add_if_not_exist.find(x.first) == idx_add_if_not_exist.end());
                idx_add_if_not_exist.emplace(x.first, n);
            }

            //if (n->temp_aliases != nullptr) {
            //    for (const std::string &s: *n->temp_aliases) {
            //        idx_add.emplace(s, n);
            //    }
            //    n->done_temp_index();
            //}

            if (idx_add_if_not_exist.size() >= 10000) {
                add_indices(idx_add_if_not_exist, false, true);
                idx_add_if_not_exist.clear();
                WDEBUG << "aux index if not exist progress " << ++ine_progress << std::endl;
            }
            if (idx_add.size() >= 10000) {
                add_indices(idx_add, false, false);
                idx_add.clear();
                WDEBUG << "aux index progress " << ++progress << std::endl;
            }
        }
    }
}

void
hyper_stub :: memory_efficient_bulk_load(int thread_id, db::data_map<std::shared_ptr<db::node_entry>> *nodes_arr)
{
    WDEBUG << "starting HyperDex bulk load." << std::endl;
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    int progress = 0;
    for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
        put_node_loop(nodes_arr[tid], node_map, progress, last_upd_clk, restore_clk);
    }

    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    progress = 0;
    int ine_progress = 0;
    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add_if_not_exist;
        std::unordered_map<std::string, node*> idx_add;

        for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
            put_index_loop(nodes_arr[tid], idx_add_if_not_exist, idx_add, ine_progress, progress);
        }

        add_indices(idx_add_if_not_exist, false, true);
        add_indices(idx_add, false, false);
    }

    WDEBUG << "bulk load done." << std::endl;
}

void
hyper_stub :: memory_efficient_bulk_load(db::data_map<std::shared_ptr<db::node_entry>> &nodes)
{
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    int progress = 0;
    put_node_loop(nodes, node_map, progress, last_upd_clk, restore_clk);
    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    progress = 0;
    int ine_progress = 0;
    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add_if_not_exist;
        std::unordered_map<std::string, node*> idx_add;

        put_index_loop(nodes, idx_add_if_not_exist, idx_add, ine_progress, progress);

        add_indices(idx_add_if_not_exist, false, true);
        add_indices(idx_add, false, false);
    }
}

void
hyper_stub :: bulk_load(int thread_id, std::unordered_map<node_handle_t, std::vector<node*>> *nodes_arr)
{
    assert(NUM_NODE_MAPS % NUM_SHARD_THREADS == 0);
    std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
    std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));

    std::unordered_map<node_handle_t, node*> node_map;
    for (int tid = thread_id; tid < NUM_NODE_MAPS; tid += NUM_SHARD_THREADS) {
        for (const auto &p: nodes_arr[tid]) {
            assert(p.second.size() == 1);
            node_map.emplace(p.first, p.second.front());
        }
    }

    put_nodes_bulk(node_map, last_upd_clk, restore_clk);

    if (AuxIndex) {
        std::unordered_map<std::string, node*> idx_add = node_map;
        for (auto &p: node_map) {
            for (const node_handle_t &alias: p.second->aliases) {
                assert(idx_add.find(alias) == idx_add.end());
                idx_add.emplace(alias, p.second);
            }

            for (auto &x: p.second->out_edges) {
                assert(x.second.size() == 1);
                assert(idx_add.find(x.first) == idx_add.end());
                idx_add.emplace(x.first, p.second);
            }
        }

        add_indices(idx_add, false, true);
    }
}

bool
hyper_stub :: update_mapping(const node_handle_t &handle, uint64_t loc)
{
    return update_nmap(handle, loc);
}

bool
hyper_stub :: recover_node(db::node &n)
{
    std::unordered_map<node_handle_t, db::node*> nodes;
    nodes.emplace(n.get_handle(), &n);
    return get_nodes(nodes, false);
}

bool
hyper_stub :: put_node_no_loop(db::node *n)
{
    if (last_clk_buf == nullptr) {
        std::shared_ptr<vc::vclock> last_upd_clk(new vc::vclock(0,0));
        std::shared_ptr<vc::vclock_t> restore_clk(new vc::vclock_t(last_upd_clk->clock));
        prepare_buffer(last_upd_clk, last_clk_buf);
        prepare_buffer(restore_clk, restore_clk_buf);
    }
    assert(last_clk_buf && restore_clk_buf);

    apn_ptr_t apn = apn_pool.acquire();
    apn->handle = n->get_handle();
    prepare_node(apn->attrs,
                 *n,
                 apn->creat_clk_buf,
                 apn->props_buf,
                 apn->out_edges_buf,
                 last_clk_buf,
                 restore_clk_buf,
                 apn->aliases_buf,
                 apn->num_attrs);

    bool success = call_no_loop(&hyperdex_client_put,
                                graph_space,
                                apn->handle.c_str(),
                                apn->handle.size(),
                                apn->attrs,
                                apn->num_attrs,
                                apn->op_id, apn->status);

    if (success) {
        async_calls.emplace(apn->op_id, apn);
        //WDEBUG << "tid=" << thread_id << "\top_id=" << apn->op_id << " in async_put_node_calls" << std::endl;

        for (const std::string &alias: n->aliases) {
            success = add_index_no_loop(n->get_handle(), alias) && success;
        }
    } else {
        WDEBUG << "hyperdex_client_put failed, op_id=" << apn->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(apn->status) << std::endl;
        WDEBUG << "node=" << apn->handle << std::endl;
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: flush_put_edge(uint32_t evict_idx)
{
    ape_ptr_t ape = put_edge_batch[evict_idx];
    ape->attr = (hyperdex_client_map_attribute*)malloc(ape->batched.size() * sizeof(hyperdex_client_map_attribute));
    prepare_edges(ape->attr, ape->batched);

    bool success;
    success = map_call_no_loop(&hyperdex_client_map_add,
                               graph_space,
                               ape->node_handle.c_str(),
                               ape->node_handle.size(),
                               ape->attr,
                               ape->batched.size(),
                               ape->op_id, ape->status);

    if (success) {
        for (const auto &apeu: ape->batched) {
            if (!apeu.alias.empty()) {
                success = add_index_no_loop(ape->node_handle, apeu.alias) && success;
            }

            if (apeu.del_after_call) {
                delete apeu.e;
            }
        }

        async_calls.emplace(ape->op_id, ape);
        put_edge_batch[evict_idx] = ape_pool.acquire();
        //WDEBUG << "tid=" << thread_id << "\top_id=" << ape->op_id << " in async_put_edge_calls" << std::endl;
    } else {
        WDEBUG << "hyperdex_client_map_add failed, op_id=" << ape->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(ape->status) << std::endl;
        WDEBUG << "node=" << ape->node_handle << ", sample edge=" << ape->batched.front().edge_handle << std::endl;
        abort_bulk_load();
    }

    assert(put_edge_batch[evict_idx]->batched.empty());
    put_edge_batch[evict_idx]->used = false;

    possibly_flush();

    return success;
}

bool
hyper_stub :: put_edge_no_loop(const node_handle_t &node_handle, db::edge *e, const std::string &alias, bool del_after_call)
{
    // search for this batch in cache
    uint32_t found_idx = UINT32_MAX;
    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        const ape_ptr_t ape = put_edge_batch[i];
        if (ape->node_handle == node_handle) {
            found_idx = i;
            break;
        }
    }

    uint32_t free_idx = UINT32_MAX;
    uint32_t evict_idx = UINT32_MAX;
    if (found_idx == UINT32_MAX) {
        // find slot for this entry
        // may have to evict
        uint32_t i;
        for (i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
            uint32_t idx = (put_edge_batch_clkhand+i) % PUT_EDGE_BATCH_SZ;
            ape_ptr_t ape = put_edge_batch[idx];
            if (ape->batched.empty()) {
                free_idx = idx;
                put_edge_batch_clkhand = (idx+1) % PUT_EDGE_BATCH_SZ;
                break;
            } else if (!ape->used) {
                evict_idx = idx;
                put_edge_batch_clkhand = (idx+1) % PUT_EDGE_BATCH_SZ;
                break;
            } else {
                ape->used = false;
            }
        }

        // no free slot, evict the clock hand
        if (free_idx == UINT32_MAX && evict_idx == UINT32_MAX) {
            evict_idx = put_edge_batch_clkhand;
            ++put_edge_batch_clkhand;
        }
    }

    assert(found_idx != UINT32_MAX 
        || free_idx != UINT32_MAX
        || evict_idx != UINT32_MAX);

    bool success = true;
    uint32_t use_idx;
    if (found_idx != UINT32_MAX) {
        use_idx = found_idx;
    } else if (free_idx != UINT32_MAX) {
        use_idx = free_idx;
    } else {
        use_idx = evict_idx;

        // flush this batch to HyperDex
        success = flush_put_edge(evict_idx);
    }

    ape_ptr_t cur_ape = put_edge_batch[use_idx];
    cur_ape->used = true;
    cur_ape->node_handle = node_handle;
    async_put_edge_unit cur_apeu;
    cur_apeu.e = e;
    cur_apeu.edge_handle = e->get_handle();
    cur_apeu.alias = alias;
    cur_apeu.del_after_call = del_after_call;
    cur_ape->batched.emplace_back(std::move(cur_apeu));

    return success;
}

bool
hyper_stub :: add_index_no_loop(const node_handle_t &node_handle, const std::string &alias)
{
    aai_ptr_t aai = aai_pool.acquire();
    aai->node_handle = node_handle;
    aai->alias = alias;
    // node handle
    aai->index_attrs[0].attr = index_attrs[0];
    aai->index_attrs[0].value = aai->node_handle.c_str();
    aai->index_attrs[0].value_sz = aai->node_handle.size();
    aai->index_attrs[0].datatype = index_dtypes[0];
    // shard
    aai->index_attrs[1].attr = index_attrs[1];
    aai->index_attrs[1].value = (const char*)&shard_id;
    aai->index_attrs[1].value_sz = sizeof(int64_t);
    aai->index_attrs[1].datatype = index_dtypes[1];

    bool success = call_no_loop(&hyperdex_client_put,
                                index_space,
                                aai->alias.c_str(),
                                aai->alias.size(),
                                aai->index_attrs, NUM_INDEX_ATTRS,
                                aai->op_id, aai->status);

    if (success) {
        async_calls.emplace(aai->op_id, aai);
        //WDEBUG << "tid=" << thread_id << "\top_id=" << aai->op_id << " in async_add_index_calls" << std::endl;
    } else {
        WDEBUG << "hyperdex_client_put failed, op_id=" << aai->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(aai->status) << std::endl;
        WDEBUG << "alias=" << aai->alias << ", node=" << aai->node_handle << std::endl;
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: flush_all_put_edge()
{
    bool success;

    for (uint32_t i = 0; i < PUT_EDGE_BATCH_SZ; i++) {
        if (!put_edge_batch[i]->batched.empty()) {
            success = flush_put_edge(i) && success;
        }
    }

    return success;
}

bool
hyper_stub :: loop_async(uint64_t outstanding_ops)
{
    bool success = true;
    uint64_t initial_ops = outstanding_ops;
    uint64_t loop_count = 0;

    while (outstanding_ops > 0) {
        int64_t op_id;
        hyperdex_client_returncode loop_code;

        loop(op_id, loop_code);
        if (op_id < 0) {
            if (loop_code == HYPERDEX_CLIENT_TIMEOUT) {
                continue;
            } else {
                WDEBUG << "hyperdex_client_loop failed, op_id=" << op_id
                       << ", loop_code=" << hyperdex_client_returncode_to_string(loop_code) << std::endl;
                abort_bulk_load();
            }
        }

        auto find_iter = async_calls.find(op_id);
        if (find_iter == async_calls.end()) {
            WDEBUG << "hyperdex_client_loop returned op_id=" << op_id
                   << " and loop_code=" << hyperdex_client_returncode_to_string(loop_code)
                   << " which was not found in async_calls_map." << std::endl;
            abort_bulk_load();
        }

        async_call_ptr_t ac_ptr = find_iter->second;
        switch (ac_ptr->status) {
            case HYPERDEX_CLIENT_SUCCESS:
                switch (ac_ptr->type) {
                    case PUT_NODE:
                        apn_pool.release(std::static_pointer_cast<async_put_node>(ac_ptr));
                        break;

                    case PUT_EDGE:
                        ape_pool.release(std::static_pointer_cast<async_put_edge>(ac_ptr));
                        break;

                    case ADD_INDEX:
                        aai_pool.release(std::static_pointer_cast<async_add_index>(ac_ptr));
                        break;
                }
                async_calls.erase(op_id);
                outstanding_ops--;
                break;

            default:
                WDEBUG << "Unexpected hyperdex op code, here are some details." << std::endl;
                WDEBUG << "type=" << async_call_type_to_string(ac_ptr->type)
                       << ", call_code=" << hyperdex_client_returncode_to_string(ac_ptr->status) << std::endl;
                abort_bulk_load();
        }

            //WDEBUG << "unsuccessful loop, op_id=" << op_id << "\treturncode=" << hyperdex_client_returncode_to_string(code) << std::endl;
            // XXX don't erase if interrupted or can loop again later
            //if (async_put_node_calls.find(op_id) != async_put_node_calls.end()) {
            //    apn_ptr_t apn = async_put_node_calls[op_id];
            //    async_put_node_calls.erase(op_id);
            //    //WDEBUG << "tid=" << thread_id << "\top_id=" << op_id << " deleting" << std::endl;

            //    if (code == HYPERDEX_CLIENT_COORDFAIL
            //     || code == HYPERDEX_CLIENT_SERVERERROR
            //     || code == HYPERDEX_CLIENT_RECONFIGURE
            //     || code == HYPERDEX_CLIENT_INTERRUPTED
            //     || code == HYPERDEX_CLIENT_CLUSTER_JUMP
            //     || code == HYPERDEX_CLIENT_OFFLINE) {
            //        bool cur_success = call_no_loop(&hyperdex_client_put,
            //                                        graph_space,
            //                                        apn->handle.c_str(),
            //                                        apn->handle.size(),
            //                                        apn->attrs,
            //                                        apn->num_attrs,
            //                                        apn->op_id, apn->status);
            //        if (cur_success) {
            //            async_put_node_calls.emplace(apn->op_id, apn);
            //            //WDEBUG << "tid=" << thread_id << "\there op_id=" << apn->op_id << std::endl;
            //        } else {
            //            apn_count--;
            //            success = false;
            //            release_apn_ptr(apn);
            //        }
            //    } else {
            //        success = false;
            //        release_apn_ptr(apn);
            //    }

            //    found = true;
            //} else if (async_put_edge_calls.find(op_id) != async_put_edge_calls.end()) {
            //    ape_ptr_t ape = async_put_edge_calls[op_id];
            //    async_put_edge_calls.erase(op_id);
            //    //WDEBUG << "tid=" << thread_id << "\top_id=" << op_id << " deleting" << std::endl;

            //    if (code == HYPERDEX_CLIENT_COORDFAIL
            //     || code == HYPERDEX_CLIENT_SERVERERROR
            //     || code == HYPERDEX_CLIENT_RECONFIGURE
            //     || code == HYPERDEX_CLIENT_INTERRUPTED
            //     || code == HYPERDEX_CLIENT_CLUSTER_JUMP
            //     || code == HYPERDEX_CLIENT_OFFLINE) {
            //        bool cur_success = map_call_no_loop(&hyperdex_client_map_add,
            //                                            graph_space,
            //                                            ape->node_handle.c_str(),
            //                                            ape->node_handle.size(),
            //                                            ape->attr,
            //                                            ape->batched.size(),
            //                                            ape->op_id, ape->status);

            //        if (cur_success) {
            //            async_put_edge_calls.emplace(ape->op_id, ape);
            //            //WDEBUG << "tid=" << thread_id << "\there op_id=" << ape->op_id << std::endl;
            //        } else {
            //            ape_count--;
            //            success = false;
            //            release_ape_ptr(ape);
            //        }
            //    } else {
            //        success = false;
            //        release_ape_ptr(ape);
            //    }

            //    found = true;
            //} else if (async_add_index_calls.find(op_id) != async_add_index_calls.end()) {
            //    aai_ptr_t aai = async_add_index_calls[op_id];
            //    async_add_index_calls.erase(op_id);
            //    //WDEBUG << "tid=" << thread_id << "\top_id=" << op_id << " deleting" << std::endl;

            //    if (code == HYPERDEX_CLIENT_COORDFAIL
            //     || code == HYPERDEX_CLIENT_SERVERERROR
            //     || code == HYPERDEX_CLIENT_RECONFIGURE
            //     || code == HYPERDEX_CLIENT_INTERRUPTED
            //     || code == HYPERDEX_CLIENT_CLUSTER_JUMP
            //     || code == HYPERDEX_CLIENT_OFFLINE) {
            //        bool cur_success = call_no_loop(&hyperdex_client_put,
            //                                        index_space,
            //                                        aai->alias.c_str(),
            //                                        aai->alias.size(),
            //                                        aai->index_attrs, NUM_INDEX_ATTRS,
            //                                        aai->op_id, aai->status);

            //        if (cur_success) {
            //            async_add_index_calls.emplace(aai->op_id, aai);
            //            //WDEBUG << "tid=" << thread_id << "\there op_id=" << aai->op_id << std::endl;
            //        } else {
            //            aai_count--;
            //            success = false;
            //            release_aai_ptr(aai);
            //        }
            //    } else {
            //        success = false;
            //        release_aai_ptr(aai);
            //    }

            //    found = true;
            //}

        if (++loop_count > 10*initial_ops) {
            WDEBUG << "exceeded max loops, loop_count=" << loop_count << ", initial_ops=" << initial_ops << std::endl;
            assert(false);
        }
    }

    return success;
}

bool
hyper_stub :: loop_async_calls(bool flush)
{
    bool success;

    if (flush) {
        success = loop_async(async_calls.size());

        apn_pool.clear();
        ape_pool.clear();
        aai_pool.clear();
    } else {
        WDEBUG << "PRE  tid=" << thread_id << "\t#async_calls=" << async_calls.size() << std::endl;

        success = loop_async(async_calls.size());

        WDEBUG << "POST tid=" << thread_id << "\t#async_calls=" << async_calls.size() << std::endl;

        //// flush first ~50% calls
        //if (async_calls.size() > FLUSH_CALL_SZ) {
        //    uint32_t num_loop = async_calls.size() - FLUSH_CALL_SZ/2;
        //    success = loop_async(num_loop);
        //}
    }

    return success;
}

// prevent too many outstanding requests
void
hyper_stub :: possibly_flush()
{
    if (async_calls.size() > FLUSH_CALL_SZ) {
        loop_async_calls(false);
    }
}

void
hyper_stub :: abort_bulk_load()
{
    WDEBUG << "Aborting bulk load now." << std::endl;
    assert(false);
}

#undef weaver_debug_
