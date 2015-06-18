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
#include "db/utils.h"
#include "db/hyper_stub.h"

#define PUT_EDGE_BUFFER_SZ 5000
#define NUM_EDGES_BUFFERED_PER_NODE 100
#define OUTSTANDING_HD_OPS 10000
#define MAX_TIMEOUTS 3000 // 5 mins of waiting total
#define INITIAL_POOL_SZ (OUTSTANDING_HD_OPS*3)

using db::hyper_stub;
using db::hyper_stub_pool;
using db::apn_ptr_t;
using db::apes_ptr_t;
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
        for (uint32_t i = 0; i < OUTSTANDING_HD_OPS; i++) {
            std::shared_ptr<T> new_ptr= std::make_shared<T>();
            pool.emplace_back(new_ptr);
        }
        sz += OUTSTANDING_HD_OPS;
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
    if (ptr->type == PUT_EDGE_SET) {
        async_call_ptr_t ac_ptr = ptr;
        apes_ptr_t apes = std::static_pointer_cast<async_put_edge_set>(ac_ptr);
        apes->node_handle.clear();
        apes->batched.clear();
        free(apes->attr);
    }
    pool.emplace_back(ptr);
}


hyper_stub :: hyper_stub(uint64_t sid, int tid)
    : shard_id(sid)
    , thread_id(tid)
    , put_edge_batch_clock(0)
    , print_op_stats_counter(0)
    , gen_seed(weaver_util::urandom_uint64())
    , mt64_gen(gen_seed)
    , uint64max_dist()
{
    assert(gen_seed != 0);
    assert(uint64max_dist.max() == UINT64_MAX);
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
            map_idx = get_map_idx(node_handle);
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
    n->max_edge_id = uint64max_dist(mt64_gen);
    node_max_edge_id[n->get_handle()] = n->max_edge_id;
    prepare_node(apn->attrs,
                 *n,
                 apn->creat_clk_buf,
                 apn->props_buf,
                 apn->out_edges_buf,
                 last_clk_buf,
                 restore_clk_buf,
                 apn->aliases_buf,
                 apn->num_attrs,
                 apn->packed_sz);

    bool success = call_no_loop(&hyperdex_client_put,
                                graph_space,
                                apn->handle.c_str(),
                                apn->handle.size(),
                                apn->attrs,
                                apn->num_attrs,
                                apn->op_id, apn->status);

    if (success) {
        apn->exec_time = timer.get_time_elapsed_millis();
        async_calls[apn->op_id] = apn;
        outstanding_node_puts.emplace(apn->handle, std::vector<apes_ptr_t>());

        for (const std::string &alias: n->aliases) {
            success = add_index_no_loop(n->get_handle(), alias, true) && success;
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
hyper_stub :: put_edge_no_loop(const node_handle_t &node_handle, db::edge *e, const std::string &alias, bool del_after_call)
{
    // write edge to edge space
    ape_ptr_t ape = ape_pool.acquire();
    ape->node_handle = node_handle;
    ape->edge_handle = e->get_handle();
    assert(node_max_edge_id.find(node_handle) != node_max_edge_id.end());
    ape->edge_id = node_max_edge_id[node_handle]++;
    ape->e = e;
    ape->alias = alias;
    ape->del_after_call = del_after_call;
    prepare_edge(ape->attrs,
                 *e,
                 ape->buf,
                 ape->packed_sz);

    bool success = call_no_loop(&hyperdex_client_put_if_not_exist,
                                edge_space,
                                (const char*)&ape->edge_id,
                                sizeof(int64_t),
                                ape->attrs, NUM_EDGE_ATTRS,
                                ape->op_id, ape->status);

    if (success) {
        ape->exec_time = timer.get_time_elapsed_millis();
        async_calls[ape->op_id] = ape;
    } else {
        WDEBUG << "hyperdex_client_put failed, op_id=" << ape->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(ape->status) << std::endl;
        WDEBUG << "edge=" << ape->edge_handle << std::endl;
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: flush_or_defer_put_edge_set(apes_ptr_t apes, bool &defer)
{
    bool success = true;
    std::string del_handle = apes->node_handle;

    put_edge_batch.erase(del_handle);

    auto outstanding_iter = outstanding_node_puts.find(del_handle);
    if (outstanding_iter != outstanding_node_puts.end()) {
        // this edge's parent node has not yet been written to HyperDex
        // defer writing edge until node has been written
        outstanding_iter->second.emplace_back(apes);
        defer = true;
    } else {
        success = flush_put_edge_set(apes, false) && success;
        defer = false;
    }

    return success;
}

bool
hyper_stub :: flush_put_edge_set(apes_ptr_t apes, bool loop_after_call)
{
    apes->attr = (hyperdex_client_attribute*)malloc(apes->batched.size() * sizeof(hyperdex_client_attribute));
    prepare_edges_set(apes->attr, apes->batched, apes->packed_sz);

    bool success = true;

    // add aliases to index
    for (const auto &apesu: apes->batched) {
        if (!apesu.alias.empty()) {
            success = add_index_no_loop(apes->node_handle, apesu.alias, loop_after_call) && success;
        }
    }

    // add edge to node object via set_add call
    success = call_no_loop(&hyperdex_client_set_add,
                           graph_space,
                           apes->node_handle.c_str(),
                           apes->node_handle.size(),
                           apes->attr,
                           apes->batched.size(),
                           apes->op_id, apes->status);

    if (success) {
        apes->exec_time = timer.get_time_elapsed_millis();
        async_calls[apes->op_id] = apes;
    } else {
        WDEBUG << "hyperdex_client_set_add failed, op_id=" << apes->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(apes->status) << std::endl;
        WDEBUG << "node=" << apes->node_handle << ", sample edge_id=" << apes->batched.front().edge_id << std::endl;
        abort_bulk_load();
    }

    if (loop_after_call) {
        possibly_flush();
    }

    return success;
}

// XXX update max_edge_id in node object at the end of bulk loading
bool
hyper_stub :: add_edge_to_node_set(const node_handle_t &node_handle,
                                   uint64_t edge_id,
                                   const std::string &alias)
{
    // add edge to node's edge list
    auto put_edge_iter = put_edge_batch.find(node_handle);
    apes_ptr_t cur_apes;
    bool get_new_apes = false;
    bool success = true;

    if (put_edge_iter != put_edge_batch.end()) {
        cur_apes = put_edge_iter->second;

        if (cur_apes->batched.size() > NUM_EDGES_BUFFERED_PER_NODE) {
            // flush edges to prevent buffering too many edges for a node
            bool defer;
            flush_or_defer_put_edge_set(cur_apes, defer);
            get_new_apes = true;
        }
    } else {
        get_new_apes = true;
        if (put_edge_batch.size() > PUT_EDGE_BUFFER_SZ) {
            WDEBUG << "flush put edge loop, sz=" << put_edge_batch.size() << std::endl;
            // write oldest PUT_EDGE_BUFFER_SZ/2
            std::vector<apes_ptr_t> can_write;
            for (const auto &p: put_edge_batch) {
                can_write.emplace_back(p.second);
            }

            std::sort(can_write.begin(), can_write.end(),
                      [](const apes_ptr_t &left, const apes_ptr_t &right) {
                          return left->time < right->time;
                      }
            );

            // remove as many MRU items as needed so that we write only PUT_EDGE_BUFFER_SZ/2 items
            if (can_write.size() > PUT_EDGE_BUFFER_SZ/2) {
                can_write.erase(can_write.begin() + PUT_EDGE_BUFFER_SZ/2, can_write.end());
            }

            uint64_t defer_count = 0;
            uint64_t call_count = 0;
            for (auto apes: can_write) {
                bool defer;
                success = flush_or_defer_put_edge_set(apes, defer) && success;
                if (defer) {
                    defer_count++;
                } else {
                    call_count++;
                }
            }
            WDEBUG << "flush put edge DONE, sz=" << put_edge_batch.size()
                   << "\tdefer_count=" << defer_count
                   << "\tcall_count=" << call_count << std::endl;
        }
    }

    if (get_new_apes) {
        cur_apes = apes_pool.acquire();
        cur_apes->reset();
        assert(cur_apes->batched.empty());
        put_edge_batch[node_handle] = cur_apes;
    }

    assert(cur_apes != nullptr);
    cur_apes->used = true;
    cur_apes->time = put_edge_batch_clock++;
    cur_apes->node_handle = node_handle;
    async_put_edge_set_unit cur_apesu;
    cur_apesu.edge_id = edge_id;
    cur_apesu.alias = alias;
    cur_apes->batched.emplace_back(std::move(cur_apesu));

    return success;
}

bool
hyper_stub :: add_index_no_loop(const node_handle_t &node_handle, const std::string &alias, bool loop_after_call)
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
    aai->packed_sz = aai->index_attrs[0].value_sz
                   + aai->index_attrs[1].value_sz; 

    bool success = call_no_loop(&hyperdex_client_put,
                                index_space,
                                aai->alias.c_str(),
                                aai->alias.size(),
                                aai->index_attrs, NUM_INDEX_ATTRS,
                                aai->op_id, aai->status);

    if (success) {
        aai->exec_time = timer.get_time_elapsed_millis();
        async_calls[aai->op_id] = aai;
    } else {
        WDEBUG << "hyperdex_client_put failed, op_id=" << aai->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(aai->status) << std::endl;
        WDEBUG << "alias=" << aai->alias << ", node=" << aai->node_handle << std::endl;
        abort_bulk_load();
    }

    if (loop_after_call) {
        possibly_flush();
    }

    return success;
}

bool
hyper_stub :: flush_all_put_edge()
{
    bool success;

    auto put_edge_batch_local = std::move(put_edge_batch);
    assert(put_edge_batch.empty());
    for (auto &p: put_edge_batch_local) {
        assert(p.first == p.second->node_handle);
        if (!p.second->batched.empty()) {
            success = flush_put_edge_set(p.second, false) && success;
        }
    }

    return success;
}

void
hyper_stub :: done_op_stat(uint64_t time, size_t op_sz)
{
    while (done_op_stats.size() > 10000) {
        done_op_stats.pop_front();
    }
    done_op_stats.push_back(std::make_pair(time, op_sz));

    if (++print_op_stats_counter % 10000 == 0) {
        float tot_time = 0;
        float tot_sz = 0;
        for (const auto &p: done_op_stats) {
            tot_time += p.first;
            tot_sz += p.second;
        }

        float avg_time_sec = tot_time / (10000*1000);
        float avg_sz = tot_sz / 10000;
        WDEBUG << "tid=" << thread_id
               << "\tAvg Time for last 10k ops=" << avg_time_sec << " s."
               << "\tAvg Sz of last 10k ops=" << avg_sz
               << std::endl;
    }
}

bool
hyper_stub :: done_op(async_call_ptr_t ac_ptr, int64_t op_id)
{
    bool success = true;

    assert(ac_ptr->exec_time != 42);
    uint64_t op_time = timer.get_time_elapsed_millis() - ac_ptr->exec_time;
    size_t op_sz = ac_ptr->packed_sz;
    switch (ac_ptr->type) {
        case PUT_NODE: {
            auto apn = std::static_pointer_cast<async_put_node>(ac_ptr);

            // flush any pending put edge calls dependent on this node
            auto outstanding_iter = outstanding_node_puts.find(apn->handle);
            assert(outstanding_iter != outstanding_node_puts.end());
            for (auto ape: outstanding_iter->second) {
                success = flush_put_edge_set(ape, false) && success;
            }
            outstanding_node_puts.erase(outstanding_iter);

            apn_pool.release(apn);
            break;
        }

        case PUT_EDGE_SET:
            apes_pool.release(std::static_pointer_cast<async_put_edge_set>(ac_ptr));
            break;

        case PUT_EDGE: {
            ape_ptr_t ape = std::static_pointer_cast<async_put_edge>(ac_ptr);
            if (ape->del_after_call) {
                delete ape->e;
            }
            assert(!ape->node_handle.empty());
            add_edge_to_node_set(ape->node_handle, ape->edge_id, ape->alias);
            ape_pool.release(ape);
            break;
       }

        case ADD_INDEX:
            aai_pool.release(std::static_pointer_cast<async_add_index>(ac_ptr));
            break;
    }

    async_calls.erase(op_id);

    done_op_stat(op_time, op_sz);

    return success;
}

bool
hyper_stub :: loop_async(uint64_t num_ops_to_leave, uint64_t &num_timeouts)
{
    bool success = true;
    uint64_t initial_ops = async_calls.size();
    num_timeouts = 0;

    while (async_calls.size() > num_ops_to_leave) {
        int64_t op_id;
        hyperdex_client_returncode loop_code;

        loop(op_id, loop_code);
        if (op_id < 0) {
            if (loop_code == HYPERDEX_CLIENT_TIMEOUT) {
                num_timeouts++;
                if (num_timeouts % 100 == 0) {
                    WDEBUG << "tid=" << thread_id << "\t#timeouts=" << num_timeouts << std::endl;
                }
                continue;
            } else {
                WDEBUG << "hyperdex_client_loop failed, op_id=" << op_id
                       << ", loop_code=" << hyperdex_client_returncode_to_string(loop_code) << std::endl;
                abort_bulk_load();
            }
        }
        assert(loop_code == HYPERDEX_CLIENT_SUCCESS);

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
                success = done_op(ac_ptr, op_id) && success;
                break;

            case HYPERDEX_CLIENT_CMPFAIL: {
                assert(ac_ptr->type == PUT_EDGE);
                async_calls.erase(op_id);
                // reissue put with new edge id
                ape_ptr_t ape = std::static_pointer_cast<async_put_edge>(ac_ptr);
                WDEBUG << "repeat edge id=" << ape->edge_id << std::endl;
                put_edge_no_loop(ape->node_handle,
                                 ape->e,
                                 ape->alias,
                                 ape->del_after_call);
                ape_pool.release(ape);
                break;
            }

            default:
                WDEBUG << "Unexpected hyperdex op code, here are some details." << std::endl;
                WDEBUG << "type=" << async_call_type_to_string(ac_ptr->type)
                       << ", call_code=" << hyperdex_client_returncode_to_string(ac_ptr->status) << std::endl;
                abort_bulk_load();
        }

        if (num_timeouts > MAX_TIMEOUTS) {
            WDEBUG << "exceeded max loops, initial_ops=" << initial_ops
                   << ", num_timeouts=" << num_timeouts
                   << ", async_calls.size=" << async_calls.size() << std::endl;
            if (num_ops_to_leave == 0) {
                for (auto &p: async_calls) {
                    done_op(p.second, p.first);
                }
            }
            break;
            // XXX assert(false);
        }
    }

    return success;
}

bool
hyper_stub :: loop_async_calls(bool flush)
{
    bool success;
    uint64_t timeouts = 0;

    if (flush) {
        success = loop_async(0, timeouts);

        apn_pool.clear();
        ape_pool.clear();
        aai_pool.clear();
        node_max_edge_id.clear();
    } else {
        success = loop_async(OUTSTANDING_HD_OPS, timeouts);
    }

    return success;
}

// prevent too many outstanding requests
void
hyper_stub :: possibly_flush()
{
    loop_async_calls(false);
}

void
hyper_stub :: abort_bulk_load()
{
    WDEBUG << "Aborting bulk load now." << std::endl;
    WDEBUG << "error message: " << hyperdex_client_error_message(cl) << std::endl;
    WDEBUG << "error loc: " << hyperdex_client_error_location(cl) << std::endl;
    assert(false);
}

#undef weaver_debug_
