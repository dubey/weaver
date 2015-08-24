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

#include <iterator>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "db/shard_constants.h"
#include "db/utils.h"
#include "db/hyper_stub.h"

#define OUTSTANDING_HD_OPS 100
#define MAX_TIMEOUTS 3000 // 5 mins of waiting total
#define INITIAL_POOL_SZ (OUTSTANDING_HD_OPS*3)

using db::hyper_stub;
//using hyper_stub::hyper_stub_pool;
//using db::apn_ptr_t;
//using db::apes_ptr_t;
//using db::ape_ptr_t;
//using db::aai_ptr_t;

template <typename T>
hyper_stub :: hyper_stub_pool<T> :: hyper_stub_pool(async_call_type t)
    : sz(INITIAL_POOL_SZ)
    , type(t)
{
    pool.reserve(INITIAL_POOL_SZ);
    for (uint32_t i = 0; i < INITIAL_POOL_SZ; i++) {
        pool.emplace_back(std::make_shared<T>());
    }
}

template <typename T>
uint64_t
hyper_stub :: hyper_stub_pool<T> :: size()
{
    return sz;
}

template <typename T>
std::shared_ptr<T>
hyper_stub :: hyper_stub_pool<T> :: acquire()
{
    std::shared_ptr<T> ret;

    if (pool.empty()) {
        for (uint32_t i = 0; i < OUTSTANDING_HD_OPS; i++) {
            std::shared_ptr<T> new_ptr= std::make_shared<T>();
            pool.emplace_back(new_ptr);
        }
        sz += OUTSTANDING_HD_OPS;
        WDEBUG << async_call_type_to_string(type) << " INC pool sz=" << sz << std::endl;
    }

    async_call_ptr_t back = pool.back();
    ret = std::static_pointer_cast<T>(back);
    pool.pop_back();

    return ret;
}

template <typename T>
void
hyper_stub :: hyper_stub_pool<T> :: release(std::shared_ptr<T> ptr)
{
    if (ptr->type == PUT_EDGE_SET) {
        async_call_ptr_t ac_ptr = ptr;
        apes_ptr_t apes = std::static_pointer_cast<async_put_edge_set>(ac_ptr);
        apes->node_handle.clear();
    }
    pool.emplace_back(ptr);

    if (sz >= 2*INITIAL_POOL_SZ
     && pool.size() > 0.75*sz) {
        // half the size of the pool
        uint64_t num_erased = std::distance(pool.begin() + sz/2, pool.end());
        pool.erase(pool.begin() + sz/2, pool.end());
        sz -= num_erased;
        WDEBUG << async_call_type_to_string(type) << " DEC pool sz=" << sz << std::endl;
    }
}


hyper_stub :: hyper_stub(uint64_t sid, int tid)
    : m_shard_id(sid)
    , m_thread_id(tid)
    , m_apn_pool(PUT_NODE)
    , m_apes_pool(PUT_EDGE_SET)
    , m_ape_pool(PUT_EDGE)
    , m_apei_pool(PUT_EDGE_ID)
    , m_aai_pool(ADD_INDEX)
    , m_async_calls()
    , m_restore_clk_buf(nullptr)
    , m_last_clk_buf(nullptr)
    , g_load_chunk(0)
    , g_loaded_elems()
    , g_node_edge_id()
    , g_bulk_load_mtx()
    , g_chunk_cond(&g_bulk_load_mtx)
    , m_done_op_stats()
    , m_timer()
    , m_print_op_stats_counter(0)
{ }

void
hyper_stub :: restore_backup(db::data_map<std::shared_ptr<db::node_entry>> *nodes,
    /*XXX std::unordered_map<node_handle_t, std::unordered_set<node_version_t, node_version_hash>> &edge_map,*/
    po6::threads::mutex *shard_mutexes)
{
    const hyperdex_client_attribute *cl_attr;
    size_t num_attrs;

    // node list
    const hyperdex_client_attribute_check attr_check = {node_attrs[0],
                                                        (const char*)&m_shard_id,
                                                        sizeof(int64_t),
                                                        node_dtypes[0],
                                                        HYPERPREDICATE_EQUALS};
    enum hyperdex_client_returncode search_status, loop_status;

    int64_t call_id = hyperdex_client_search(m_cl, node_space, &attr_check, 1, &search_status, &cl_attr, &num_attrs);
    if (call_id < 0) {
        WDEBUG << "Hyperdex function failed, op id = " << call_id
               << ", status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
        WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
        WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
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
        loop_id = hyperdex_client_loop(m_cl, -1, &loop_status);
        if (loop_id != call_id
         || loop_status != HYPERDEX_CLIENT_SUCCESS
         || (search_status != HYPERDEX_CLIENT_SUCCESS && search_status != HYPERDEX_CLIENT_SEARCHDONE)) {
            WDEBUG << "Hyperdex function failed, call id = " << call_id
                   << ", loop_id = " << loop_id
                   << ", loop status = " << hyperdex_client_returncode_to_string(loop_status)
                   << ", search status = " << hyperdex_client_returncode_to_string(search_status) << std::endl;
            WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
            WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
            return;
        }

        if (search_status == HYPERDEX_CLIENT_SEARCHDONE) {
            loop_done = true;
        } else if (search_status == HYPERDEX_CLIENT_SUCCESS) {
            assert(num_attrs == NUM_NODE_ATTRS+1); // node handle + graph attrs

            uint64_t key_idx = UINT64_MAX;
            hyperdex_client_attribute node_attrs[NUM_NODE_ATTRS];
            for (uint64_t i = 0, j = 0; i < num_attrs; i++) {
                if (strncmp(cl_attr[i].attr, node_key, 4) == 0) {
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
    if (get_nodes(nodes, true, false)) {
        n.edge_ids.clear();
        return true;
    } else {
        return false;
    }
}

void
hyper_stub :: mark_done_chunk(uint64_t chunk)
{
    g_bulk_load_mtx.lock();
    assert(chunk == (g_load_chunk+1));
    g_load_chunk = chunk;
    g_loaded_elems.clear();
    g_chunk_cond.broadcast();
    g_bulk_load_mtx.unlock();
}

void
hyper_stub :: check_loaded_chunk(uint64_t chunk, uint32_t chunk_elem,
                                 bool &loaded_chunk, bool &loaded_elem)
{
    g_bulk_load_mtx.lock();
    if (chunk > g_load_chunk) {
        loaded_chunk = false;

        if (g_loaded_elems.find(chunk_elem) == g_loaded_elems.end()) {
            loaded_elem = false;
            g_loaded_elems.emplace(chunk_elem);
        } else {
            loaded_elem = true;
        }
    } else {
        loaded_chunk = true;
        loaded_elem  = true;
    }
    g_bulk_load_mtx.unlock();
}

void
hyper_stub :: wait_done_chunk(uint64_t chunk)
{
    g_bulk_load_mtx.lock();
    while (chunk > g_load_chunk) {
        g_chunk_cond.wait();
    }
    g_bulk_load_mtx.unlock();
}

bool
hyper_stub :: new_node(const node_handle_t &handle, uint64_t start_edge_idx)
{
    bool found;

    assert(start_edge_idx);
    g_bulk_load_mtx.lock();
    if (g_node_edge_id.find(handle) != g_node_edge_id.end()) {
        found = true;
    } else {
        found = false;
        g_node_edge_id[handle] = std::make_pair(start_edge_idx, 0);
    }
    g_bulk_load_mtx.unlock();

    return found;
}

bool
hyper_stub :: put_node_no_loop(db::node *n)
{
    if (m_last_clk_buf == nullptr) {
        std::shared_ptr<vc::vclock> last_upd_clk  = std::make_shared<vc::vclock>(0,0);
        std::shared_ptr<vc::vclock_t> restore_clk = std::make_shared<vc::vclock_t>(last_upd_clk->clock);
        prepare_buffer(last_upd_clk, m_last_clk_buf);
        prepare_buffer(restore_clk, m_restore_clk_buf);
    }
    assert(m_last_clk_buf && m_restore_clk_buf);

    apn_ptr_t apn = m_apn_pool.acquire();

    bool success = put_node_async(apn,
                                  n,
                                  m_last_clk_buf,
                                  m_restore_clk_buf,
                                  m_async_calls,
                                  false,
                                  false);

    if (success) {
        apn->exec_time = m_timer.get_real_time_millis();

        for (const std::string &alias: n->aliases) {
            success = add_index_no_loop(n->get_handle(), alias) && success;
        }
    }
    
    if (!success) {
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

// return edge id for new edge
uint64_t
hyper_stub :: new_edge(const node_handle_t &node_handle)
{
    g_bulk_load_mtx.lock();

    auto edge_id_iter = g_node_edge_id.find(node_handle);
    assert(edge_id_iter != g_node_edge_id.end());
    std::pair<uint64_t, uint64_t> &edge_id = edge_id_iter->second;
    uint64_t eid = edge_id.first + edge_id.second++;

    g_bulk_load_mtx.unlock();

    return eid;
}

bool
hyper_stub :: put_edge_id_no_loop(uint64_t edge_id,
                                  const edge_handle_t &edge_handle)
{
    apei_ptr_t apei = m_apei_pool.acquire();

    bool success = put_edge_id_async(apei,
                                     edge_id,
                                     edge_handle,
                                     m_async_calls,
                                     false);

    if (success) {
        apei->exec_time = m_timer.get_real_time_millis();
    } else {
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: put_edge_no_loop(const node_handle_t &node_handle,
                               db::edge *e,
                               uint64_t edge_id,
                               bool del_after_call)
{
    // add edge id
    if (!put_edge_id_no_loop(edge_id, e->get_handle())) {
        return false;
    }

    // write edge to edge space
    ape_ptr_t ape = m_ape_pool.acquire();

    bool success = put_edge_async(ape,
                                  node_handle,
                                  e,
                                  edge_id,
                                  m_shard_id,
                                  del_after_call,
                                  m_async_calls,
                                  false,
                                  false);

    if (success) {
        ape->exec_time = m_timer.get_real_time_millis();
    } else {
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: put_node_edge_id_set_no_loop(const node_handle_t &node_handle,
                                           int64_t start_id,
                                           int64_t end_id)
{
    apes_ptr_t apes = m_apes_pool.acquire();
    apes->node_handle = node_handle;
    apes->max_edge_id = end_id;

    // edge id set
    std::set<int64_t> id_set;
    for (int64_t i = start_id; i < end_id; i++) {
        id_set.emplace(i);
    }
    prepare_buffer(id_set, apes->set_buf);
    apes->set_attr[0].attr = node_attrs[3];
    apes->set_attr[0].value = (const char*)apes->set_buf->data();
    apes->set_attr[0].value_sz = apes->set_buf->size();
    apes->set_attr[0].datatype = node_dtypes[3];

    // max edge id
    apes->set_attr[1].attr = node_attrs[4];
    apes->set_attr[1].value = (const char*)&apes->max_edge_id;
    apes->set_attr[1].value_sz = sizeof(int64_t);
    apes->set_attr[1].datatype = node_dtypes[4];

    apes->packed_sz = apes->set_attr[0].value_sz + apes->set_attr[1].value_sz;

    bool success = call_no_loop(&hyperdex_client_put,
                                node_space,
                                apes->node_handle.c_str(),
                                apes->node_handle.size(),
                                apes->set_attr, 2,
                                apes->op_id, apes->status);

    if (success) {
        apes->exec_time = m_timer.get_real_time_millis();
        m_async_calls[apes->op_id] = apes;
    } else {
        WDEBUG << "hyperdex_client_put failed, op_id=" << apes->op_id
               << ", call_code=" << hyperdex_client_returncode_to_string(apes->status) << std::endl;
        WDEBUG << "node=" << apes->node_handle << std::endl;
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: add_index_no_loop(const node_handle_t &node_handle,
                                const std::string &alias)
{
    aai_ptr_t aai = m_aai_pool.acquire();

    bool success = add_index_async(aai,
                                   node_handle,
                                   alias,
                                   m_shard_id,
                                   m_async_calls,
                                   false,
                                   false);

    if (success) {
        aai->exec_time = m_timer.get_real_time_millis();
    } else {
        abort_bulk_load();
    }

    possibly_flush();

    return success;
}

bool
hyper_stub :: flush_all_edge_ids()
{
    WDEBUG << "flush edge id set, #nodes=" << g_node_edge_id.size() << std::endl;

    for (const auto &p: g_node_edge_id) {
        int64_t start_id = (int64_t)p.second.first;
        int64_t end_id = (int64_t)(p.second.first + p.second.second);
        if (!put_node_edge_id_set_no_loop(p.first, start_id, end_id)) {
            return false;
        }
    }
    g_node_edge_id.clear();

    return true;
}

void
hyper_stub :: done_op_stat(uint64_t time, size_t op_sz)
{
    m_done_op_stats.push_back(std::make_pair(time, op_sz));
    while (m_done_op_stats.size() > 10000) {
        m_done_op_stats.pop_front();
    }

    if (++m_print_op_stats_counter % 10000 == 0) {
        float tot_time = 0;
        float tot_sz = 0;
        for (const auto &p: m_done_op_stats) {
            tot_time += p.first;
            tot_sz += p.second;
        }

        float avg_time_sec = tot_time / (m_done_op_stats.size()*1000);
        float avg_sz = tot_sz / m_done_op_stats.size();
        WDEBUG << "tid=" << m_thread_id
               << "\tAvg Time for last 10k ops=" << avg_time_sec << " s."
               << "\tAvg Sz of last 10k ops=" << avg_sz
               << "\t#ops=" << m_done_op_stats.size()
               << std::endl;
    }
}

bool
hyper_stub :: done_op(async_call_ptr_t ac_ptr, int64_t op_id)
{
    bool success = true;

    assert(ac_ptr->exec_time != 42);
    uint64_t op_time = m_timer.get_real_time_millis() - ac_ptr->exec_time;
    size_t op_sz = ac_ptr->packed_sz;
    switch (ac_ptr->type) {
        case PUT_NODE: {
            m_apn_pool.release(std::static_pointer_cast<async_put_node>(ac_ptr));
            break;
        }

        case PUT_EDGE_SET:
            m_apes_pool.release(std::static_pointer_cast<async_put_edge_set>(ac_ptr));
            break;

        case PUT_EDGE: {
            ape_ptr_t ape = std::static_pointer_cast<async_put_edge>(ac_ptr);
            if (ape->del_after_call) {
                delete ape->e;
            }
            m_ape_pool.release(ape);
            break;
       }

        case PUT_EDGE_ID:
            m_apei_pool.release(std::static_pointer_cast<async_put_edge_id>(ac_ptr));
            break;

        case ADD_INDEX:
            m_aai_pool.release(std::static_pointer_cast<async_add_index>(ac_ptr));
            break;

        default:
            WDEBUG << "impossible async call type=" << async_call_type_to_string(ac_ptr->type) << std::endl;
            assert(false);
    }

    m_async_calls.erase(op_id);

    done_op_stat(op_time, op_sz);

    return success;
}

bool
hyper_stub :: loop_async_and_flush(uint64_t num_ops_to_leave,
                                   uint64_t &num_timeouts)
{
    bool success = true;
    uint64_t initial_ops = m_async_calls.size();
    num_timeouts = 0;

    while (true) {
        if (m_async_calls.size() > num_ops_to_leave) {
            int64_t op_id;
            hyperdex_client_returncode loop_code;

            loop(false, op_id, loop_code);
            if (op_id < 0) {
                if (loop_code == HYPERDEX_CLIENT_TIMEOUT) {
                    num_timeouts++;
                    if (num_timeouts % 100 == 0) {
                        WDEBUG << "tid=" << m_thread_id << "\t#timeouts=" << num_timeouts << std::endl;
                    }

                    if (num_timeouts > MAX_TIMEOUTS) {
                        WDEBUG << "exceeded max loops, initial_ops=" << initial_ops
                               << ", num_timeouts=" << num_timeouts
                               << ", m_async_calls.size=" << m_async_calls.size() << std::endl;
                        assert(false);
                        if (num_ops_to_leave == 0) {
                            for (auto &p: m_async_calls) {
                                done_op(p.second, p.first);
                            }
                        }
                        break;
                    }

                    continue;
                } else {
                    WDEBUG << "hyperdex_client_loop failed, op_id=" << op_id
                           << ", loop_code=" << hyperdex_client_returncode_to_string(loop_code) << std::endl;
                    if (loop_code == HYPERDEX_CLIENT_NONEPENDING) {
                        WDEBUG << "#async_calls=" << m_async_calls.size() << std::endl;
                        m_async_calls.clear();
                        break;
                    } else {
                        abort_bulk_load();
                    }
                }
            }
            assert(loop_code == HYPERDEX_CLIENT_SUCCESS);

            auto find_iter = m_async_calls.find(op_id);
            if (find_iter == m_async_calls.end()) {
                WDEBUG << "hyperdex_client_loop returned op_id=" << op_id
                       << " and loop_code=" << hyperdex_client_returncode_to_string(loop_code)
                       << " which was not found in m_async_calls_map." << std::endl;
                abort_bulk_load();
            }

            async_call_ptr_t ac_ptr = find_iter->second;
            switch (ac_ptr->status) {
                case HYPERDEX_CLIENT_SUCCESS:
                    success = done_op(ac_ptr, op_id) && success;
                    break;

                default:
                    WDEBUG << "Unexpected hyperdex op code, here are some details." << std::endl;
                    WDEBUG << "type=" << async_call_type_to_string(ac_ptr->type)
                           << ", call_code=" << hyperdex_client_returncode_to_string(ac_ptr->status) << std::endl;
                    abort_bulk_load();
            }

        } else {
            break;
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
        success = loop_async_and_flush(0, timeouts);
    } else {
        success = loop_async_and_flush(OUTSTANDING_HD_OPS, timeouts);
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
    WDEBUG << "error message: " << hyperdex_client_error_message(m_cl) << std::endl;
    WDEBUG << "error loc: " << hyperdex_client_error_location(m_cl) << std::endl;
    assert(false);
}

void
hyper_stub :: done_bulk_load()
{
    m_apn_pool.clear();
    m_ape_pool.clear();
    m_apes_pool.clear();
    m_apei_pool.clear();
    m_aai_pool.clear();
}

#undef weaver_debug_
