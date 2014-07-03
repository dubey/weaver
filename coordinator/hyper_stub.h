/*
 * ===============================================================
 *    Description:  Hyperdex client stub for timestamper state.
 *
 *        Created:  2014-02-26 13:37:34
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_hyper_stub_h_
#define weaver_coordinator_hyper_stub_h_

#include "common/hyper_stub_base.h"
#include "coordinator/current_tx.h"

#define NUM_MAP_ATTRS 2

namespace coordinator
{
    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t vt_id;
            // vt id -> set of outstanding tx ids
            const char *vt_set_space = "weaver_vt_tx_set_data";
            const char *set_attr = "tx_id_set";
            // tx id -> tx data
            const char *vt_map_space = "weaver_vt_tx_map_data";
            const char *tx_data_attr = "tx_data";
            const char *tx_status_attr = "status"; // 0 for prepare, 1 for commit
            const enum hyperdatatype set_dtype = HYPERDATATYPE_SET_INT64;
            const enum hyperdatatype map_dtypes[NUM_MAP_ATTRS];

        public:
            hyper_stub(uint64_t vtid, bool put_initial);
            void prepare_tx(transaction::pending_tx &tx);
            void commit_tx(transaction::pending_tx &tx);
            void del_tx(uint64_t tx_id);
            void restore_backup(std::unordered_map<uint64_t, current_tx> &prepare_tx,
                std::unordered_map<uint64_t, current_tx> &outstanding_tx);
        private:
            void recreate_tx(const hyperdex_client_attribute *attr, size_t num_attrs,
                coordinator::current_tx &cur_tx, bool &status);
    };
}

#endif
