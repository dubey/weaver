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

#include "common/weaver_constants.h"
#include "common/hyper_stub_base.h"

namespace coordinator
{
    class hyper_stub : private hyper_stub_base
    {
        private:
            const uint64_t vt_id;
            // vt id -> set of outstanding tx ids
            const char *vt_set_space = "weaver_vt_tx_set_data";
            // tx id -> tx data
            const char *vt_map_space = "weaver_vt_tx_map_data";
            const char *attr = "tx";
            const enum hyperdatatype set_dtype = HYPERDATATYPE_SET_INT64;
            const enum hyperdatatype map_dtype = HYPERDATATYPE_STRING;

        public:
            hyper_stub(uint64_t sid) : vt_id(sid) { }
            void restore_backup(std::unordered_map<uint64_t, transaction::pending_tx> &txs);
            void put_tx(uint64_t tx_id, message::message &tx_msg);
            void del_tx(uint64_t tx_id);
    };
}

#endif
