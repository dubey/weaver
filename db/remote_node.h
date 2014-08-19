/*
 * ===============================================================
 *    Description:  Remote node pointer for edges.
 *
 *        Created:  03/01/2013 11:29:16 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/types.h"

#ifndef weaver_db_element_remote_node_h_
#define weaver_db_element_remote_node_h_

namespace db
{
namespace element
{
    class remote_node
    {
        public:
            remote_node() { }
            remote_node(uint64_t l, const node_handle_t &h) : loc(l), handle(h) { }

        public:
            uint64_t loc;
            node_handle_t handle;
            bool operator==(const db::element::remote_node &t) const { return (handle == t.handle) && (loc == t.loc); }
            bool operator!=(const db::element::remote_node &t) const { return (handle != t.handle) || (loc != t.loc); }
    };

    static db::element::remote_node coordinator(0, node_handle_t(""));
}
}

#endif
