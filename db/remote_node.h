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
            remote_node(uint64_t l, const node_id_t &i) : loc(l), id(i) { }

        public:
            uint64_t loc;
            node_id_t id;
            node_id_t get_id() { return id; }
            bool operator==(const db::element::remote_node &t) const { return (id == t.id) && (loc == t.loc); }
            bool operator!=(const db::element::remote_node &t) const { return (id != t.id) || (loc != t.loc); }
    };

    static db::element::remote_node coordinator(0, node_id_t(""));
}
}

#endif
