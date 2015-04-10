/*
 * ===============================================================
 *    Description:  A predicate based on node/edge property.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_property_predicate_h_
#define weaver_common_property_predicate_h_

#include <string>

#include "node_prog/property.h"

namespace predicate
{
    enum relation
    {
        EQUALS,
        LESS,
        GREATER,
        LESS_EQUAL,
        GREATER_EQUAL,
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS
    };

    struct prop_predicate
    {
        std::string key;
        std::string value;
        relation rel;

        bool check(const node_prog::property &prop) const;
    };
}

#endif
