/*
 * ===============================================================
 *    Description:  Implement property predicate.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <assert.h>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/property_predicate.h"

using predicate::prop_predicate;

bool
prop_predicate :: check(const node_prog::property &prop) const
{
    switch (rel) {
        case EQUALS:
            return key == prop.get_key() && prop.get_value() == value;
            break;

        case LESS:
            return key == prop.get_key() && prop.get_value() < value;
            break;

        case GREATER:
            return key == prop.get_key() && prop.get_value() > value;
            break;

        case LESS_EQUAL:
            return key == prop.get_key() && prop.get_value() <= value;
            break;

        case GREATER_EQUAL:
            return key == prop.get_key() && prop.get_value() >= value;
            break;

        case STARTS_WITH:
            return key == prop.get_key()
                && prop.get_value().size() >= value.size()
                && prop.get_value().compare(0, value.size(), value) == 0;
            break;

        case ENDS_WITH:
            return key == prop.get_key()
                && prop.get_value().size() >= value.size()
                && prop.get_value().compare(prop.get_value().size()-value.size(), value.size(), value) == 0;
            break;

        case CONTAINS:
            return key == prop.get_key()
                && prop.get_value().find(value) != std::string::npos;
            break;

        default:
            WDEBUG << "bad rel " << rel << std::endl;
            return false;
    }
}
