/*
 * ===============================================================
 *    Description:  Testing reachability cache
 *
 *        Created:  12/04/2012 11:09:50 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include "db/cache/cache.h"

void
cache_test()
{
    cache::reach_cache c;
    c.insert_entry(42, 84, 21);
    c.insert_entry(20, 40, 60);
    assert(c.entry_exists(42, 84));
    assert(c.entry_exists(20, 40));
    assert(!c.entry_exists(30, 60));

    c.remove_entry(21);
    assert(!c.entry_exists(42, 84));
    assert(c.entry_exists(20, 40));
    assert(!c.entry_exists(30, 60));
}
