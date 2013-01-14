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
#include "cache.h"

int
main()
{
    cache::reach_cache c;
    po6::net::location loc1 ("127.0.0.1", 42);
    po6::net::location loc2 ("10.10.1.1", 84);
    c.insert_entry(loc1, (void*)0xdeadbeef, true);
    c.insert_entry(loc2, (void*)0xcafebabe, false);

    std::cout << "Entry exists " << c.entry_exists(loc1, (void*)0xdeadbeef)
              << " " << c.entry_exists(loc2, (void*)0xdeadbeef);
    std::cout << "\nValues " << c.get_cached_value(loc2, (void*)0xcafebabe)
              << " " << c.get_cached_value(loc1, (void*)0xdeadbeef)
              << std::endl;
    return 0;
}
