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
main ()
{
    cache::reach_cache c;
    c.insert_entry (42, (void*)0xdeadbeef, true);
    c.insert_entry (84, (void*)0xcafebabe, false);

    std::cout << "Entry exists " << c.entry_exists (42, (void*)0xdeadbeef)
              << " " << c.entry_exists (43, (void*)0xdeadbeef);
    std::cout << "\nValues " << c.get_cached_value (84, (void*)0xcafebabe)
              << " " << c.get_cached_value (42, (void*)0xdeadbeef)
              << std::endl;
    return 0;
}
