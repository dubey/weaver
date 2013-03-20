/*
 * ===============================================================
 *    Description:  Debugging functions
 *
 *        Created:  03/20/2013 11:20:25 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>

// called when unhandled exception calls terminate()
void
debug_terminate()
{
    std::cerr << "In custom terminate" << std::endl;
    exit(-1);
}
