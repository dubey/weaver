/*
 * ===============================================================
 *    Description:  Each graph element (node or edge) can have
 *                  properties, which are key-value pairs 
 *
 *        Created:  Friday 12 October 2012 01:28:02  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PUB_PROPERTY__
#define __PUB_PROPERTY__

#include <string>

namespace node_prog
{
    class property
    {
        public:
            virtual const std::string& get_key() = 0;
            virtual const std::string& get_value() = 0;
   };
}

#endif
