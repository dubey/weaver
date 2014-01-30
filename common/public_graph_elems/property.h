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

#include "db/element/property.h"

namespace common
{
    class property : private db::element::property
    {
        public:
            using db::element::property::key;
            using db::element::property::value;
            /*
            using db::element::property::property;
            */
            //property(std::string &k, std::string &v) /*: db::element::property(k, v)*/ {};
    };
}

#endif
