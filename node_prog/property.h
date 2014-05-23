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

#ifndef weaver_node_prog_property_h_
#define weaver_node_prog_property_h_

#include <string>

namespace node_prog
{
    class property
    {

        public:
            std::string key;
            std::string value;

            property() : key(""), value("") { }
            property(std::string &k, std::string &v) : key(k), value(v) { }

            const std::string& get_key() { return key; }
            const std::string& get_value() { return value; }
   };
}

#endif
