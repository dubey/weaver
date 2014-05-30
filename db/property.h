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

#ifndef weaver_db_element_property_h_
#define weaver_db_element_property_h_

#include <string>

#include "common/weaver_constants.h"
#include "common/vclock.h"

#include "node_prog/property.h"

namespace db
{
namespace element
{
    class property : public node_prog::property
    {
        public:
            property();
            property(std::string&, std::string&);
            property(std::string&, std::string&, vc::vclock&);
        
            vc::vclock creat_time;
            vc::vclock del_time;

            bool equals(std::string const &key2, std::string const &value2) const;
            bool operator==(property const &p2) const;

            const vc::vclock& get_creat_time() const;
            const vc::vclock& get_del_time() const;
            void update_del_time(vc::vclock&);
    };
}
}

namespace std
{
    template <>
    struct hash<db::element::property> 
    {
        private:
            std::function<size_t(const std::string&)> string_hasher;

        public:
            hash<db::element::property>() : string_hasher(std::hash<std::string>()) { }

            size_t operator()(const db::element::property &p) const throw() 
            {
                size_t hkey = string_hasher(p.key);
                size_t hvalue = string_hasher(p.value);
                return ((hkey + 0x9e3779b9 + (hvalue<<6) + (hvalue>>2)) ^ hvalue);
            }
    };
}

#endif
